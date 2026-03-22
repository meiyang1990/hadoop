// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.mapred.nativetask.handlers;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.mapred.RawKeyValueIterator;
import org.apache.hadoop.mapred.nativetask.Constants;
import org.apache.hadoop.mapred.nativetask.DataReceiver;
import org.apache.hadoop.mapred.nativetask.NativeDataSource;
import org.apache.hadoop.mapred.nativetask.buffer.BufferType;
import org.apache.hadoop.mapred.nativetask.buffer.ByteBufferDataReader;
import org.apache.hadoop.mapred.nativetask.buffer.InputBuffer;
import org.apache.hadoop.util.Progress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 本地任务shuffle阶段的拉取器，主动触发数据加载并从缓冲区拉取键值对数据，供本地执行器使用
 * 实现了RawKeyValueIterator迭代器接口和DataReceiver数据接收接口
 */
@InterfaceAudience.Private
public class BufferPuller implements RawKeyValueIterator, DataReceiver {
  
  private static final Logger LOG = LoggerFactory.getLogger(BufferPuller.class);

  public final static int KV_HEADER_LENGTH = Constants.SIZEOF_KV_LENGTH;

  byte[] keyBytes = new byte[0];
  byte[] valueBytes = new byte[0];

  private InputBuffer inputBuffer;
  private InputBuffer asideBuffer;

  int remain = 0;

  private ByteBufferDataReader nativeReader;

  DataInputBuffer keyBuffer = new DataInputBuffer();
  DataInputBuffer valueBuffer = new DataInputBuffer();

  private boolean noMoreData = false;

  private NativeDataSource input;
  private boolean closed = false;

  /**
   * 构造BufferPuller，初始化输入缓冲区和备用缓冲区
   * @param handler 本地数据源，提供输入缓冲区和数据加载能力
   * @throws IOException 初始化失败时抛出IO异常
   */
  public BufferPuller(NativeDataSource handler) throws IOException {
    this.input = handler;
    this.inputBuffer = handler.getInputBuffer();
    nativeReader = new ByteBufferDataReader(null);
    this.asideBuffer = new InputBuffer(BufferType.HEAP_BUFFER, inputBuffer.capacity());
  }

  @Override
  public DataInputBuffer getKey() throws IOException {
    return keyBuffer;
  }

  @Override
  public DataInputBuffer getValue() throws IOException {
    return valueBuffer;
  }
  
  /**
   * 重置拉取器状态，允许重新拉取数据
   */
  public void reset() {
    noMoreData = false;
  }

  /**
   * 迭代获取下一个键值对，从缓冲区拉取数据
   * @return 是否存在下一个键值对，存在返回true，已拉取完所有数据返回false
   * @throws IO异常 读取数据失败时抛出
   */
  @Override
  public boolean next() throws IOException {
    if (closed) {
      return false;
    }
    
    if (noMoreData) {
      return false;
    }
    final int asideRemain = asideBuffer.remaining();
    final int inputRemain = inputBuffer.remaining();

    // 两个缓冲区都为空，触发数据源加载新数据
    if (asideRemain == 0 && inputRemain == 0) {
      input.loadData();
    }

    // 优先从备用缓冲区读取，备用缓冲区为空则从主输入缓冲区读取
    if (asideBuffer.remaining() > 0) {
      return nextKeyValue(asideBuffer);
    } else if (inputBuffer.remaining() > 0) {
      return nextKeyValue(inputBuffer);
    } else {
      noMoreData = true;
      return false;
    }
  }

  /**
   * 从指定输入缓冲区解析下一个键值对
   * @param buffer 要读取的输入缓冲区
   * @return 解析成功返回true
   * @throws IOException 读取数据失败时抛出IO异常
   */
  private boolean nextKeyValue(InputBuffer buffer) throws IOException {
    if (closed) {
      return false;
    }
    
    // 重置Reader位置指向缓冲区起始
    nativeReader.reset(buffer);

    // 读取键长度，空间不足则扩容
    final int keyLength = nativeReader.readInt();
    if (keyBytes.length < keyLength) {
      keyBytes = new byte[keyLength];
    }

    // 读取值长度，空间不足则扩容
    final int valueLength = nativeReader.readInt();
    if (valueBytes.length < valueLength) {
      valueBytes = new byte[valueLength];
    }
    
    // 读取键、值字节数据
    IOUtils.readFully(nativeReader, keyBytes, 0, keyLength);
    IOUtils.readFully(nativeReader, valueBytes, 0, valueLength);

    // 重置输入缓冲区，准备返回给迭代器使用者
    keyBuffer.reset(keyBytes, keyLength);
    valueBuffer.reset(valueBytes, valueLength);

    return true;
  }

  /**
   * 接收从对端发送来的数据，处理不完整的键值对，将未完成的键值对存放到备用缓冲区
   * @return 接收成功返回true
   * @throws IOException 数据不完整或读取失败时抛出IO异常
   */
  @Override
  public boolean receiveData() throws IOException {
    if (closed) {
      return false;
    }
    
    final ByteBuffer input = inputBuffer.getByteBuffer();
    
    // 先把剩余数据填充到备用缓冲区
    if (null != asideBuffer && asideBuffer.length() > 0) {
      if (asideBuffer.remaining() > 0) {
        final byte[] output = asideBuffer.getByteBuffer().array();
        final int write = Math.min(asideBuffer.remaining(), input.remaining());
        input.get(output, asideBuffer.position(), write);
        asideBuffer.position(asideBuffer.position() + write);
      }

      if (asideBuffer.remaining() == 0) {
        asideBuffer.position(0);
      }
    }

    if (input.remaining() == 0) {
      return true;
    }

    // 数据长度不足一个KV头，说明数据不完整
    if (input.remaining() < KV_HEADER_LENGTH) {
      throw new IOException("incomplete data, input length is: " + input.remaining());
    }
    // 预读取KV头，计算整个键值对总长度
    final int position = input.position();
    final int keyLength = input.getInt();
    final int valueLength = input.getInt();
    input.position(position);
    final int kvLength = keyLength + valueLength + KV_HEADER_LENGTH;
    final int remaining = input.remaining();

    // 当前输入缓冲区剩余数据不足一个完整键值对，转存到备用缓冲区
    if (kvLength > remaining) {
      if (null == asideBuffer || asideBuffer.capacity() < kvLength) {
        asideBuffer = new InputBuffer(BufferType.HEAP_BUFFER, kvLength);
      }
      asideBuffer.rewind(0, kvLength);

      input.get(asideBuffer.array(), 0, remaining);
      asideBuffer.position(remaining);
    }
    return true;
  }

  @Override
  public Progress getProgress() {
    return null;
  }
  
  /**
   * Closes the iterator so that the underlying streams can be closed.
   */
  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }
    if (null != nativeReader) {
      nativeReader.close();
    }
    closed = true;
  }
}