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

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.nativetask.Constants;
import org.apache.hadoop.mapred.nativetask.buffer.BufferType;
import org.apache.hadoop.mapred.nativetask.buffer.ByteBufferDataReader;
import org.apache.hadoop.mapred.nativetask.buffer.InputBuffer;
import org.apache.hadoop.mapred.nativetask.serde.KVSerializer;
import org.apache.hadoop.mapred.nativetask.util.SizedWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 从Native Task输出缓冲区接收键值对数据，反序列化后写入Hadoop RecordWriter
 * 用于Native任务计算结果向Java框架流转，处理不完整数据的缓存与拼接
 * @param <OK> 输出键类型
 * @param <OV> 输出值类型
 */
@InterfaceAudience.Private
public class BufferPushee<OK, OV> implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(BufferPushee.class);
  
  // KV对头部长度：键长度+值长度各占4字节整数
  public final static int KV_HEADER_LENGTH = Constants.SIZEOF_KV_LENGTH;

  // 缓存不完整KV数据的备用缓冲区
  private InputBuffer asideBuffer;
  // 临时存储反序列化后的输出键
  private final SizedWritable<OK> tmpOutputKey;
  // 临时存储反序列化后的输出值
  private final SizedWritable<OV> tmpOutputValue;
  // 最终写入的Hadoop记录写出器
  private RecordWriter<OK, OV> writer;
  // 从ByteBuffer读取数据的reader
  private ByteBufferDataReader nativeReader;

  // KV对反序列化器
  private KVSerializer<OK, OV> deserializer;
  // 实例是否已关闭
  private boolean closed = false;

  /**
   * 构造BufferPushee，初始化反序列化工具与临时存储
   * @param oKClass 输出键类对象
   * @param oVClass 输出值类对象
   * @param writer 结果写出器
   * @throws IOException 初始化失败时抛出
   */
  public BufferPushee(Class<OK> oKClass, Class<OV> oVClass,
                      RecordWriter<OK, OV> writer) throws IOException {
    tmpOutputKey = new SizedWritable<OK>(oKClass);
    tmpOutputValue = new SizedWritable<OV>(oVClass);

    this.writer = writer;

    if (null != oKClass && null != oVClass) {
      this.deserializer = new KVSerializer<OK, OV>(oKClass, oVClass);
    }
    this.nativeReader = new ByteBufferDataReader(null);
  }

  /**
   * 从输入缓冲区收集Native输出的KV数据，处理不完整数据并写出
   * @param buffer 输入缓冲区，来自Native Task
   * @return 收集成功返回true，已关闭返回false
   * @throws IOException 数据不完整或写出失败时抛出
   */
  public boolean collect(InputBuffer buffer) throws IOException {
    if (closed) {
      return false;
    }
    
    final ByteBuffer input = buffer.getByteBuffer();
    // 处理上次剩余的未完成数据，先从当前缓冲区补全
    if (null != asideBuffer && asideBuffer.length() > 0) {
      if (asideBuffer.remaining() > 0) {
        final byte[] output = asideBuffer.getByteBuffer().array();
        // 计算本次可拷贝的字节数
        final int write = Math.min(asideBuffer.remaining(), input.remaining());
        // 从输入缓冲区拷贝数据到备用缓冲区
        input.get(output, asideBuffer.position(), write);
        // 更新备用缓冲区位置
        asideBuffer.position(asideBuffer.position() + write);
      }

      // 备用缓冲区已补全完整KV，写出
      if (asideBuffer.remaining() == 0 && asideBuffer.position() > 0) {
        asideBuffer.position(0);
        write(asideBuffer);
        // 重置备用缓冲区
        asideBuffer.rewind(0, 0);
      }
    }

    if (input.remaining() == 0) {
      return true;
    }

    // 检查剩余数据至少能放下KV头部
    if (input.remaining() < KV_HEADER_LENGTH) {
      throw new IOException("incomplete data, input length is: " + input.remaining());
    }
    // 读取KV长度信息
    final int position = input.position();
    final int keyLength = input.getInt();
    final int valueLength = input.getInt();
    // 恢复输入位置
    input.position(position);
    // 计算整个KV对总长度
    final int kvLength = keyLength + valueLength + KV_HEADER_LENGTH;
    final int remaining = input.remaining();

    // 当前缓冲区剩余字节不足一个完整KV，存入备用缓冲区
    if (kvLength > remaining) {
      // 如果备用缓冲区容量不足，重新分配
      if (null == asideBuffer || asideBuffer.capacity() < kvLength) {
        asideBuffer = new InputBuffer(BufferType.HEAP_BUFFER, kvLength);
      }
      // 重置备用缓冲区准备接收数据
      asideBuffer.rewind(0, kvLength);
      // 将当前缓冲区剩余数据拷贝到备用缓冲区
      input.get(asideBuffer.array(), 0, remaining);
      asideBuffer.position(remaining);
    } else {
      // 数据完整，直接写出
      write(buffer);
    }
    return true;
  }

  @SuppressWarnings("unchecked")
  /**
   * 从输入缓冲区反序列化所有KV对并写入RecordWriter
   * @param input 输入缓冲区
   * @return 写出成功返回true，已关闭返回false
   * @throws IOException 反序列化或写出失败时抛出
   */
  private boolean write(InputBuffer input) throws IOException {
    if (closed) {
      return false;
    }
    int totalRead = 0;
    final int remain = input.remaining();
    this.nativeReader.reset(input);
    // 循环反序列化直到读完当前缓冲区所有数据
    while (remain > totalRead) {
      final int read = deserializer.deserializeKV(nativeReader, tmpOutputKey, tmpOutputValue);
      if (read != 0) {
        totalRead += read;
        writer.write((OK) (tmpOutputKey.v), (OV) (tmpOutputValue.v));
      }
    }
    // 校验读取总字节数是否和预期一致
    if (remain != totalRead) {
      throw new IOException("We expect to read " + remain +
                            ", but we actually read: " + totalRead);
    }
    return true;
  }

  @Override
  /**
   * 关闭BufferPushee，释放资源并关闭底层写出器
   * @throws IOException 关闭失败时抛出
   */
  public void close() throws IOException {
    if (closed) {
      return;
    }
    if (null != writer) {
      writer.close(null);
    }
    if (null != nativeReader) {
      nativeReader.close();
    }
    closed = true;
  }
}