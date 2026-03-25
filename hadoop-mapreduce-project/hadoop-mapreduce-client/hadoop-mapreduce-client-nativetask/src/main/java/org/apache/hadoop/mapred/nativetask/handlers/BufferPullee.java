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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.mapred.RawKeyValueIterator;
import org.apache.hadoop.mapred.nativetask.Constants;
import org.apache.hadoop.mapred.nativetask.NativeDataTarget;
import org.apache.hadoop.mapred.nativetask.buffer.ByteBufferDataWriter;
import org.apache.hadoop.mapred.nativetask.buffer.OutputBuffer;
import org.apache.hadoop.mapred.nativetask.serde.KVSerializer;
import org.apache.hadoop.mapred.nativetask.util.SizedWritable;

/**
 * 为Native Task从Java端拉取排序后的键值对数据，填充到输出缓冲区供Native层消费
 * 
 * 响应Native端{@link BufferPuller}的拉取请求，将Java迭代器中的键值对序列化后写入输出缓冲区
 */
@InterfaceAudience.Private
public class BufferPullee<IK, IV> implements IDataLoader {

  public static final int KV_HEADER_LENGTH = Constants.SIZEOF_KV_LENGTH;

  private final SizedWritable<IK> tmpInputKey;
  private final SizedWritable<IV> tmpInputValue;
  private boolean inputKVBufferd = false;
  private RawKeyValueIterator rIter;
  private ByteBufferDataWriter nativeWriter;
  protected KVSerializer<IK, IV> serializer;
  private final OutputBuffer outputBuffer;
  private final NativeDataTarget target;
  private boolean closed = false;
  
  /**
   * 构造BufferPullee，初始化键值对序列化器和输出缓冲区
   * @param iKClass 键类型Class对象
   * @param iVClass 值类型Class对象
   * @param rIter 排序后键值对迭代器
   * @param target Native数据输出目标
   * @throws IOException 初始化失败时抛出IO异常
   */
  public BufferPullee(Class<IK> iKClass, Class<IV> iVClass,
                      RawKeyValueIterator rIter, NativeDataTarget target)
      throws IOException {
    this.rIter = rIter;
    tmpInputKey = new SizedWritable<IK>(iKClass);
    tmpInputValue = new SizedWritable<IV>(iVClass);

    if (null != iKClass && null != iVClass) {
      this.serializer = new KVSerializer<IK, IV>(iKClass, iVClass);
    }
    this.outputBuffer = target.getOutputBuffer();
    this.target = target;
  }

  /**
   * 从键值对迭代器加载数据，序列化后写入Native输出缓冲区
   * @return 本次写入缓冲区的字节数，已关闭则返回0
   * @throws IOException IO操作失败时抛出异常
   */
  @Override
  public int load() throws IOException {
    if (closed) {
      return 0;
    }
    
    if (null == outputBuffer) {
      throw new IOException("output buffer not set");
    }

    this.nativeWriter = new ByteBufferDataWriter(target);
    // 重置输出缓冲区指针，准备写入新数据
    outputBuffer.rewind();

    int written = 0;
    boolean firstKV = true;

    // 处理上一次加载剩余未写入的键值对
    if (inputKVBufferd) {
      written += serializer.serializeKV(nativeWriter, tmpInputKey, tmpInputValue);
      inputKVBufferd = false;
      firstKV = false;
    }

    // 遍历迭代器，持续写入键值对直到缓冲区满
    while (rIter.next()) {
      inputKVBufferd = false;
      // 读取当前键值对到临时对象
      tmpInputKey.readFields(rIter.getKey());
      tmpInputValue.readFields(rIter.getValue());
      // 更新键值对长度信息
      serializer.updateLength(tmpInputKey, tmpInputValue);

      // 计算当前键值对总大小（含头信息）
      final int kvSize = tmpInputKey.length + tmpInputValue.length + KV_HEADER_LENGTH;

      // 缓冲区剩余空间不足容纳当前键值对，缓存后退出
      if (!firstKV && nativeWriter.shortOfSpace(kvSize)) {
        inputKVBufferd = true;
        break;
      } else {
        // 序列化写入当前键值对
        written += serializer.serializeKV(nativeWriter, tmpInputKey, tmpInputValue);
        firstKV = false;
      }
    }

    // 刷新未写出的数据到缓冲区
    if (nativeWriter.hasUnFlushedData()) {
      nativeWriter.flush();
    }
    return written;
  }

  /**
   * 关闭资源，释放迭代器和写入器
   * @throws IOException 关闭失败时抛出IO异常
   */
  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }
    if (null != rIter) {
      rIter.close();
    }
    if (null != nativeWriter) {
      nativeWriter.close();
    }
    closed = true;
  }
}