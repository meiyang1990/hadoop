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
package org.apache.hadoop.mapred.nativetask.buffer;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.mapred.nativetask.NativeDataTarget;

import org.apache.hadoop.util.Preconditions;

/**
 * 基于固定大小ByteBuffer实现的数据输出流，为Native Task提供带缓存的数据写入能力
 * 当ByteBuffer缓存填满时，会同步将缓冲区传递给下游NativeDataTarget处理
 * 是MapReduce本地任务IO路径中，Java层到Native层数据传递的缓存输出组件
 */
@InterfaceAudience.Private
public class ByteBufferDataWriter extends DataOutputStream {
  private final ByteBuffer buffer;
  private final NativeDataTarget target;

  private final static byte TRUE = (byte) 1;
  private final static byte FALSE = (byte) 0;
  private final java.io.DataOutputStream javaWriter;

  /**
   * 检查剩余空间，如果剩余空间不足以写入指定长度数据则触发刷新
   * @param length 待写入数据长度
   * @throws IOException 刷新时抛出IO异常
   */
  private void checkSizeAndFlushIfNecessary(int length) throws IOException {
    if (buffer.position() > 0 && buffer.remaining() < length) {
      flush();
    }
  }

  /**
   * 构造基于NativeDataTarget输出缓冲区的ByteBufferDataWriter
   * @param handler 下游数据处理目标，提供输出缓冲区并处理数据发送
   */
  public ByteBufferDataWriter(NativeDataTarget handler) {
    Preconditions.checkNotNull(handler);
    this.buffer = handler.getOutputBuffer().getByteBuffer();
    this.target = handler;
    this.javaWriter = new java.io.DataOutputStream(this);
  }

  @Override
  public synchronized void write(int v) throws IOException {
    // 检查空间，空间不足则刷新
    checkSizeAndFlushIfNecessary(1);
    buffer.put((byte) v);
  }

  @Override
  /**
   * 检查缓冲区剩余空间是否不足以容纳指定长度数据
   * @param dataLength 待写入数据长度
   * @return true表示剩余空间不足，false表示空间足够
   * @throws IOException IO异常
   */
  public boolean shortOfSpace(int dataLength) throws IOException {
    if (buffer.remaining() < dataLength) {
      return true;
    }
    return false;
  }

  @Override
  public synchronized void write(byte b[], int off, int len) throws IOException {
    int remain = len;
    int offset = off;
    // 循环写入，直到所有数据都写入缓冲区
    while (remain > 0) {
      int currentFlush = 0;
      if (buffer.remaining() > 0) {
        // 计算当前缓冲区能容纳的最大数据量
        currentFlush = Math.min(buffer.remaining(), remain);
        buffer.put(b, offset, currentFlush);
        remain -= currentFlush;
        offset += currentFlush;
      } else {
        // 缓冲区已满，刷新后继续写入
        flush();
      }
    }
  }

  @Override
  /**
   * 刷新缓冲区，将已缓存数据发送给下游目标，然后重置缓冲区 position 指针
   * @throws IOException 发送数据时抛出IO异常
   */
  public void flush() throws IOException {
    target.sendData();
    buffer.position(0);
  }

  @Override
  /**
   * 关闭输出流，如果存在未刷新数据则先刷新，然后通知下游数据发送完成
   * @throws IOException IO异常
   */
  public void close() throws IOException {
    if (hasUnFlushedData()) {
      flush();
    }
    target.finishSendData();
  }

  @Override
  public final void writeBoolean(boolean v) throws IOException {
    checkSizeAndFlushIfNecessary(1);
    buffer.put(v ? TRUE : FALSE);
  }

  @Override
  public final void writeByte(int v) throws IOException {
    checkSizeAndFlushIfNecessary(1);
    buffer.put((byte) v);
  }

  @Override
  public final void writeShort(int v) throws IOException {
    checkSizeAndFlushIfNecessary(2);
    buffer.putShort((short) v);
  }

  @Override
  public final void writeChar(int v) throws IOException {
    checkSizeAndFlushIfNecessary(2);
    buffer.put((byte) ((v >>> 8) & 0xFF));
    buffer.put((byte) ((v >>> 0) & 0xFF));
  }

  @Override
  public final void writeInt(int v) throws IOException {
    checkSizeAndFlushIfNecessary(4);
    buffer.putInt(v);
  }

  @Override
  public final void writeLong(long v) throws IOException {
    checkSizeAndFlushIfNecessary(8);
    buffer.putLong(v);
  }

  @Override
  public final void writeFloat(float v) throws IOException {
    checkSizeAndFlushIfNecessary(4);
    writeInt(Float.floatToIntBits(v));
  }

  @Override
  public final void writeDouble(double v) throws IOException {
    checkSizeAndFlushIfNecessary(8);
    writeLong(Double.doubleToLongBits(v));
  }

  @Override
  public final void writeBytes(String s) throws IOException {
    javaWriter.writeBytes(s);
  }

  @Override
  public final void writeChars(String s) throws IOException {
    javaWriter.writeChars(s);
  }

  @Override
  public final void writeUTF(String str) throws IOException {
    javaWriter.writeUTF(str);
  }

  @Override
  /**
   * 检查缓冲区是否存在未刷新的数据
   * @return true表示存在未刷新数据，false表示缓冲区为空
   */
  public boolean hasUnFlushedData() {
    return buffer.position() > 0;
  }
}