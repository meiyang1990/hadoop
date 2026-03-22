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

import org.apache.hadoop.util.DirectBufferPool;
import org.apache.hadoop.classification.InterfaceAudience;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * 原生任务输入缓冲区，为MapReduce本地任务提供存储输入数据的缓冲区抽象
 * 支持直接内存缓冲区和堆内存缓冲区两种类型，支持从内存池分配直接缓冲区以优化性能
 */
@InterfaceAudience.Private
public class InputBuffer implements Closeable {

  // 全局直接内存缓冲区池，复用直接缓冲区减少GC
  static DirectBufferPool bufferPool = new DirectBufferPool();

  private ByteBuffer byteBuffer;
  private final BufferType type;

  /**
   * 构造指定类型和容量的输入缓冲区
   * @param type 缓冲区类型（直接内存/堆内存）
   * @param inputSize 缓冲区容量大小
   * @throws IOException 内存分配异常
   */
  public InputBuffer(BufferType type, int inputSize) throws IOException {

    final int capacity = inputSize;
    this.type = type;

    if (capacity > 0) {

      switch (type) {
      case DIRECT_BUFFER:
        // 从内存池获取直接缓冲区
        this.byteBuffer = bufferPool.getBuffer(capacity);
        // 设置为大端字节序，符合Hadoop序列化约定
        this.byteBuffer.order(ByteOrder.BIG_ENDIAN);
        break;
      case HEAP_BUFFER:
        // 在堆上分配缓冲区
        this.byteBuffer = ByteBuffer.allocate(capacity);
        // 设置为大端字节序，符合Hadoop序列化约定
        this.byteBuffer.order(ByteOrder.BIG_ENDIAN);
        break;
      }
      // 初始化位置和限制，准备接收新数据
      byteBuffer.position(0);
      byteBuffer.limit(0);
    }
  }

  /**
   * 获取当前缓冲区的类型
   * @return 缓冲区类型（直接内存/堆内存）
   */
  public BufferType getType() {
    return this.type;
  }

  /**
   * 包装已有字节数组构造堆输入缓冲区
   * @param bytes 待包装的字节数组
   */
  public InputBuffer(byte[] bytes) {
    this.type = BufferType.HEAP_BUFFER;
    if (bytes.length > 0) {
      this.byteBuffer = ByteBuffer.wrap(bytes);
      this.byteBuffer.order(ByteOrder.BIG_ENDIAN);
      byteBuffer.position(0);
      byteBuffer.limit(0);
    }
  }

  /**
   * 获取内部存储数据的ByteBuffer对象
   * @return 内部ByteBuffer
   */
  public ByteBuffer getByteBuffer() {
    return this.byteBuffer;
  }

  /**
   * 获取当前缓冲区中有效数据的长度
   * @return 有效数据长度
   */
  public int length() {
    if (null == byteBuffer) {
      return 0;
    }
    return byteBuffer.limit();
  }

  /**
   * 重置缓冲区的位置和限制，准备重新读取指定范围的数据
   * @param startOffset 起始读取位置
   * @param length 有效数据长度
   */
  public void rewind(int startOffset, int length) {
    if (null == byteBuffer) {
      return;
    }
    byteBuffer.position(startOffset);
    byteBuffer.limit(length);
  }

  /**
   * 获取缓冲区中剩余未读取的数据长度
   * @return 剩余未读取数据长度
   */
  public int remaining() {
    if (null == byteBuffer) {
      return 0;
    }
    return byteBuffer.remaining();
  }

  /**
   * 获取当前缓冲区的读取位置
   * @return 当前读取位置
   */
  public int position() {
    if (null == byteBuffer) {
      return 0;
    }
    return byteBuffer.position();
  }

  /**
   * 设置缓冲区的读取位置
   * @param pos 目标读取位置
   * @return 设置后的读取位置
   */
  public int position(int pos) {
    if (null == byteBuffer) {
      return 0;
    }

    byteBuffer.position(pos);
    return pos;
  }

  /**
   * 获取缓冲区的总容量
   * @return 缓冲区总容量
   */
  public int capacity() {
    if (null == byteBuffer) {
      return 0;
    }
    return byteBuffer.capacity();
  }

  /**
   * 获取堆缓冲区的底层字节数组
   * @return 底层字节数组，直接缓冲区返回null
   */
  public byte[] array() {
    if (null == byteBuffer) {
      return null;
    }
    return byteBuffer.array();
  }

  @Override
  /**
   * 关闭缓冲区，归还直接缓冲区到内存池
   */
  public void close() {
    if (byteBuffer != null && byteBuffer.isDirect()) {
      bufferPool.returnBuffer(byteBuffer);
      byteBuffer = null;
    }
  }
}