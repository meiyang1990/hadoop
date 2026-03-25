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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.hadoop.classification.InterfaceAudience;

/**
 * 原生任务输出缓冲区，用于封装不同类型的字节缓冲区，为MapReduce本地任务输出数据提供存储容器
 * 支持直接内存缓冲区和堆内存缓冲区两种类型，管理缓冲区的位置、容量等状态
 */
@InterfaceAudience.Private
public class OutputBuffer {
  protected ByteBuffer byteBuffer;
  private final BufferType type;

  /**
   * 构造指定类型和容量的输出缓冲区
   * @param type 缓冲区类型（直接内存/堆内存）
   * @param outputBufferCapacity 缓冲区容量
   */
  public OutputBuffer(BufferType type, int outputBufferCapacity) {

    this.type = type;
    if (outputBufferCapacity > 0) {
      switch (type) {
      case DIRECT_BUFFER:
        // 分配直接内存缓冲区
        this.byteBuffer = ByteBuffer.allocateDirect(outputBufferCapacity);
        // 设置为大端字节序，兼容原生C代码处理
        this.byteBuffer.order(ByteOrder.BIG_ENDIAN);
        break;
      case HEAP_BUFFER:
        // 分配堆内存缓冲区
        this.byteBuffer = ByteBuffer.allocate(outputBufferCapacity);
        // 设置为大端字节序，兼容原生C代码处理
        this.byteBuffer.order(ByteOrder.BIG_ENDIAN);
        break;
      }
    }
  }

  /**
   * 基于已有字节数组构造堆输出缓冲区
   * @param bytes 用于包装的字节数组
   */
  public OutputBuffer(byte[] bytes) {
    this.type = BufferType.HEAP_BUFFER;
    final int outputBufferCapacity = bytes.length;
    if (outputBufferCapacity > 0) {
      // 包装已有字节数组作为缓冲区
      this.byteBuffer = ByteBuffer.wrap(bytes);
      // 设置为大端字节序，兼容原生C代码处理
      this.byteBuffer.order(ByteOrder.BIG_ENDIAN);
      // 重置缓冲区位置到起始处
      this.byteBuffer.position(0);
    }
  }

  /**
   * 获取缓冲区类型
   * @return 缓冲区类型（直接内存/堆内存）
   */
  public BufferType getType() {
    return this.type;
  }

  /**
   * 获取内部封装的字节缓冲区
   * @return NIO字节缓冲区实例
   */
  public ByteBuffer getByteBuffer() {
    return this.byteBuffer;
  }

  /**
   * 获取已写入数据的长度
   * @return 已写入数据的字节数
   */
  public int length() {
    return byteBuffer.position();
  }

  /**
   * 重置缓冲区位置，准备重新写入数据
   */
  public void rewind() {
    byteBuffer.position(0);
  }

  /**
   * 获取缓冲区容量上限
   * @return 缓冲区最大可写入字节数
   */
  public int limit() {
    return byteBuffer.limit();
  }
}