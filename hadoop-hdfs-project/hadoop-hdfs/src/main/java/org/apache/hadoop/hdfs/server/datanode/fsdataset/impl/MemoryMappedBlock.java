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

package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import java.nio.MappedByteBuffer;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.ExtendedBlockId;
import org.apache.hadoop.io.nativeio.NativeIO;

/**
 * 文件作用：HDFS DataNode节点上内存映射块的实现类，封装将HDFS数据块映射到内存的相关操作
 * 核心职责：维护内存映射缓冲区引用，提供内存映射块的生命周期管理，支持正确释放内存映射资源
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class MemoryMappedBlock implements MappableBlock {
  private MappedByteBuffer mmap;
  private final long length;

  /**
   * 构造内存映射块对象，绑定内存映射缓冲区和块长度
   * @param mmap 内存映射缓冲区对象
   * @param length 数据块总长度
   */
  MemoryMappedBlock(MappedByteBuffer mmap, long length) {
    this.mmap = mmap;
    this.length = length;
    assert length > 0;
  }

  /**
   * 获取内存映射块的总长度
   * @return 数据块长度（字节）
   */
  @Override
  public long getLength() {
    return length;
  }

  /**
   * 获取内存映射块的起始内存地址，当前实现不支持直接返回地址
   * @return 固定返回-1，表示不提供直接地址访问
   */
  @Override
  public long getAddress() {
    return -1L;
  }

  /**
   * 获取内存映射块对应的唯一键，当前实现不维护块标识信息
   * @return 固定返回null
   */
  @Override
  public ExtendedBlockId getKey() {
    return null;
  }

  /**
   * 关闭并释放内存映射资源，调用原生方法解除内存映射
   */
  @Override
  public void close() {
    if (mmap != null) {
      // 调用原生接口释放内存映射
      NativeIO.POSIX.munmap(mmap);
      mmap = null;
    }
  }
}