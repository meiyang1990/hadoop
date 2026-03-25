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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.ExtendedBlockId;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * 文件级注释：HDFS DataNode 持久化内存（PMEM）块映射实现类，表示一个被映射到持久化内存的HDFS数据块
 * 实现了MappableBlock接口，负责维护持久化内存中映射块的元信息，并提供资源释放能力
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class NativePmemMappedBlock implements MappableBlock {
  private static final Logger LOG =
      LoggerFactory.getLogger(NativePmemMappedBlock.class);

  // 持久化内存中块映射的起始地址
  private long pmemMappedAddress = -1L;
  // 映射块的字节长度
  private long length;
  // 块唯一标识，用于定位块
  private ExtendedBlockId key;

  /**
   * 构造函数：创建一个持久化内存映射块实例
   * @param pmemMappedAddress 持久化内存映射起始地址
   * @param length 映射块长度
   * @param key 块唯一标识
   */
  NativePmemMappedBlock(long pmemMappedAddress, long length,
      ExtendedBlockId key) {
    assert length > 0;
    this.pmemMappedAddress = pmemMappedAddress;
    this.length = length;
    this.key = key;
  }

  @Override
  /**
   * 获取映射块的字节长度
   * @return 映射块长度
   */
  public long getLength() {
    return length;
  }

  @Override
  /**
   * 获取映射块在持久化内存中的起始地址
   * @return 持久化内存映射起始地址
   */
  public long getAddress() {
    return pmemMappedAddress;
  }

  @Override
  /**
   * 获取当前块的唯一标识
   * @return 扩展块ID
   */
  public ExtendedBlockId getKey() {
    return key;
  }

  @Override
  /**
   * 关闭并释放持久化内存映射资源：取消内存映射并删除缓存文件
   */
  public void close() {
    // 仅处理已映射的块
    if (pmemMappedAddress != -1L) {
      try {
        // 获取当前块在PMEM缓存中的文件路径
        String cacheFilePath =
            PmemVolumeManager.getInstance().getCachePath(key);
        // 调用原生方法取消持久化内存映射
        boolean success =
            NativeIO.POSIX.Pmem.unmapBlock(pmemMappedAddress, length);
        if (!success) {
          throw new IOException("Failed to unmap the mapped file from " +
              "pmem address: " + pmemMappedAddress);
        }
        // 标记地址为无效，表示已释放
        pmemMappedAddress = -1L;
        // 删除本地缓存文件
        FsDatasetUtil.deleteMappedFile(cacheFilePath);
        LOG.info("Successfully uncached one replica:{} from persistent memory"
            + ", [cached path={}, length={}]", key, cacheFilePath, length);
      } catch (IOException e) {
        LOG.warn("IOException occurred for block {}!", key, e);
      }
    }
  }
}