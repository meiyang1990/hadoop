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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * 文件级注释：HDFS DataNode上存储在持久化内存(PMEM)中的内存映射块实现，
 * 基于Java映射字节缓冲区实现，不依赖PMDK库，实现MappableBlock接口定义块的基础操作能力。
 *
 * Represents an HDFS block that is mapped to persistent memory by DataNode
 * with mapped byte buffer. PMDK is NOT involved in this implementation.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class PmemMappedBlock implements MappableBlock {
  private static final Logger LOG =
      LoggerFactory.getLogger(PmemMappedBlock.class);
  private long length;
  private ExtendedBlockId key;

  /**
   * 构造函数：创建持久化内存映射块实例
   * @param length 块大小，单位字节
   * @param key 块的唯一标识（包含块ID和区块池ID）
   */
  PmemMappedBlock(long length, ExtendedBlockId key) {
    assert length > 0;
    this.length = length;
    this.key = key;
  }

  @Override
  public long getLength() {
    return length;
  }

  @Override
  public long getAddress() {
    return -1L;
  }

  @Override
  public ExtendedBlockId getKey() {
    return key;
  }

  /**
   * 关闭并清理持久化内存中的映射块，删除对应的缓存文件
   */
  @Override
  public void close() {
    String cacheFilePath = null;
    try {
      // 获取当前块在持久化内存中的缓存文件路径
      cacheFilePath =
          PmemVolumeManager.getInstance().getCachePath(key);
      // 删除映射的缓存文件，回收持久化内存空间
      FsDatasetUtil.deleteMappedFile(cacheFilePath);
      LOG.info("Successfully uncached one replica:{} from persistent memory"
          + ", [cached path={}, length={}]", key, cacheFilePath, length);
    } catch (IOException e) {
      LOG.warn("Failed to delete the mapped File: {}!", cacheFilePath, e);
    }
  }
}