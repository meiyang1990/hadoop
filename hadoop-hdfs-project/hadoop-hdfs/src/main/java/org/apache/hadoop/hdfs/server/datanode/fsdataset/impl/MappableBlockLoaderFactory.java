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
import org.apache.hadoop.hdfs.server.datanode.DNConf;
import org.apache.hadoop.io.nativeio.NativeIO;

/**
 * 文件路径: hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/fsdataset/impl/MappableBlockLoaderFactory.java
 * 
 * 可映射块加载器工厂，用于根据DataNode配置创建对应类型的块缓存加载器实例。
 * 支持普通DRAM内存缓存和持久化内存(PMEM)缓存两种场景，根据配置和环境选择实现。
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class MappableBlockLoaderFactory {

  /**
   * 工具类禁止实例化
   */
  private MappableBlockLoaderFactory() {
    // Prevent instantiation
  }

  /**
   * 根据DataNode配置创建对应类型的可映射块加载器。
   * 未配置持久化内存卷时返回DRAM缓存加载器；
   * 配置了持久化内存卷且支持PMDK原生库时返回原生PMEM加载器；
   * 否则返回纯Java实现的PMEM加载器。
   * @param conf DataNode配置对象
   * @return 对应类型的MappableBlockLoader实例
   */
  public static MappableBlockLoader createCacheLoader(DNConf conf) {
    // 未配置持久化内存卷，使用DRAM内存映射加载器
    if (conf.getPmemVolumes() == null || conf.getPmemVolumes().length == 0) {
      return new MemoryMappableBlockLoader();
    }
    // 检查原生IO和PMDK支持，可用则返回原生PMEM加载器
    if (NativeIO.isAvailable() && NativeIO.POSIX.isPmdkAvailable()) {
      return new NativePmemMappableBlockLoader();
    }
    // 返回纯Java实现的PMEM加载器
    return new PmemMappableBlockLoader();
  }
}