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

import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.server.datanode.FSCachingGetSpaceUsed;
import org.apache.hadoop.hdfs.server.datanode.ReplicaInfo;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Objects;

/**
 * 文件所在模块：HDFS -> DataNode -> FsDataset 实现层
 * 核心职责：基于内存Replica信息缓存计算HDFS已用空间，提供更快更准确的空间统计
 * 
 * 本实现从FsDatasetImpl的内存volumeMap中读取Replica信息统计已用空间，
 * 相比DU（磁盘用量）实现：仅统计块文件和元数据文件，不包含临时文件、扫描缓存文件等额外文件，
 * 统计结果更准确，统计速度更快。可通过配置fs.getspaceused.classname启用本实现。
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ReplicaCachingGetSpaceUsed extends FSCachingGetSpaceUsed {
  static final Logger LOG =
      LoggerFactory.getLogger(ReplicaCachingGetSpaceUsed.class);

  // 副本拷贝慢日志阈值，超过该阈值输出debug日志
  private static final long DEEP_COPY_REPLICA_THRESHOLD_MS = 50;
  // 空间刷新慢日志阈值，超过该阈值输出debug日志
  private static final long REPLICA_CACHING_GET_SPACE_USED_THRESHOLD_MS = 1000;
  // 所属的FsVolumeImpl实例
  private final FsVolumeImpl volume;
  // 所属块池ID
  private final String bpid;

  /**
   * 构造函数，通过Builder构建ReplicaCachingGetSpaceUsed实例
   * @param builder 构造器，包含所需的volume和bpid等配置
   * @throws IOException 构造过程可能抛出IO异常
   */
  public ReplicaCachingGetSpaceUsed(Builder builder) throws IOException {
    super(builder);
    setShouldFirstRefresh(false);
    volume = builder.getVolume();
    bpid = builder.getBpid();
  }

  /**
   * 刷新HDFS已用空间统计，从内存Replica列表重新计算已用空间
   */
  @Override
  protected void refresh() {
    // 记录刷新开始时间
    long start = Time.monotonicNow();
    // 累计HDFS已用字节数
    long dfsUsed = 0;
    // 统计当前卷的副本数量
    long count = 0;

    // 获取当前卷所属的FsDataset实例
    FsDatasetSpi fsDataset = volume.getDataset();
    try {
      // 深拷贝当前块池的所有Replica信息，避免遍历时并发修改
      Collection<ReplicaInfo> replicaInfos =
          (Collection<ReplicaInfo>) fsDataset.deepCopyReplica(bpid);
      // 计算拷贝耗时
      long cost = Time.monotonicNow() - start;
      // 拷贝耗时超过阈值，输出debug日志
      if (cost > DEEP_COPY_REPLICA_THRESHOLD_MS) {
        LOG.debug(
            "Copy replica infos, blockPoolId: {}, replicas size: {}, "
                + "duration: {}ms",
            bpid, replicaInfos.size(), Time.monotonicNow() - start);
      }

      // 遍历拷贝得到的副本列表累加空间
      if (CollectionUtils.isNotEmpty(replicaInfos)) {
        for (ReplicaInfo replicaInfo : replicaInfos) {
          // 仅统计属于当前卷的副本
          if (Objects.equals(replicaInfo.getVolume().getStorageID(),
              volume.getStorageID())) {
            // 累加块文件占用磁盘空间
            dfsUsed += replicaInfo.getBytesOnDisk();
            // 累加元数据文件占用空间
            dfsUsed += replicaInfo.getMetadataLength();
            // 当前卷副本数计数+1
            count++;
          }
        }
      }

      // 更新缓存的已用空间值
      this.used.set(dfsUsed);
      // 计算整个刷新过程总耗时
      cost = Time.monotonicNow() - start;
      // 刷新耗时超过阈值，输出debug日志
      if (cost > REPLICA_CACHING_GET_SPACE_USED_THRESHOLD_MS) {
        LOG.debug(
            "Refresh dfs used, bpid: {}, replicas size: {}, dfsUsed: {} "
                + "on volume: {}, duration: {}ms",
            bpid, count, used, volume.getStorageID(),
            Time.monotonicNow() - start);
      }
    } catch (Exception e) {
      // 刷新过程异常，记录错误日志
      LOG.error("ReplicaCachingGetSpaceUsed refresh error", e);
    }
  }
}