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
package org.apache.hadoop.hdfs.server.namenode.sps;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.sps.StoragePolicySatisfier.DatanodeMap;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorageReport;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 文件级注释：数据节点缓存管理器，为存储策略满足器SPS维护数据节点存储信息缓存
 * 
 * 该类由StoragePolicySatisfier实例化，负责缓存数据节点的存储报告信息。
 * 支持按配置的刷新间隔，定期从NameNode获取最新的活跃数据节点存储信息更新本地缓存，
 * 为块迁移任务调度提供可用数据节点的查询能力。
 */
@InterfaceAudience.Private
public class DatanodeCacheManager {
  private static final Logger LOG = LoggerFactory
      .getLogger(DatanodeCacheManager.class);

  // 缓存数据节点存储信息的映射表
  private final DatanodeMap datanodeMap;
  // 集群网络拓扑信息缓存
  private NetworkTopology cluster;

  /**
   * 缓存刷新间隔，单位毫秒
   */
  private final long refreshIntervalMs;

  // 上次访问缓存的时间戳
  private long lastAccessedTime;

  /**
   * 构造方法：初始化数据节点缓存管理器，从配置读取刷新间隔
   * @param conf Hadoop配置对象
   */
  public DatanodeCacheManager(Configuration conf) {
    refreshIntervalMs = conf.getLong(
        DFSConfigKeys.DFS_SPS_DATANODE_CACHE_REFRESH_INTERVAL_MS,
        DFSConfigKeys.DFS_SPS_DATANODE_CACHE_REFRESH_INTERVAL_MS_DEFAULT);

    LOG.info("DatanodeCacheManager refresh interval is {} milliseconds",
        refreshIntervalMs);
    datanodeMap = new DatanodeMap();
  }

  /**
   * 获取带有可用空间的活跃数据节点存储信息缓存，到期自动刷新
   * 
   * 从本地缓存返回数据节点信息，如果距离上次刷新已经超过配置间隔，
   * 则从NameNode获取最新的活跃数据节点存储报告更新本地缓存后返回。
   * 仅保留有剩余可用空间的数据节点，用于块迁移任务调度。
   *
   * @param spsContext SPS上下文，提供获取活跃数据节点和网络拓扑的能力
   * @return 缓存的数据节点存储信息映射表
   * @throws IOException 获取数据节点信息异常时抛出
   */
  public DatanodeMap getLiveDatanodeStorageReport(
      Context spsContext) throws IOException {
    // 获取当前 monotonic 时间戳
    long now = Time.monotonicNow();
    // 计算距离上次访问经过的时间
    long elapsedTimeMs = now - lastAccessedTime;
    // 判断是否需要刷新缓存
    boolean refreshNeeded = elapsedTimeMs >= refreshIntervalMs;
    // 更新上次访问时间为当前时间
    lastAccessedTime = now;
    if (refreshNeeded) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("elapsedTimeMs > refreshIntervalMs : {} > {},"
            + " so refreshing cache", elapsedTimeMs, refreshIntervalMs);
      }
      // 清空之前的缓存
      datanodeMap.reset(); // clear all previously cached items.

      // 从NameNode获取最新的活跃数据节点存储报告，构建新的缓存
      DatanodeStorageReport[] liveDns = spsContext
          .getLiveDatanodeStorageReport();
      // 遍历所有活跃数据节点
      for (DatanodeStorageReport storage : liveDns) {
        StorageReport[] storageReports = storage.getStorageReports();
        List<StorageType> storageTypes = new ArrayList<>();
        List<Long> remainingSizeList = new ArrayList<>();
        // 遍历数据节点的所有存储目录
        for (StorageReport t : storageReports) {
          // 仅保留剩余空间大于0的存储，用于块迁移
          if (t.getRemaining() > 0) {
            storageTypes.add(t.getStorage().getStorageType());
            remainingSizeList.add(t.getRemaining());
          }
        }
        // 将数据节点添加到缓存映射
        datanodeMap.addTarget(storage.getDatanodeInfo(), storageTypes,
            remainingSizeList);
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("LIVE datanodes: {}", datanodeMap);
      }
      // 更新集群网络拓扑缓存
      cluster = spsContext.getNetworkTopology(datanodeMap);
    }
    return datanodeMap;
  }

  /**
   * 获取缓存的集群网络拓扑信息
   * @return 当前缓存的集群网络拓扑
   */
  NetworkTopology getCluster() {
    return cluster;
  }
}