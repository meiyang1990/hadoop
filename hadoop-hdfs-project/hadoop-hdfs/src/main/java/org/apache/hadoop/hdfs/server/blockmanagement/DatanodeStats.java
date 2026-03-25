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
package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;

import java.util.EnumMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Datanode整体统计信息聚合类，用于聚合集群中所有DataNode的容量、存储、心跳等统计数据。
 * 退役中/已退役节点仅统计已使用容量，不统计总容量。核心用于NameNode侧的集群容量汇总展示。
 */
class DatanodeStats {

  private final StorageTypeStatsMap statsMap = new StorageTypeStatsMap();
  // 总容量（仅统计服务中节点）
  private long capacityTotal = 0L;
  // DFS已使用容量（仅统计服务中节点）
  private long capacityUsed = 0L;
  // 非Dfs已使用容量（仅统计服务中节点）
  private long capacityUsedNonDfs = 0L;
  // 剩余可用容量（仅统计服务中节点）
  private long capacityRemaining = 0L;
  // 块池已使用容量（仅统计服务中节点）
  private long blockPoolUsed = 0L;
  // 所有节点数据传输线程总数
  private int xceiverCount = 0;
  // 缓存总容量
  private long cacheCapacity = 0L;
  // 缓存已使用容量
  private long cacheUsed = 0L;

  // 服务中节点数量
  private int nodesInService = 0;
  // 服务中节点数据传输线程总数
  private int nodesInServiceXceiverCount = 0;
  // 服务中节点可用卷总数量
  private int nodesInServiceAvailableVolumeCount = 0;
  // 过期心跳总次数
  private int expiredHeartbeats = 0;

  /**
   * 将指定DataNode的统计信息添加到聚合统计中。
   * 根据节点状态（服务中/退役中/进入维护）累加不同维度的统计数据。
   * @param node 待添加的DataNode描述符
   */
  synchronized void add(final DatanodeDescriptor node) {
    xceiverCount += node.getXceiverCount();
    if (node.isInService()) {
      // 服务中节点累加所有维度统计
      capacityUsed += node.getDfsUsed();
      capacityUsedNonDfs += node.getNonDfsUsed();
      blockPoolUsed += node.getBlockPoolUsed();
      nodesInService++;
      nodesInServiceXceiverCount += node.getXceiverCount();
      capacityTotal += node.getCapacity();
      capacityRemaining += node.getRemaining();
      cacheCapacity += node.getCacheCapacity();
      cacheUsed += node.getCacheUsed();
      nodesInServiceAvailableVolumeCount += node.getNumVolumesAvailable();
    } else if (node.isDecommissionInProgress() ||
        node.isEnteringMaintenance()) {
      // 退役中/维护中节点仅累加缓存统计
      cacheCapacity += node.getCacheCapacity();
      cacheUsed += node.getCacheUsed();
    }
    // 按存储类型聚合存储信息，去重避免同一节点同一存储类型多次计数
    Set<StorageType> storageTypes = new HashSet<>();
    for (DatanodeStorageInfo storageInfo : node.getStorageInfos()) {
      if (storageInfo.getState() != DatanodeStorage.State.FAILED) {
        // 累加存储单元统计
        statsMap.addStorage(storageInfo, node);
        storageTypes.add(storageInfo.getStorageType());
      }
    }
    // 每个存储类型累加节点计数
    for (StorageType storageType : storageTypes) {
      statsMap.addNode(storageType, node);
    }
  }

  /**
   * 从聚合统计中移除指定DataNode的统计信息。
   * 根据节点状态（服务中/退役中/进入维护）减去对应维度的统计数据。
   * @param node 待移除的DataNode描述符
   */
  synchronized void subtract(final DatanodeDescriptor node) {
    xceiverCount -= node.getXceiverCount();
    if (node.isInService()) {
      // 服务中节点减去所有维度统计
      capacityUsed -= node.getDfsUsed();
      capacityUsedNonDfs -= node.getNonDfsUsed();
      blockPoolUsed -= node.getBlockPoolUsed();
      nodesInService--;
      nodesInServiceXceiverCount -= node.getXceiverCount();
      capacityTotal -= node.getCapacity();
      capacityRemaining -= node.getRemaining();
      cacheCapacity -= node.getCacheCapacity();
      cacheUsed -= node.getCacheUsed();
      nodesInServiceAvailableVolumeCount -= node.getNumVolumesAvailable();
    } else if (node.isDecommissionInProgress() ||
        node.isEnteringMaintenance()) {
      // 退役中/维护中节点仅减去缓存统计
      cacheCapacity -= node.getCacheCapacity();
      cacheUsed -= node.getCacheUsed();
    }
    // 按存储类型移除存储信息，去重避免同一节点同一存储类型多次减计数
    Set<StorageType> storageTypes = new HashSet<>();
    for (DatanodeStorageInfo storageInfo : node.getStorageInfos()) {
      if (storageInfo.getState() != DatanodeStorage.State.FAILED) {
        // 减去存储单元统计
        statsMap.subtractStorage(storageInfo, node);
        storageTypes.add(storageInfo.getStorageType());
      }
    }
    // 每个存储类型减去节点计数
    for (StorageType storageType : storageTypes) {
      statsMap.subtractNode(storageType, node);
    }
  }

  /** Increment expired heartbeat counter. */
  void incrExpiredHeartbeats() {
    expiredHeartbeats++;
  }

  synchronized Map<StorageType, StorageTypeStats> getStatsMap() {
    return statsMap.get();
  }

  synchronized long getCapacityTotal() {
    return capacityTotal;
  }

  synchronized long getCapacityUsed() {
    return capacityUsed;
  }

  synchronized long getCapacityRemaining() {
    return capacityRemaining;
  }

  synchronized long getBlockPoolUsed() {
    return blockPoolUsed;
  }

  synchronized int getXceiverCount() {
    return xceiverCount;
  }

  synchronized long getCacheCapacity() {
    return cacheCapacity;
  }

  synchronized long getCacheUsed() {
    return cacheUsed;
  }

  synchronized int getNodesInService() {
    return nodesInService;
  }

  synchronized int getNodesInServiceXceiverCount() {
    return nodesInServiceXceiverCount;
  }

  synchronized int getNodesInServiceAvailableVolumeCount() {
    return nodesInServiceAvailableVolumeCount;
  }

  synchronized int getExpiredHeartbeats() {
    return expiredHeartbeats;
  }

  synchronized float getCapacityRemainingPercent() {
    // 计算剩余容量占总容量百分比
    return DFSUtilClient.getPercentRemaining(capacityRemaining, capacityTotal);
  }

  synchronized float getPercentBlockPoolUsed() {
    // 计算块池已用占总容量百分比
    return DFSUtilClient.getPercentUsed(blockPoolUsed, capacityTotal);
  }

  synchronized long getCapacityUsedNonDFS() {
    return capacityUsedNonDfs;
  }

  synchronized float getCapacityUsedPercent() {
    // 计算Dfs已用占总容量百分比
    return DFSUtilClient.getPercentUsed(capacityUsed, capacityTotal);
  }

  /**
   * 按存储类型分类的统计信息内部容器，维护每种存储类型的聚合统计。
   */
  static final class StorageTypeStatsMap {

    private Map<StorageType, StorageTypeStats> storageTypeStatsMap =
        new EnumMap<>(StorageType.class);

    /**
     * 获取所有存储类型统计的副本，避免外部修改内部状态。
     * @return 存储类型统计映射表副本
     */
    private Map<StorageType, StorageTypeStats> get() {
      return new EnumMap<>(storageTypeStatsMap);
    }

    /**
     * 向指定存储类型添加一个DataNode节点统计。
     * @param storageType 存储类型
     * @param node 待添加的DataNode描述符
     */
    private void addNode(StorageType storageType,
        final DatanodeDescriptor node) {
      StorageTypeStats storageTypeStats =
          storageTypeStatsMap.get(storageType);
      if (storageTypeStats == null) {
        storageTypeStats = new StorageTypeStats(storageType);
        storageTypeStatsMap.put(storageType, storageTypeStats);
      }
      storageTypeStats.addNode(node);
    }

    /**
     * 向指定存储类型添加一个存储单元统计。
     * @param info 存储单元信息
     * @param node 所属DataNode描述符
     */
    private void addStorage(final DatanodeStorageInfo info,
        final DatanodeDescriptor node) {
      StorageTypeStats storageTypeStats =
          storageTypeStatsMap.get(info.getStorageType());
      if (storageTypeStats == null) {
        storageTypeStats = new StorageTypeStats(info.getStorageType());
        storageTypeStatsMap.put(info.getStorageType(), storageTypeStats);
      }
      storageTypeStats.addStorage(info, node);
    }

    /**
     * 从指定存储类型移除一个存储单元统计。
     * @param info 存储单元信息
     * @param node 所属DataNode描述符
     */
    private void subtractStorage(final DatanodeStorageInfo info,
        final DatanodeDescriptor node) {
      StorageTypeStats storageTypeStats =
          storageTypeStatsMap.get(info.getStorageType());
      if (storageTypeStats != null) {
        storageTypeStats.subtractStorage(info, node);
      }
    }

    /**
     * 从指定存储类型移除一个DataNode节点统计。如果移除后该存储类型无服务中节点，则从映射表中删除。
     * @param storageType 存储类型
     * @param node 待移除的DataNode描述符
     */
    private void subtractNode(StorageType storageType,
        final DatanodeDescriptor node) {
      StorageTypeStats storageTypeStats = storageTypeStatsMap.get(storageType);
      if (storageTypeStats != null) {
        storageTypeStats.subtractNode(node);
        if (storageTypeStats.getNodesInService() == 0) {
          // 该存储类型已无服务中节点，清理映射项
          storageTypeStatsMap.remove(storageType);
        }
      }
    }
  }
}