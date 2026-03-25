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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.Namesystem;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.hdfs.server.protocol.VolumeFailureSummary;
import org.apache.hadoop.hdfs.util.RwLockMode;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.StopWatch;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * 文件: HeartbeatManager.java
 * 所属模块: HDFS服务端 - 块管理
 * 核心职责: 管理来自DataNode的心跳汇报，定期检测过期心跳，识别并移除死亡/ stale节点，维护DataNode存储统计信息
 * 同步说明: DataNode列表和统计信息通过HeartbeatManager对象锁进行同步保护
 */
class HeartbeatManager implements DatanodeStatistics {
  static final Logger LOG = LoggerFactory.getLogger(HeartbeatManager.class);
  private static final String REPORT_DELTA_STALE_DN_HEADER =
      "StaleNodes Report: [New Stale Nodes]: %d";
  private static final String REPORT_STALE_DN_LINE_ENTRY = "%n\t %s";
  private static final String REPORT_STALE_DN_LINE_TAIL = ", %s";
  private static final String REPORT_REMOVE_DEAD_NODE_ENTRY =
      "StaleNodes Report: [Remove DeadNode]: %s";
  private static final String REPORT_REMOVE_STALE_NODE_ENTRY =
      "StaleNodes Report: [Remove StaleNode]: %s";
  private static final int REPORT_STALE_NODE_NODES_PER_LINE = 10;
  /**
   * 存储存活DataNode描述符列表，来源于DatanodeManager的datanodeMap子集
   * Monitor线程定期扫描移除过期节点，由HeartbeatManager锁同步保护
   */
  private final List<DatanodeDescriptor> datanodes = new ArrayList<>();

  /** DataNode存储统计信息，由HeartbeatManager锁同步保护 */
  private final DatanodeStats stats = new DatanodeStats();

  /** 过期心跳检查间隔时间 */
  private final long heartbeatRecheckInterval;
  /** 后台心跳监控线程 */
  private final Daemon heartbeatThread = new Daemon(new Monitor());
  /** 心跳检查耗时计时器，用于检测长时间GC停顿 */
  private final StopWatch heartbeatStopWatch = new StopWatch();
  /** 单次批量移除死亡DataNode的最大数量，避免一次性移除过多节点引发雪崩 */
  private final int numOfDeadDatanodesRemove;

  final Namesystem namesystem;
  final BlockManager blockManager;
  /** 是否启用stale节点变更日志记录 */
  private final boolean enableLogStaleNodes;

  /** 当前处于stale状态的DataNode集合 */
  private final Set<DatanodeDescriptor> staleDataNodes = new HashSet<>();

  /**
   * 构造HeartbeatManager实例，从配置加载各项参数并初始化
   * @param namesystem NameNode命名系统引用
   * @param blockManager 块管理器引用
   * @param conf Hadoop配置对象
   */
  HeartbeatManager(final Namesystem namesystem,
      final BlockManager blockManager, final Configuration conf) {
    this.namesystem = namesystem;
    this.blockManager = blockManager;
    boolean avoidStaleDataNodesForWrite = conf.getBoolean(
        DFSConfigKeys.DFS_NAMENODE_AVOID_STALE_DATANODE_FOR_WRITE_KEY,
        DFSConfigKeys.DFS_NAMENODE_AVOID_STALE_DATANODE_FOR_WRITE_DEFAULT);
    long recheckInterval = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY,
        DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_DEFAULT); // 5 min
    long staleInterval = conf.getLong(
        DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_KEY,
        DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_DEFAULT);// 30s
    enableLogStaleNodes = conf.getBoolean(
        DFSConfigKeys.DFS_NAMENODE_ENABLE_LOG_STALE_DATANODE_KEY,
        DFSConfigKeys.DFS_NAMENODE_ENABLE_LOG_STALE_DATANODE_DEFAULT);
    this.numOfDeadDatanodesRemove = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_REMOVE_DEAD_DATANODE_BATCHNUM_KEY,
        DFSConfigKeys.DFS_NAMENODE_REMOVE_BAD_BATCH_NUM_DEFAULT);

    // 如果开启写避开stale节点且stale间隔小于检查间隔，将检查间隔调整为stale间隔
    if (avoidStaleDataNodesForWrite && staleInterval < recheckInterval) {
      this.heartbeatRecheckInterval = staleInterval;
      LOG.info("Setting heartbeat recheck interval to " + staleInterval
          + " since " + DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_KEY
          + " is less than "
          + DFSConfigKeys.DFS_NAMENODE_HEARTBEAT_RECHECK_INTERVAL_KEY);
    } else {
      this.heartbeatRecheckInterval = recheckInterval;
    }
  }

  /**
   * 启动心跳监控后台线程
   */
  void activate() {
    heartbeatThread.start();
  }

  /**
   * 关闭心跳监控线程，释放资源
   */
  void close() {
    heartbeatThread.interrupt();
    try {
      // 线程未启动时调用无影响
      heartbeatThread.join(3000);
    } catch (InterruptedException ignored) {
    }
  }
  
  /**
   * 获取当前存活DataNode数量
   * @return 存活节点数
   */
  synchronized int getLiveDatanodeCount() {
    return datanodes.size();
  }

  @Override
  public long getCapacityTotal() {
    return stats.getCapacityTotal();
  }

  @Override
  public long getCapacityUsed() {
    return stats.getCapacityUsed();
  }

  @Override
  public float getCapacityUsedPercent() {
    return stats.getCapacityUsedPercent();
  }

  @Override
  public long getCapacityRemaining() {
    return stats.getCapacityRemaining();
  }

  @Override
  public float getCapacityRemainingPercent() {
    return stats.getCapacityRemainingPercent();
  }

  @Override
  public long getBlockPoolUsed() {
    return stats.getBlockPoolUsed();
  }

  @Override
  public float getPercentBlockPoolUsed() {
    return stats.getPercentBlockPoolUsed();
  }

  @Override
  public long getCapacityUsedNonDFS() {
    return stats.getCapacityUsedNonDFS();
  }

  @Override
  public int getXceiverCount() {
    return stats.getXceiverCount();
  }
  
  @Override
  public int getInServiceXceiverCount() {
    return stats.getNodesInServiceXceiverCount();
  }
  
  @Override
  public int getNumDatanodesInService() {
    return stats.getNodesInService();
  }

  @Override
  public int getInServiceAvailableVolumeCount() {
    return stats.getNodesInServiceAvailableVolumeCount();
  }
  
  @Override
  public long getCacheCapacity() {
    return stats.getCacheCapacity();
  }

  @Override
  public long getCacheUsed() {
    return stats.getCacheUsed();
  }

  @Override
  public synchronized long[] getStats() {
    return new long[] {getCapacityTotal(),
                       getCapacityUsed(),
                       getCapacityRemaining(),
                       -1L,
                       -1L,
                       -1L,
                       -1L,
                       -1L,
                       -1L};
  }

  @Override
  public int getExpiredHeartbeats() {
    return stats.getExpiredHeartbeats();
  }

  @Override
  public Map<StorageType, StorageTypeStats> getStorageTypeStats() {
    return stats.getStatsMap();
  }

  @Override
  public long getProvidedCapacity() {
    return blockManager.getProvidedCapacity();
  }

  /**
   * 注册新DataNode到心跳管理器
   * @param d 待注册的DataNode描述符
   */
  synchronized void register(final DatanodeDescriptor d) {
    if (!d.isAlive()) {
      addDatanode(d);

      // 更新心跳时间戳
      d.updateHeartbeatState(StorageReport.EMPTY_ARRAY, 0L, 0L, 0, 0, null);
      stats.add(d);
    }
  }

  /**
   * 获取所有存活DataNode数组
   * @return 存活DataNode描述符数组
   */
  synchronized DatanodeDescriptor[] getDatanodes() {
    return datanodes.toArray(new DatanodeDescriptor[datanodes.size()]);
  }

  /**
   * 添加DataNode到存活列表
   * @param d 待添加的DataNode描述符
   */
  synchronized void addDatanode(final DatanodeDescriptor d) {
    // 更新in-service节点计数
    datanodes.add(d);
    d.setAlive(true);
  }

  /**
   * 更新DataNode统计信息
   * @param d 需要更新的DataNode描述符
   */
  void updateDnStat(final DatanodeDescriptor d){
    stats.add(d);
  }

  /**
   * 从心跳管理器移除指定DataNode
   * @param node 待移除DataNode描述符
   */
  synchronized void removeDatanode(DatanodeDescriptor node) {
    if (node.isAlive()) {
      stats.subtract(node);
      datanodes.remove(node);
      removeNodeFromStaleList(node);
      node.setAlive(false);
    }
  }

  /**
   * 处理DataNode上报的心跳，更新节点状态和统计信息
   * @param node 上报心跳的DataNode节点
   * @param reports 存储汇报数组
   * @param cacheCapacity 缓存总容量
   * @param cacheUsed 已用缓存容量
   * @param xceiverCount 流式线程数量
   * @param failedVolumes 失败卷数量
   * @param volumeFailureSummary 卷故障汇总信息
   */
  synchronized void updateHeartbeat(final DatanodeDescriptor node,
      StorageReport[] reports, long cacheCapacity, long cacheUsed,
      int xceiverCount, int failedVolumes,
      VolumeFailureSummary volumeFailureSummary) {
    stats.subtract(node);
    try {
      blockManager.updateHeartbeat(node, reports, cacheCapacity, cacheUsed,
          xceiverCount, failedVolumes, volumeFailureSummary);
    } finally {
      stats.add(node);
    }
  }

  /**
   * 处理DataNode上报的生命线消息，仅更新状态不修改心跳注册标记
   * @param node 上报生命线的DataNode节点
   * @param reports 存储汇报数组
   * @param cacheCapacity 缓存总容量
   * @param cacheUsed 已用缓存容量
   * @param xceiverCount 流式线程数量
   * @param failedVolumes 失败卷数量
   * @param volumeFailureSummary 卷故障汇总信息
   */
  synchronized void updateLifeline(final DatanodeDescriptor node,
      StorageReport[] reports, long cacheCapacity, long cacheUsed,
      int xceiverCount, int failedVolumes,
      VolumeFailureSummary volumeFailureSummary) {
    stats.subtract(node);
    try {
      // 此处 intentionally 调用updateHeartbeatState而非updateHeartbeat
      // 因为生命线消息不算注册后的首次心跳，不需要修改heartbeatedSinceRegistration标记
      blockManager.updateHeartbeatState(node, reports, cacheCapacity, cacheUsed,
          xceiverCount, failedVolumes, volumeFailureSummary);
    } finally {
      stats.add(node);
    }
  }

  /**
   * 启动DataNode退役流程
   * @param node 待退役DataNode节点
   */
  synchronized void startDecommission(final DatanodeDescriptor node) {
    if (!node.isAlive()) {
      LOG.info("Dead node {} is decommissioned immediately.", node);
      node.setDecommissioned();
    } else {
      stats.subtract(node);
      node.startDecommission();
      stats.add(node);
    }
  }

  /**
   * 启动DataNode维护状态流程
   * @param node 进入维护状态的DataNode节点
   */
  synchronized void startMaintenance(final DatanodeDescriptor node) {
    if (!node.isAlive()) {
      LOG.info("Dead node {} is put in maintenance state immediately.", node);
      node.setInMaintenance();
    } else {
      stats.subtract(node);
      if (node.isDecommissioned()) {
        LOG.info("Decommissioned node " + node + " is put in maintenance state"
            + " immediately.");
        node.setInMaintenance();
      } else if (blockManager.getMinReplicationToBeInMaintenance() == 0) {
        LOG.info("MinReplicationToBeInMaintenance is set to zero. " + node +
            " is put in maintenance state" + " immediately.");
        node.setInMaintenance();
      } else {
        node.startMaintenance();
      }
      stats.add(node);
    }
  }

  /**
   * 停止DataNode维护状态
   * @param node 退出维护状态的DataNode节点
   */
  synchronized void stopMaintenance(final DatanodeDescriptor node) {
    LOG.info("Stopping maintenance of {} node {}",
        node.isAlive() ? "live" : "dead", node);
    if (!node.isAlive()) {
      node.stopMaintenance();
    } else {
      stats.subtract(node);
      node.stopMaintenance();
      stats.add(node);
    }
  }

  /**
   * 停止DataNode退役流程
   * @param node 停止退役的DataNode节点
   */
  synchronized void stopDecommission(final DatanodeDescriptor node) {
    LOG.info("Stopping decommissioning of {} node {}",
        node.isAlive() ? "live" : "dead", node);
    if (!node.isAlive()) {
      node.stopDecommission();
    } else {
      stats.subtract(node);
      node.stopDecommission();
      stats.add(node);
    }
  }

  @VisibleForTesting
  void restartHeartbeatStopWatch() {
    heartbeatStopWatch.reset().start();
  }

  @VisibleForTesting
  boolean shouldAbortHeartbeatCheck(long offset) {
    long elapsed = heartbeatStopWatch.now(TimeUnit.MILLISECONDS);
    return elapsed + offset > heartbeatRecheckInterval;
  }

  /**
   * 从stale节点列表移除节点，仅用于节点死亡场景
   * 本方法需在同步块内调用
   * @param d 待移除节点描述符
   * @return 节点原本是否在stale列表中
   */
  private boolean removeNodeFromStaleList(DatanodeDescriptor d) {
    return removeNodeFromStaleList(d, true);
  }

  /**
   * 从stale节点列表移除节点，支持区分死亡移除和恢复非stale移除
   * 本方法需在同步块内调用
   * @param d 待移除节点描述符
   * @param isDead 是否因节点死亡移除
   * @return 节点原本是否在stale列表中
   */
  private boolean removeNodeFromStaleList(DatanodeDescriptor d,
      boolean isDead) {
    boolean result = false;
    result = staleDataNodes.remove(d);
    if (enableLogStaleNodes && result) {
      LOG.info(String.format(isDead ?
              REPORT_REMOVE_DEAD_NODE_ENTRY : REPORT_REMOVE_STALE_NODE_ENTRY,
          d));
    }
    return result;
  }

  /**
   * 输出本轮检查新增的stale节点日志
   * @param staleNodes 本轮新增的stale节点列表
   */
  private void dumpStaleNodes(List<DatanodeDescriptor> staleNodes) {
    // 开启日志且有新增stale节点才输出
    if (enableLogStaleNodes && (!staleNodes.isEmpty())) {
      StringBuilder staleLogMSG =
          new StringBuilder(String.format(REPORT_DELTA_STALE_DN_HEADER,
              staleNodes.size()));
      for (int ind = 0; ind < staleNodes.size(); ind++) {
        String logFormat = (ind % REPORT_STALE_NODE_NODES_PER_LINE == 0) ?
            REPORT_STALE_DN_LINE_ENTRY