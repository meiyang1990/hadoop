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

import static org.apache.hadoop.util.Time.monotonicNow;

import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.Namesystem;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * HDFS DataNode退役和维护状态管理器，负责管理DataNode的退役、上线维护和进入维护等生命周期操作。
 * 通过后台定时监控线程，定期检查处于退役中或进入维护中的DataNode状态，判断其块复制是否满足要求，
 * 并在满足条件时将其切换到最终状态（已退役/维护中）。
 * 
 * 核心职责：
 * <ul>
 * <li>处理DataNode的退役启动与停止，更新集群拓扑和心跳管理器状态</li>
 * <li>处理DataNode的维护启动与停止，支持维护过期自动处理</li>
 * <li>协调后台监控线程追踪待处理节点，定期检查块复制进度</li>
 * <li>判断块是否满足足够复制条件，允许节点完成状态转换</li>
 * </ul>
 * 
 * 本类依赖FSNamesystem锁进行同步，所有状态变更操作需要持有对应锁。
 */
@InterfaceAudience.Private
public class DatanodeAdminManager {
  private static final Logger LOG =
      LoggerFactory.getLogger(DatanodeAdminManager.class);
  private final Namesystem namesystem;
  private final BlockManager blockManager;
  private final HeartbeatManager hbManager;
  private final ScheduledExecutorService executor;

  private DatanodeAdminMonitorInterface monitor = null;

  /**
   * 构造DataNode管理员管理器，初始化后台定时执行线程池。
   * @param namesystem NameNode命名系统对象
   * @param blockManager HDFS块管理器
   * @param hbManager 心跳管理器
   */
  DatanodeAdminManager(final Namesystem namesystem,
      final BlockManager blockManager, final HeartbeatManager hbManager) {
    this.namesystem = namesystem;
    this.blockManager = blockManager;
    this.hbManager = hbManager;

    executor = Executors.newScheduledThreadPool(1,
        new ThreadFactoryBuilder().setNameFormat("DatanodeAdminMonitor-%d")
            .setDaemon(true).build());
  }

  /**
   * 启动DataNode管理员监控线程，从配置加载监控实现并启动定时任务。
   * @param conf Hadoop配置对象
   */
  void activate(Configuration conf) {
    // 从配置读取监控间隔时间，转换为秒
    final int intervalSecs = (int) conf.getTimeDuration(
        DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_INTERVAL_KEY,
        DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_INTERVAL_DEFAULT,
        TimeUnit.SECONDS);
    Preconditions.checkArgument(intervalSecs >= 0, "Cannot set a negative " +
        "value for " + DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_INTERVAL_KEY);

    Class cls = null;
    try {
      // 从配置加载监控实现类，默认使用配置中的默认实现
      cls = conf.getClass(
          DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_MONITOR_CLASS,
          Class.forName(DFSConfigKeys
                  .DFS_NAMENODE_DECOMMISSION_MONITOR_CLASS_DEFAULT));
      // 通过反射创建监控实例
      monitor =
          (DatanodeAdminMonitorInterface)ReflectionUtils.newInstance(cls, conf);
      // 注入依赖对象
      monitor.setBlockManager(blockManager);
      monitor.setNameSystem(namesystem);
      monitor.setDatanodeAdminManager(this);
    } catch (Exception e) {
      throw new RuntimeException("Unable to create the Decommission monitor " +
          "from "+cls, e);
    }
    // 启动固定间隔的定时监控任务
    executor.scheduleWithFixedDelay(monitor, intervalSecs, intervalSecs,
        TimeUnit.SECONDS);

    LOG.debug("Activating DatanodeAdminManager with interval {} seconds.", intervalSecs);
  }

  /**
   * 关闭监控线程，等待线程终止。
   */
  void close() {
    executor.shutdownNow();
    try {
      executor.awaitTermination(3000, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {}
  }

  /**
   * 启动指定DataNode的退役流程。
   * @param node 待退役的DataNode描述符
   */
  @VisibleForTesting
  public void startDecommission(DatanodeDescriptor node) {
    if (!node.isDecommissionInProgress() && !node.isDecommissioned()) {
      // 更新心跳管理器维护的DataNode统计状态
      hbManager.startDecommission(node);
      // 更新集群网络拓扑，标记节点退役
      blockManager.getDatanodeManager().getNetworkTopology().decommissionNode(node);
      // 心跳管理器会直接将已死亡节点标记为已退役，存活节点进入进行中状态
      if (node.isDecommissionInProgress()) {
        for (DatanodeStorageInfo storage : node.getStorageInfos()) {
          LOG.info("Starting decommission of {} {} with {} blocks",
              node, storage, storage.numBlocks());
        }
        // 记录退役开始时间
        node.getLeavingServiceStatus().setStartTime(monotonicNow());
        // 将节点添加到监控追踪列表
        monitor.startTrackingNode(node);
      }
    } else {
      LOG.trace("startDecommission: Node {} in {}, nothing to do.",
          node, node.getAdminState());
    }
  }

  /**
   * 停止指定DataNode的退役流程，将节点恢复为正常状态。
   * @param node 待取消退役的DataNode描述符
   */
  @VisibleForTesting
  public void stopDecommission(DatanodeDescriptor node) {
    if (node.isDecommissionInProgress() || node.isDecommissioned()) {
      // 更新心跳管理器维护的DataNode统计状态
      hbManager.stopDecommission(node);
      // 更新集群网络拓扑，恢复节点为正常状态
      blockManager.getDatanodeManager().getNetworkTopology().recommissionNode(node);
      // 若节点已存活，处理多余冗余块
      if (node.isAlive()) {
        blockManager.processExtraRedundancyBlocksOnInService(node);
      }
      // 从监控追踪列表移除节点
      monitor.stopTrackingNode(node);
    } else {
      LOG.trace("stopDecommission: Node {} in {}, nothing to do.",
          node, node.getAdminState());
    }
  }

  /**
   * 启动指定DataNode的维护流程。
   * @param node 待进入维护的DataNode描述符
   * @param maintenanceExpireTimeInMS 维护过期时间戳（毫秒）
   */
  @VisibleForTesting
  public void startMaintenance(DatanodeDescriptor node,
      long maintenanceExpireTimeInMS) {
    // 更新维护过期时间，即使节点已在维护中也允许调整过期时间
    node.setMaintenanceExpireTimeInMS(maintenanceExpireTimeInMS);
    if (!node.isMaintenance()) {
      // 更新心跳管理器维护的DataNode统计状态
      hbManager.startMaintenance(node);
      // 心跳管理器会直接将已死亡节点标记为维护中，存活节点进入进入中状态
      if (node.isEnteringMaintenance()) {
        for (DatanodeStorageInfo storage : node.getStorageInfos()) {
          LOG.info("Starting maintenance of {} {} with {} blocks",
              node, storage, storage.numBlocks());
        }
        // 记录维护开始时间
        node.getLeavingServiceStatus().setStartTime(monotonicNow());
      }
      // 无论处于哪个阶段都添加追踪，用于处理维护过期
      monitor.startTrackingNode(node);
    } else {
      LOG.trace("startMaintenance: Node {} in {}, nothing to do.",
          node, node.getAdminState());
    }
  }


  /**
   * 停止指定DataNode的维护流程，将节点恢复为正常状态。
   * @param node 待退出维护的DataNode描述符
   */
  @VisibleForTesting
  public void stopMaintenance(DatanodeDescriptor node) {
    if (node.isMaintenance()) {
      // 更新心跳管理器维护的DataNode统计状态
      hbManager.stopMaintenance(node);

      // 如果节点在维护期间已死亡，需要移除节点关联的所有块，触发必要复制
      if (!node.isAlive()) {
        blockManager.removeBlocksAssociatedTo(node);
      } else {
        // 节点存活，处理多余冗余块（节点恢复后可能存在多余副本）
        blockManager.processExtraRedundancyBlocksOnInService(node);
      }

      // 从监控追踪列表移除节点
      monitor.stopTrackingNode(node);
    } else {
      LOG.trace("stopMaintenance: Node {} in {}, nothing to do.",
          node, node.getAdminState());
    }
  }

  /**
   * 将指定DataNode标记为已退役，记录完成日志。
   * @param dn 目标DataNode
   */
  protected void setDecommissioned(DatanodeDescriptor dn) {
    dn.setDecommissioned();
    LOG.info("Decommissioning complete for node {}", dn);
  }

  /**
   * 将指定DataNode标记为已进入维护，记录完成日志。
   * @param dn 目标DataNode
   */
  protected void setInMaintenance(DatanodeDescriptor dn) {
    dn.setInMaintenance();
    LOG.info("Node {} has entered maintenance mode.", dn);
  }

  /**
   * 检查处于退役中/进入维护中的节点上的块是否满足足够复制要求。
   * 退役/维护不要求块一定达到完整复制级别，只要满足最低阈值即可。
   * @param block 待检查块信息
   * @param bc 块所属的文件块集合
   * @param numberReplicas 该块的各类副本计数
   * @param isDecommission 是否是退役场景检查
   * @param isMaintenance 是否是维护场景检查
   * @return true表示满足复制要求，节点可以完成状态转换；false表示不满足，需要继续等待复制
   */
  protected boolean isSufficient(BlockInfo block, BlockCollection bc,
                               NumberReplicas numberReplicas,
                               boolean isDecommission,
                               boolean isMaintenance) {
    if (blockManager.hasEnoughEffectiveReplicas(block, numberReplicas, 0)) {
      // 已经满足有效副本要求，无需继续等待
      LOG.trace("Block {} does not need replication.", block);
      return true;
    }

    final int numExpected = blockManager.getExpectedLiveRedundancyNum(block,
        numberReplicas);
    final int numLive = numberReplicas.liveReplicas();

    // 当前块副本不满足要求，继续检查
    LOG.trace("Block {} numExpected={}, numLive={}", block, numExpected,
        numLive);
    if (isDecommission && numExpected > numLive) {
      if (bc.isUnderConstruction() && block.equals(bc.getLastBlock())) {
        // 正在写入的文件最后一块，只要满足最小副本数即可允许退役
        if (blockManager.hasMinStorage(block, numLive)) {
          LOG.trace("UC block {} sufficiently-replicated since numLive ({}) "
              + ">= minR ({})", block, numLive,
              blockManager.getMinStorageNum(block));
          return true;
        } else {
          LOG.trace("UC block {} insufficiently-replicated since numLive "
              + "({}) < minR ({})", block, numLive,
              blockManager.getMinStorageNum(block));
        }
      } else {
        // 已关闭文件，满足默认复制数即可允许退役
        if (numLive >= blockManager.getDefaultStorageNum(block)) {
          return true;
        }
      }
    }
    // 维护场景，满足维护最小副本数即可
    if (isMaintenance && numLive >= blockManager.getMinMaintenanceStorageNum(block)) {
      return true;
    }
    return false;
  }

  /**
   * 记录块复制信息到块状态变更日志，用于问题诊断。
   * @param block 待记录块信息
   * @param bc 块所属文件集合
   * @param srcNode 当前正在处理的源DataNode
   * @param num 副本计数
   * @param storages 持有该块的存储列表
   */
  protected void logBlockReplicationInfo(BlockInfo block,
      BlockCollection bc,
      DatanodeDescriptor srcNode, NumberReplicas num,
      Iterable<DatanodeStorageInfo> storages) {
    if (!NameNode.blockStateChangeLog.isInfoEnabled()) {
      return;
    }

    int curReplicas = num.liveReplicas();
    int curExpectedRedundancy = blockManager.getExpectedRedundancyNum(block);
    StringBuilder nodeList = new StringBuilder();
    for (DatanodeStorageInfo storage : storages) {
      final DatanodeDescriptor node = storage.getDatanodeDescriptor();
      nodeList.append(node).append(' ');
    }
    NameNode.blockStateChangeLog.info(
        "Block: " + block + ", Expected Replicas: "
        + curExpectedRedundancy + ", live replicas: " + curReplicas
        + ", corrupt replicas: " + num.corruptReplicas()
        + ", decommissioned replicas: " + num.decommissioned()
        + ", decommissioning replicas: " + num.decommissioning()
        + ", maintenance replicas: " + num.maintenanceReplicas()
        + ", live entering maintenance replicas: "
        + num.liveEnteringMaintenanceReplicas()
        + ", replicas on stale nodes: " + num.replicasOnStaleNodes()
        + ", readonly replicas: " + num.readOnlyReplicas()
        + ", excess replicas: " + num.excessReplicas()
        + ", Is Open File: " + bc.isUnderConstruction()
        + ", Datanodes having this block: " + nodeList + ", Current Datanode: "
        + srcNode + ", Is current datanode decommissioning: "
        + srcNode.isDecommissionInProgress() +
        ", Is current datanode entering maintenance: "
        + srcNode.isEnteringMaintenance());
  }

  @VisibleForTesting
  public int getNumPendingNodes() {
    return monitor.getPendingNodeCount();
  }

  @VisibleForTesting
  public int getNumTrackedNodes() {
    return monitor.getTrackedNodeCount();
  }

  @VisibleForTesting
  public int getNumNodesChecked() {
    return monitor.getNumNodesChecked();
  }

  @VisibleForTesting
  public Queue<DatanodeDescriptor> getPendingNodes() {
    return monitor.getPendingNodes();
  }

  @VisibleForTesting
  void runMonitorForTest() throws ExecutionException, InterruptedException {
    executor.submit(monitor).get();
  }

  /**
   * 刷新待处理块复制限制配置，动态更新监控参数。
   * @param pendingRepLimit 新的待处理复制限制值
   * @param key 配置项键名
   */
  public void refreshPendingRepLimit(int pendingRepLimit, String key) {
    ensurePositiveInt(pendingRepLimit, key);
    this.monitor.setPendingRepLimit(pendingRepLimit);
  }

  @VisibleForTesting
  public int getPendingRepLimit() {
    return this.monitor.getPendingRepLimit();
  }

  /**
   * 刷新每次锁持有处理块数配置，动态更新监控参数。
   * @param blocksPerLock 每次锁持有处理的最大块数
   * @param key 配置项键名
   */
  public void refreshBlocksPerLock(int blocksPerLock, String key) {
    ensurePositiveInt(blocksPerLock, key);
    this.monitor.setBlocksPerLock(blocksPerLock);
  }

  @VisibleForTesting
  public int getBlocksPerLock() {
    return this.monitor.getBlocksPerLock();
  }

  /**
   * 校验输入整数值是否为正整数，不满足则抛出异常。
   * @param val 待校验值
   * @param key 配置项键名，用于异常信息
   */
  private void ensurePositiveInt(int val, String key) {
    Preconditions.checkArgument(
        (