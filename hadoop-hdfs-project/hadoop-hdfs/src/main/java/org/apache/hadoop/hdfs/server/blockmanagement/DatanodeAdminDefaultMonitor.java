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

import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.INodeId;
import org.apache.hadoop.hdfs.util.CyclicIteration;
import org.apache.hadoop.hdfs.util.LightWeightHashSet;
import org.apache.hadoop.hdfs.util.LightWeightLinkedSet;
import org.apache.hadoop.hdfs.util.RwLockMode;
import org.apache.hadoop.util.ChunkedArrayList;
import org.apache.hadoop.classification.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractList;
import java.util.TreeMap;
import java.util.ArrayList;
import java.util.Map;
import java.util.List;
import java.util.Iterator;

/**
 * 文件说明：数据节点退役/维护状态默认监控器，负责监控正在进行退役或进入维护的节点，
 * 检查其上所有块是否已经完成足够复制，完成后将节点标记为目标状态。
 * 由于操作需要持有命名系统锁，每次监控周期的工作量会被限制，避免阻塞其他操作。
 * Checks to see if datanodes have finished DECOMMISSION_INPROGRESS or
 * ENTERING_MAINTENANCE state.
 * <p>
 * Since this is done while holding the namesystem lock,
 * the amount of work per monitor tick is limited.
 */

/**
 * 类说明：数据节点管理操作默认监控器，继承基础监控类实现数据节点退役/维护的进度监控逻辑，
 * 核心职责是增量检查待退役/维护节点上的块复制状态，当所有块完成足够复制后完成状态切换。
 */
public class DatanodeAdminDefaultMonitor extends DatanodeAdminAdminBase
    implements DatanodeAdminMonitorInterface {

  /**
   * 跟踪正在进行退役/进入维护的数据节点，存储每个节点上需要等待复制完成的低冗余块列表。
   * 节点进入维护后仍会保留在map中，直到维护超时才会移除。
   * 当节点刚加入时，块列表为null，第一次扫描后才会填充。
   * 该map可能会过时，最终完成状态切换前会使用实际块映射重新校验。
   */
  private final TreeMap<DatanodeDescriptor, AbstractList<BlockInfo>>
      outOfServiceNodeBlocks;

  /**
   * 每个监控周期最多检查的块数量，避免单次操作占用锁时间过长。
   */
  private int numBlocksPerCheck;

  /**
   * 当前监控周期已经检查的块数量。
   */
  private int numBlocksChecked = 0;
  /**
   * 当前锁持有周期已经检查的块数量，用于锁让步统计。
   */
  private int numBlocksCheckedPerLock = 0;
  /**
   * 当前监控周期已经检查的节点数量，用于统计监控进度。
   */
  private int numNodesChecked = 0;
  /**
   * 循环迭代的上次处理节点，支持分段循环处理多个节点。
   */
  private DatanodeDescriptor iterkey = new DatanodeDescriptor(
      new DatanodeID("", "", "", 0, 0, 0, 0));

  private static final Logger LOG =
      LoggerFactory.getLogger(DatanodeAdminDefaultMonitor.class);

  /**
   * 构造函数：初始化空的退役/维护节点块映射。
   */
  DatanodeAdminDefaultMonitor() {
    this.outOfServiceNodeBlocks = new TreeMap<>();
  }

  /**
   * 函数说明：从配置中加载监控参数，包括每个周期最大检查块数，处理过期配置警告。
   */
  @Override
  protected void processConf() {
    numBlocksPerCheck = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BLOCKS_PER_INTERVAL_KEY,
        DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BLOCKS_PER_INTERVAL_DEFAULT);
    if (numBlocksPerCheck <= 0) {
      LOG.error("{} must be greater than zero. Defaulting to {}",
          DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BLOCKS_PER_INTERVAL_KEY,
          DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BLOCKS_PER_INTERVAL_DEFAULT);
      numBlocksPerCheck =
          DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BLOCKS_PER_INTERVAL_DEFAULT;
    }

    final String deprecatedKey = "dfs.namenode.decommission.nodes.per.interval";
    final String strNodes = conf.get(deprecatedKey);
    if (strNodes != null) {
      LOG.warn("Deprecated configuration key {} will be ignored.", deprecatedKey);
      LOG.warn("Please update your configuration to use {} instead.",
          DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BLOCKS_PER_INTERVAL_KEY);
    }

    LOG.info("Initialized the Default Decommission and Maintenance monitor");
  }

  /**
   * 函数说明：检查当前周期检查块数是否已经达到配置上限。
   * @return true表示已达到上限，需要停止当前周期处理
   */
  private boolean exceededNumBlocksPerCheck() {
    LOG.trace("Processed {} blocks so far this tick", numBlocksChecked);
    return numBlocksChecked >= numBlocksPerCheck;
  }

  /**
   * 函数说明：停止跟踪指定数据节点的退役/维护进度，将节点移至取消队列。
   * @param dn 要停止跟踪的数据节点
   */
  @Override
  public void stopTrackingNode(DatanodeDescriptor dn) {
    getPendingNodes().remove(dn);
    getCancelledNodes().add(dn);
  }

  /**
   * 函数说明：获取当前正在跟踪的节点数量。
   * @return 正在跟踪的退役/维护节点数
   */
  @Override
  public int getTrackedNodeCount() {
    return outOfServiceNodeBlocks.size();
  }

  /**
   * 函数说明：获取当前周期已经检查的节点数量，用于监控统计。
   * @return 已检查节点数
   */
  @Override
  public int getNumNodesChecked() {
    return numNodesChecked;
  }

  @VisibleForTesting
  @Override
  public int getPendingRepLimit() {
    return 0;
  }

  @Override
  public void setPendingRepLimit(int pendingRepLimit) {
    // nothing.
  }

  @VisibleForTesting
  @Override
  public int getBlocksPerLock() {
    return 0;
  }

  @Override
  public void setBlocksPerLock(int blocksPerLock) {
    // nothing.
  }

  /**
   * 函数说明：监控线程主运行方法，每个周期执行一次退役/维护节点检查流程。
   * 获取命名系统全局写锁，按顺序处理取消节点、待处理节点，然后执行块检查。
   */
  @Override
  public void run() {
    LOG.debug("DatanodeAdminMonitor is running.");
    if (!namesystem.isRunning()) {
      LOG.info("Namesystem is not running, skipping " +
          "decommissioning/maintenance checks.");
      return;
    }
    // 重置当前周期检查计数
    numBlocksChecked = 0;
    numBlocksCheckedPerLock = 0;
    numNodesChecked = 0;
    // 停止维护操作需要获取FS读锁，这里统一获取全局写锁
    namesystem.writeLock(RwLockMode.GLOBAL);
    try {
      processCancelledNodes();
      processPendingNodes();
      check();
    } catch (Exception e) {
      LOG.warn("DatanodeAdminMonitor caught exception when processing node.",
          e);
    } finally {
      namesystem.writeUnlock(RwLockMode.GLOBAL, "DatanodeAdminMonitorThread");
    }
    if (numBlocksChecked + numNodesChecked > 0) {
      LOG.info("Checked {} blocks and {} nodes this tick. {} nodes are now " +
              "in maintenance or transitioning state. {} nodes pending.",
          numBlocksChecked, numNodesChecked, outOfServiceNodeBlocks.size(),
          getPendingNodes().size());
    }
  }

  /**
   * 函数说明：将待处理队列中的节点转移到跟踪映射中，受并发跟踪节点数上限限制。
   */
  private void processPendingNodes() {
    while (!getPendingNodes().isEmpty() &&
        (maxConcurrentTrackedNodes == 0 ||
            outOfServiceNodeBlocks.size() < maxConcurrentTrackedNodes)) {
      outOfServiceNodeBlocks.put(getPendingNodes().poll(), null);
    }
  }

  /**
   * 函数说明：处理被管理员取消退役/维护的节点，从跟踪映射中移除这些节点。
   * 该方法必须在写锁下执行，避免并发修改内部结构。
   */
  private void processCancelledNodes() {
    while(!getCancelledNodes().isEmpty()) {
      DatanodeDescriptor dn = getCancelledNodes().poll();
      outOfServiceNodeBlocks.remove(dn);
    }
  }

  /**
   * 函数说明：核心检查逻辑，循环遍历所有跟踪节点，检查每个节点的块复制进度，
   * 完成块复制的节点标记为目标状态，维护过期节点停止维护并移除跟踪。
   */
  private void check() {
    // 基于上次处理位置创建循环迭代器
    final Iterator<Map.Entry<DatanodeDescriptor, AbstractList<BlockInfo>>>
        it = new CyclicIteration<>(outOfServiceNodeBlocks,
        iterkey).iterator();
    final List<DatanodeDescriptor> toRemove = new ArrayList<>();
    final List<DatanodeDescriptor> unhealthyDns = new ArrayList<>();
    boolean isValidState = true;

    while (it.hasNext() && !exceededNumBlocksPerCheck() && namesystem
        .isRunning()) {
      numNodesChecked++;
      final Map.Entry<DatanodeDescriptor, AbstractList<BlockInfo>>
          entry = it.next();
      final DatanodeDescriptor dn = entry.getKey();
      try {
        AbstractList<BlockInfo> blocks = entry.getValue();
        boolean fullScan = false;
        // 维护已过期，停止跟踪该节点
        if (dn.isMaintenance() && dn.maintenanceExpired()) {
          dnAdmin.stopMaintenance(dn);
          toRemove.add(dn);
          continue;
        }
        // 节点已经进入维护且未过期，跳过检查
        if (dn.isInMaintenance()) {
          continue;
        }
        // 新加入跟踪的节点，执行全量扫描收集不足复制块
        if (blocks == null) {
          LOG.debug("Newly-added node {}, doing full scan to find " +
              "insufficiently-replicated blocks.", dn);
          blocks = handleInsufficientlyStored(dn);
          outOfServiceNodeBlocks.put(dn, blocks);
          fullScan = true;
        } else {
          // 已跟踪节点，剪枝已经完成复制的块
          LOG.debug("Processing {} node {}", dn.getAdminState(), dn);
          pruneReliableBlocks(dn, blocks);
        }
        // 检查节点是否健康，不健康节点推迟退役
        final boolean isHealthy = blockManager.isNodeHealthyForDecommissionOrMaintenance(dn);
        if (!isHealthy) {
          unhealthyDns.add(dn);
        }
        // 所有块都完成复制，进行最终校验
        if (blocks.size() == 0) {
          if (!fullScan) {
            // 之前不是全量扫描，需要重新全量扫描校验
            LOG.debug("Node {} has finished replicating current set of "
                + "blocks, checking with the full block map.", dn);
            blocks = handleInsufficientlyStored(dn);
            outOfServiceNodeBlocks.put(dn, blocks);
          }
          // 全量扫描后仍然无不足块且节点健康，标记为目标状态
          if (blocks.size() == 0 && isHealthy) {
            if (dn.isDecommissionInProgress()) {
              dnAdmin.setDecommissioned(dn);
              toRemove.add(dn);
            } else if (dn.isEnteringMaintenance()) {
              // 进入维护状态的节点保留在map中跟踪维护过期
              dnAdmin.setInMaintenance(dn);
            } else {
              isValidState  = false;
              Preconditions.checkState(false,
                  "Node %s is in an invalid state! "
                      + "Invalid state: %s %s blocks are on this dn.",
                  dn, dn.getAdminState(), blocks.size());
            }
            LOG.debug("Node {} is sufficiently replicated and healthy, "
                + "marked as {}.", dn, dn.getAdminState());
          } else {
            LOG.info("Node {} {} healthy."
                    + " It needs to replicate {} more blocks."
                    + " {} is still in progress.", dn,
                isHealthy ? "is": "isn't", blocks.size(), dn.getAdminState());
          }
        } else {
          LOG.info("Node {} still has {} blocks to replicate "
                  + "before it is a candidate to finish {}.",
              dn, blocks.size(), dn.getAdminState());
        }
      } catch (Exception e) {
        // 处理异常，将节点放回待处理队列延后处理
        LOG.warn("DatanodeAdminMonitor caught exception when processing node "
            + "{}.", dn, e);
        if(isValidState){
          getPendingNodes().add(dn);
        } else {
          LOG.warn("Ignoring the node {} which is in invalid state", dn);
        }
        toRemove.add(dn);
        unhealthyDns.remove(dn);
      } finally {
        // 记录本次处理到的节点，下次循环从这里开始
        iterkey = dn;
      }
    }

    // 超过最大并发跟踪节点数，输出警告并调整队列
    int numTrackedNodes = outOfServiceNodeBlocks.size() - toRemove.size();
    int numQueuedNodes = getPendingNodes().size();
    int numDecommissioningNodes = numTrackedNodes + numQueuedNodes;
    if (numDecommissioningNodes > maxConcurrentTrackedNodes) {
      LOG.warn(
          "{} nodes are decommissioning but only {} nodes will be tracked at a time. "
              + "{} nodes are currently queued waiting to be decommissioned.",
          numDecommissioningNodes, maxConcurrentTrackedNodes, numQueuedNodes);

      // 将不健康节点重新放回等待队列，给健康节点让出跟踪名额
      getUnhealthyNodesToRequeue(unhealthyDns, numDecommissioningNodes).forEach(dn -> {
        getPendingNodes().add(dn);
        outOfServiceNodeBlocks.remove(dn);
      });
    }

    // 移除已经完成退役或维护过期的节点
    for (DatanodeDescriptor dn : toRemove) {
      Preconditions.checkState(dn.isDecommissioned() || dn.isInService(),
          "Removing node %s that is not yet decommissioned or in service!",
          dn);
      outOfServiceNodeBlocks.remove(dn);
    }
  }

  /**
   * 函数说明：从节点的不足复制块列表中移除已经满足冗余要求的块。
   * @param datanode 目标数据节点
   * @param blocks 节点的不足复制块列表
   */
  private void pruneReliableBlocks(final DatanodeDescriptor datanode,
                                   AbstractList<BlockInfo> blocks) {
    processBlocksInternal(datanode, blocks.iterator(), null, true);
  }

  /**
   * 函数说明：全量扫描数据节点上的所有块，收集不满足冗余要求、需要继续复制的块，
   * 同时为这些块调度复制任务。
   * @param datanode 目标数据节点
   * @return 不满足冗余要求的块列表
   */
  private AbstractList<BlockInfo> handleInsufficientlyStored(
      final DatanodeDescriptor datanode) {
    AbstractList<BlockInfo> insufficient = new ChunkedArrayList<>();
    processBlocksInternal(datanode, datanode.getBlockIterator(),
        insufficient, false);
    return insufficient;
  }

  /**
   * 函数说明：块处理核心逻辑，整合剪枝可靠块和收集不足块的共享逻辑，
   * 遍历块迭代器，检查每个块的冗余状态，处理块复制调度，统计不足块信息。
   * @param datanode 目标数据节点
   * @param it 块迭代器
   * @param insufficientList 收集不足块的输出列表，可为null表示不收集
   * @param pruneReliableBlocks 是否需要从迭代器中移除满足冗余要求的块
   */
  private void processBlocksInternal(
      final DatanodeDescriptor datanode,
      final Iterator<BlockInfo