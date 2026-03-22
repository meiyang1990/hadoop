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

import org.apache.hadoop.thirdparty.com.google.common.collect.Iterables;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.INodeId;
import org.apache.hadoop.hdfs.util.LightWeightHashSet;
import org.apache.hadoop.hdfs.util.LightWeightLinkedSet;
import org.apache.hadoop.hdfs.util.RwLockMode;
import org.apache.hadoop.classification.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Map;
import java.util.List;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.stream.Collectors;

/**
 * 文件：org.apache.hadoop.hdfs.server.blockmanagement.DatanodeAdminBackoffMonitor.java
 * 所属模块：HDFS 服务端，块管理模块
 * 核心职责：退避式数据节点退役/维护监控器，控制退服过程中块复制的速率，避免一下子把复制队列打满，
 *          保证在批量数据节点退服场景下，集群依然稳定运行，逐步完成块复制，最终才将节点标记为退服完成。
 */
/**
 * This class implements the logic to track decommissioning and entering
 * maintenance nodes, ensure all their blocks are adequately replicated
 * before they are moved to the decommissioned or maintenance state.
 *
 * This monitor avoids flooding the replication queue with all pending blocks
 * and instead feeds them to the queue as the prior set complete replication.
 *
 * HDFS-14854 contains details about the overall design of this class.
 *
 */
/**
 * 退避式数据节点退服/维护监控器，采用流量控制方式分批处理待复制块，避免复制队列过载
 * 核心职责：跟踪正在退役/进入维护的节点，逐步将需要复制的块放入复制队列，
 * 等所有块都完成足够复制后，才将节点移动到最终的退服/维护状态。
 * 继承自基础监控类，实现了数据节点管理监控接口。
 */
public class DatanodeAdminBackoffMonitor extends DatanodeAdminMonitorBase
    implements DatanodeAdminMonitorInterface  {
  /**
   * Map containing the DECOMMISSION_INPROGRESS or ENTERING_MAINTENANCE
   * datanodes that are being tracked so they can be be marked as
   * DECOMMISSIONED or IN_MAINTENANCE. Even after the node is marked as
   * IN_MAINTENANCE, the node remains in the map until
   * maintenance expires checked during a monitor tick.
   * <p/>
   * This holds a set of references to the under-replicated blocks on the DN
   * at the time the DN is added to the map, i.e. the blocks that are
   * preventing the node from being marked as decommissioned. During a monitor
   * tick, this list is pruned as blocks becomes replicated.
   * <p/>
   * Note also that the reference to the list of under-replicated blocks
   * will be null on initial add
   * <p/>
   * However, this map can become out-of-date since it is not updated by block
   * reports or other events. Before being finally marking as decommissioned,
   * another check is done with the actual block map.
   */
  /** 待退服/待进入维护节点的跟踪Map：Key是数据节点描述符，Value是该节点上待复制的块集合 */
  private HashMap<DatanodeDescriptor, HashMap<BlockInfo, Integer>>
      outOfServiceNodeBlocks = new HashMap<>();

  /**
   * The number of blocks to process when moving blocks to pendingReplication
   * before releasing and reclaiming the namenode lock.
   */
  /** 每次持有锁处理块的最大数量，处理完后释放锁让其他任务执行 */
  private volatile int blocksPerLock;

  /**
   * The number of blocks that have been checked on this tick.
   */
  /** 当前监控周期内已检查的块数量 */
  private int numBlocksChecked = 0;
  /**
   * The maximum number of blocks to hold in PendingRep at any time.
   */
  /** 等待复制队列中允许存放的最大块数量 */
  private volatile int pendingRepLimit;

  /**
   * The list of blocks which have been placed onto the replication queue
   * and are waiting to be sufficiently replicated.
   */
  /** 已放入复制队列、等待完成复制的块集合：按数据节点分组存放 */
  private final Map<DatanodeDescriptor, List<BlockInfo>>
      pendingRep = new HashMap<>();

  private static final Logger LOG =
      LoggerFactory.getLogger(DatanodeAdminBackoffMonitor.class);

  DatanodeAdminBackoffMonitor() {
  }


  /**
   * 从配置文件加载退避监控器的各项参数，包括等待队列大小限制和每次锁处理块数量，
   * 对无效配置会使用默认值并打错误日志。
   */
  @Override
  protected void processConf() {
    this.pendingRepLimit = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_BACKOFF_MONITOR_PENDING_LIMIT,
        DFSConfigKeys.
            DFS_NAMENODE_DECOMMISSION_BACKOFF_MONITOR_PENDING_LIMIT_DEFAULT);
    if (this.pendingRepLimit < 1) {
      LOG.error("{} is set to an invalid value, it must be greater than "+
              "zero. Defaulting to {}",
          DFSConfigKeys.
              DFS_NAMENODE_DECOMMISSION_BACKOFF_MONITOR_PENDING_LIMIT,
          DFSConfigKeys.
              DFS_NAMENODE_DECOMMISSION_BACKOFF_MONITOR_PENDING_LIMIT_DEFAULT
      );
      this.pendingRepLimit = DFSConfigKeys.
          DFS_NAMENODE_DECOMMISSION_BACKOFF_MONITOR_PENDING_LIMIT_DEFAULT;
    }
    this.blocksPerLock = conf.getInt(
        DFSConfigKeys.
            DFS_NAMENODE_DECOMMISSION_BACKOFF_MONITOR_PENDING_BLOCKS_PER_LOCK,
        DFSConfigKeys.
            DFS_NAMENODE_DECOMMISSION_BACKOFF_MONITOR_PENDING_BLOCKS_PER_LOCK_DEFAULT
    );
    if (blocksPerLock <= 0) {
      LOG.error("{} is set to an invalid value, it must be greater than "+
              "zero. Defaulting to {}",
          DFSConfigKeys.
              DFS_NAMENODE_DECOMMISSION_BACKOFF_MONITOR_PENDING_BLOCKS_PER_LOCK,
          DFSConfigKeys.
              DFS_NAMENODE_DECOMMISSION_BACKOFF_MONITOR_PENDING_BLOCKS_PER_LOCK_DEFAULT);
      blocksPerLock =
          DFSConfigKeys.
              DFS_NAMENODE_DECOMMISSION_BACKOFF_MONITOR_PENDING_BLOCKS_PER_LOCK_DEFAULT;
    }
    LOG.info("Initialized the Backoff Decommission and Maintenance Monitor");
  }

  /**
   * 停止跟踪指定数据节点的退服/维护流程，将节点加入取消队列，后续会从跟踪列表中移除。
   * 调用该方法必须持有NameNode写锁。
   * @param dn 需要停止跟踪的数据节点
   */
  @Override
  public void stopTrackingNode(DatanodeDescriptor dn) {
    getPendingNodes().remove(dn);
    getCancelledNodes().add(dn);
  }

  /**
   * 获取当前正在跟踪的节点总数。
   * @return 当前正在跟踪的退服/维护节点数量
   */
  @Override
  public int getTrackedNodeCount() {
    return outOfServiceNodeBlocks.size();
  }

  /**
   * 获取当前监控周期检查过的节点数量。
   * @return 当前周期检查的节点总数
   */
  @Override
  public int getNumNodesChecked() {
    // We always check all nodes on each tick
    return outOfServiceNodeBlocks.size();
  }

  /**
   * 监控线程主执行方法，每个周期执行一次退服/维护节点检查流程。
   * 按顺序处理取消节点、处理排队节点、检查节点块复制进度、处理完成节点。
   */
  @Override
  public void run() {
    LOG.debug("DatanodeAdminMonitorV2 is running.");
    if (!namesystem.isRunning()) {
      LOG.info("Namesystem is not running, skipping " +
          "decommissioning/maintenance checks.");
      return;
    }
    // 每个周期开始前重置已检查块计数
    numBlocksChecked = 0;
    // 检查退服或维护进度
    try {
      namesystem.writeLock(RwLockMode.BM);
      try {
        /**
         * Other threads can modify the pendingNode list and the cancelled
         * node list, so we must process them under the NN write lock to
         * prevent any concurrent modifications.
         *
         * Always process the cancelled list before the pending list, as
         * it is possible for a node to be cancelled, and then quickly added
         * back again. If we process these the other way around, the added
         * node will be removed from tracking by the pending cancel.
         */
        // 先处理已取消退服的节点
        processCancelledNodes();

        // 如果并发退服节点超过最大跟踪限制，输出警告并把不健康节点重新排队腾出空间
        int numTrackedNodes = outOfServiceNodeBlocks.size();
        int numQueuedNodes = getPendingNodes().size();
        int numDecommissioningNodes = numTrackedNodes + numQueuedNodes;
        if (numDecommissioningNodes > maxConcurrentTrackedNodes) {
          LOG.warn(
              "{} nodes are decommissioning but only {} nodes will be tracked at a time. "
                  + "{} nodes are currently queued waiting to be decommissioned.",
              numDecommissioningNodes, maxConcurrentTrackedNodes, numQueuedNodes);

          // 筛选出不健康节点，重新排队，给健康节点腾跟踪位置
          final List<DatanodeDescriptor> unhealthyDns = outOfServiceNodeBlocks.keySet().stream()
              .filter(dn -> !blockManager.isNodeHealthyForDecommissionOrMaintenance(dn))
              .collect(Collectors.toList());
          getUnhealthyNodesToRequeue(unhealthyDns, numDecommissioningNodes).forEach(dn -> {
            getPendingNodes().add(dn);
            outOfServiceNodeBlocks.remove(dn);
            pendingRep.remove(dn);
          });
        }

        // 将排队节点加入跟踪列表
        processPendingNodes();
      } finally {
        namesystem.writeUnlock(RwLockMode.BM, "DatanodeAdminMonitorV2Thread");
      }
      // 后续检查过程中会根据需要自行获取释放读写锁，无需持续持有
      check();
    } catch (Exception e) {
      LOG.warn("DatanodeAdminMonitor caught exception when processing node.",
          e);
    }
    // 如果本次周期有检查输出统计日志
    if (numBlocksChecked + outOfServiceNodeBlocks.size() > 0) {
      LOG.info("Checked {} blocks this tick. {} nodes are now " +
          "in maintenance or transitioning state. {} nodes pending. {} " +
          "nodes waiting to be cancelled.",
          numBlocksChecked, outOfServiceNodeBlocks.size(), getPendingNodes().size(),
          getCancelledNodes().size());
    }
  }

  /**
   * 将排队等待的节点加入跟踪列表，启动退服/维护流程，
   * 控制并发跟踪节点数量不超过配置最大值。
   * 调用该方法必须持有NameNode写锁，避免并发修改排队列表。
   */
  private void processPendingNodes() {
    while (!getPendingNodes().isEmpty() &&
        (maxConcurrentTrackedNodes == 0 ||
            outOfServiceNodeBlocks.size() < maxConcurrentTrackedNodes)) {
      outOfServiceNodeBlocks.put(getPendingNodes().poll(), null);
    }
  }

  /**
   * 处理被管理员取消退服/维护的节点，从所有跟踪列表中移除这些节点。
   * 调用该方法必须持有NameNode写锁，避免并发修改取消列表。
   */
  private void processCancelledNodes() {
    while(!getCancelledNodes().isEmpty()) {
      DatanodeDescriptor dn = getCancelledNodes().poll();
      outOfServiceNodeBlocks.remove(dn);
      pendingRep.remove(dn);
    }
  }

  /**
   * 核心检查流程，按步骤推进所有跟踪节点的退服/维护进度：
   * 1. 扫描新增节点的存储，加载所有待处理块
   * 2. 处理已过期的维护节点，将其恢复为服务中状态
   * 3. 清理等待复制队列中已完成复制的块
   * 4. 将新一批块移动到等待复制队列，放入BlockManager复制队列
   * 5. 检查是否有节点完成所有块复制
   * 6. 将完成节点标记为最终状态
   * 该方法调用的子方法会多次获取释放NameNode锁，不会长时间持有锁阻塞其他操作。
   */
  private void check() {
    final List<DatanodeDescriptor> toRemove = new ArrayList<>();

    if (outOfServiceNodeBlocks.size() == 0) {
      // No nodes currently being tracked so simply return
      return;
    }

    // Check if there are any pending nodes to process, ie those where the
    // storage has not been scanned yet. For all which are pending, scan
    // the storage and load the under-replicated block list into
    // outOfServiceNodeBlocks. As this does not modify any external structures
    // it can be done under the namenode *read* lock, and the lock can be
    // dropped between each storage on each node.
    //
    // TODO - This is an expensive call, depending on how many nodes are
    //        to be processed, but it requires only the read lock and it will
    //        be dropped and re-taken frequently. We may want to throttle this
    //        to process only a few nodes per iteration.
    // 扫描所有未初始化的新增节点，加载其存储上的块信息
    outOfServiceNodeBlocks.keySet()
        .stream()
        .filter(n -> outOfServiceNodeBlocks.get(n) == null)
        .forEach(n -> scanDatanodeStorage(n, true));

    // 处理已过期的维护节点
    processMaintenanceNodes();
    // First check the pending replication list and remove any blocks
    // which are now replicated OK. This list is constrained in size so this
    // call should not be overly expensive.
    // 清理已完成复制的块
    processPendingReplication();

    // Now move a limited number of blocks to pending
    // 将新一批块加入等待复制队列
    moveBlocksToPending();

    // Check if any nodes have reached zero blocks and also update the stats
    // exposed via JMX for all nodes still being processed.
    // 检查哪些节点已经完成所有块复制
    checkForCompletedNodes(toRemove);

    // Finally move the nodes to their final state if they are ready.
    // 处理完成节点，将其标记为最终状态
    processCompletedNodes(toRemove);
  }

  /**
   * 检查所有维护中节点的过期时间，如果维护已过期，停止维护将节点恢复为服务中状态。
   * 处理每个节点后主动释放锁再重新获取，避免长时间阻塞其他操作。
   */
  private void processMaintenanceNodes() {
    // Check for any maintenance state nodes which need to be expired
    namesystem.writeLock(RwLockMode.GLOBAL);
    try {
      for (DatanodeDescriptor dn : outOfServiceNodeBlocks.keySet()) {
        if (dn.isMaintenance() && dn.maintenanceExpired()) {
          // If maintenance expires, stop tracking it. This can be an
          // expensive call, as it may need to invalidate blocks. Therefore
          // we can yield and retake the write lock after each node
          //
          // The call to stopMaintenance makes a call to stopTrackingNode()
          // which added the node to the cancelled list. Therefore expired
          // maintenance nodes do not need to be added to the toRemove list.
          dnAdmin.stopMaintenance(dn);
          namesystem.writeUnlock(RwLockMode.GLOBAL, "processMaintenanceNodes");
          namesystem.writeLock(RwLockMode.GLOBAL);
        }
      }
    } finally {
      namesystem.writeUnlock(RwLockMode.GLOBAL, "processMaintenanceNodes");
    }
  }

  /**
   * 处理所有已完成块复制的节点，根据节点类型将其标记为退服完成或维护完成状态。
   * @param toRemove 需要处理的已完成节点列表
   */
  private void processCompletedNodes(List<DatanodeDescriptor> toRemove) {
    if (toRemove.size() == 0)