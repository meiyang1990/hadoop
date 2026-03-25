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

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.Namesystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.stream.Stream;

/**
 * 文件级注释：数据节点下线/维护监控基类，为具体监控实现提供公共基础能力，支撑数据节点退役和维护模式的进度管理
 *
 * 抽象基类，提供数据节点管理监控的基础方法，被退避监控和默认监控继承，
 * 用于控制数据节点退役和维护模式的执行流程。
 */
public abstract class DatanodeAdminMonitorBase
    implements DatanodeAdminMonitorInterface, Configurable {

  /**
   * 按最后更新时间降序排序，降低不健康节点的优先级，因为不健康节点无法完成退役。
   */
  static final Comparator<DatanodeDescriptor> PENDING_NODES_QUEUE_COMPARATOR =
      (dn1, dn2) -> Long.compare(dn2.getLastUpdate(), dn1.getLastUpdate());

  protected BlockManager blockManager;
  protected Namesystem namesystem;
  protected DatanodeAdminManager dnAdmin;
  protected Configuration conf;

  // 待处理节点优先级队列，按规则排序等待处理
  private final PriorityQueue<DatanodeDescriptor> pendingNodes = new PriorityQueue<>(
      PENDING_NODES_QUEUE_COMPARATOR);

  /**
   * 已取消退役/维护的节点存入此队列，等待后续处理。
   */
  private final Queue<DatanodeDescriptor> cancelledNodes = new ArrayDeque<>();

  /**
   * 退役节点跟踪队列的最大并发跟踪节点数，0表示无限制。
   */
  protected int maxConcurrentTrackedNodes;

  private static final Logger LOG =
      LoggerFactory.getLogger(DatanodeAdminMonitorBase.class);

  /**
   * 设置集群命名系统实例，用于关联NameNode的核心元数据管理。
   *
   * @param ns 集群命名系统实例
   */
  @Override
  public void setNameSystem(Namesystem ns) {
    this.namesystem = ns;
  }

  /**
   * 设置集群块管理器实例，用于管理数据块副本分布。
   *
   * @param bm 集群块管理器实例
   */
  @Override
  public void setBlockManager(BlockManager bm) {
    this.blockManager = bm;
  }

  /**
   * 设置数据节点管理员实例，关联NameNode中的核心管理对象。
   *
   * @param admin 数据节点管理员实例
   */
  @Override
  public void setDatanodeAdminManager(DatanodeAdminManager admin) {
    this.dnAdmin = admin;
  }

  /**
   * 配置设置方法，由反射工具类用于创建监控实例时传入配置，
   * 读取最大并发跟踪节点数配置并完成初始化。
   *
   * @param conf 要使用的配置对象
   */
  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    // 从配置中读取最大并发跟踪节点数，使用默认值如果未配置
    this.maxConcurrentTrackedNodes = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_MAX_CONCURRENT_TRACKED_NODES,
        DFSConfigKeys
            .DFS_NAMENODE_DECOMMISSION_MAX_CONCURRENT_TRACKED_NODES_DEFAULT);
    // 配置校验：值不能为负数
    if (this.maxConcurrentTrackedNodes < 0) {
      LOG.error("{} is set to an invalid value, it must be zero or greater. "+
              "Defaulting to {}",
          DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_MAX_CONCURRENT_TRACKED_NODES,
          DFSConfigKeys
              .DFS_NAMENODE_DECOMMISSION_MAX_CONCURRENT_TRACKED_NODES_DEFAULT);
      this.maxConcurrentTrackedNodes =
          DFSConfigKeys
              .DFS_NAMENODE_DECOMMISSION_MAX_CONCURRENT_TRACKED_NODES_DEFAULT;
    }

    LOG.debug("Activating DatanodeAdminMonitor with {} max concurrently tracked nodes.",
        maxConcurrentTrackedNodes);
    // 调用子类实现处理额外配置
    processConf();
  }

  /**
   * 获取当前对象存储的配置实例。
   *
   * @return 对象创建时使用的配置
   */
  @Override
  public Configuration getConf() {
    return this.conf;
  }

  /**
   * 抽象方法，必须由子类实现，用于从配置中加载子类特有的配置项，
   * 初始化子类实例变量。
   */
  protected abstract void processConf();

  /**
   * 开始跟踪一个待退役/维护的数据节点，将节点加入待处理队列，
   * 必须在NameNode写锁下调用。
   * @param dn 要开始跟踪的数据节点
   */
  @Override
  public void startTrackingNode(DatanodeDescriptor dn) {
    pendingNodes.add(dn);
  }

  /**
   * 获取待处理队列中的节点数量，即等待开始退役但尚未开始处理的节点数。
   *
   * @return 待处理节点数量
   */
  @Override
  public int getPendingNodeCount() {
    return pendingNodes.size();
  }

  @Override
  public Queue<DatanodeDescriptor> getPendingNodes() {
    return pendingNodes;
  }

  @Override
  public Queue<DatanodeDescriptor> getCancelledNodes() {
    return cancelledNodes;
  }

  /**
   * 获取需要重新排队的不健康节点：当达到最大并发跟踪限制时，
   * 将处于退役中但已死亡的不健康节点重新排队，避免阻塞健康节点的退役。
   *
   * @param unhealthyDns 所有不健康的数据节点列表
   * @param numDecommissioningNodes 当前正在进行退役的节点总数
   * @return 需要重新排队的不健康节点流
   */
  Stream<DatanodeDescriptor> getUnhealthyNodesToRequeue(
      final List<DatanodeDescriptor> unhealthyDns, int numDecommissioningNodes) {
    if (!unhealthyDns.isEmpty()) {
      // 计算需要重新排队的不健康节点数量
      final int numUnhealthyNodesToRequeue =
          Math.min(numDecommissioningNodes - maxConcurrentTrackedNodes, unhealthyDns.size());

      LOG.warn("{} limit has been reached, re-queueing {} "
              + "nodes which are dead while in Decommission In Progress.",
          DFSConfigKeys.DFS_NAMENODE_DECOMMISSION_MAX_CONCURRENT_TRACKED_NODES,
          numUnhealthyNodesToRequeue);

      // 按最后更新时间升序排序，使不健康时间最长的节点优先被重新排队
      return unhealthyDns.stream().sorted(PENDING_NODES_QUEUE_COMPARATOR.reversed())
          .limit(numUnhealthyNodesToRequeue);
    }
    return Stream.empty();
  }

  /**
   * 当数据块不满足冗余要求时，将数据块添加到块重构队列进行恢复。
   * 区分退役和维护模式分别判断是否需要重构。
   * @param isDecommission 是否是退役模式
   * @param block 待处理数据块
   * @param num 副本数量统计
   * @param liveReplicas 存活副本数量
   */
  void addReconstructionBlockIfNeeded(boolean isDecommission, BlockInfo block,
      NumberReplicas num, int liveReplicas) {
    // 根据模式判断是否需要块重构
    boolean neededReconstruction = isDecommission ?
        blockManager.isNeededReconstruction(block, num) :
        blockManager.isNeededReconstructionForMaintenance(block, num);
    if (neededReconstruction) {
      // 仅当块不在重构队列、没有等待重构副本、且块队列已经完成初始化时添加
      if (!blockManager.neededReconstruction.contains(block) &&
          blockManager.pendingReconstruction.getNumReplicas(block) == 0 &&
          blockManager.isPopulatingReplQueues()) {
        // 仅在活动NameNode退出安全模式后处理这些块
        blockManager.neededReconstruction.add(block,
            liveReplicas, num.readOnlyReplicas(),
            num.outOfServiceReplicas(),
            blockManager.getExpectedRedundancyNum(block));
      }
    }
  }
}