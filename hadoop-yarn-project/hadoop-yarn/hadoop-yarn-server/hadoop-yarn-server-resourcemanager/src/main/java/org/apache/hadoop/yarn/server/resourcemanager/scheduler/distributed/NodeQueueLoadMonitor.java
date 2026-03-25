// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.distributed;

import org.apache.commons.math3.util.Precision;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.resource.DominantResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.ResourceOption;
import org.apache.hadoop.yarn.server.api.protocolrecords.NMContainerStatus;
import org.apache.hadoop.yarn.server.api.records.OpportunisticContainersStatus;
import org.apache.hadoop.yarn.server.resourcemanager.ClusterMonitor;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_OPP_CONTAINER_ALLOCATION_NODES_NUMBER_USED;

/**
 * 节点队列负载监控器，负责跟踪NodeManager上容器队列的负载指标（队列长度、总等待时间），
 * 并定期对节点按负载从小到大排序，为机会容器分配提供节点选择依据。
 */
public class NodeQueueLoadMonitor implements ClusterMonitor {

  protected final static Logger LOG = LoggerFactory.
      getLogger(NodeQueueLoadMonitor.class);

  protected int numNodesForAnyAllocation =
      DEFAULT_OPP_CONTAINER_ALLOCATION_NODES_NUMBER_USED;

  /**
   * 负载比较器枚举，定义不同的节点负载比较策略。
   */
  public enum LoadComparator implements Comparator<ClusterNode> {
    /**
     * 仅按队列长度排序。分配时不考虑节点可用资源，仅增加队列长度计数。
     */
    QUEUE_LENGTH,
    /**
     * 仅按队列中容器等待时间排序，不考虑资源和队列长度。
     */
    QUEUE_WAIT_TIME,
    /**
     * 先按队列长度排序，再按可用资源排序。分配时优先检查资源可用性，资源充足时不将机会容器放入节点队列。
     */
    QUEUE_LENGTH_THEN_RESOURCES;

    private Resource clusterResource = Resources.none();
    private final DominantResourceCalculator resourceCalculator =
        new DominantResourceCalculator();

    private boolean shouldPerformMinRatioComputation() {
      // 检查集群资源是否有效，即不存在零或负的主要资源
      if (clusterResource == null) {
        return false;
      }

      return !resourceCalculator.isAnyMajorResourceZeroOrNegative(
          clusterResource);
    }

    /**
     * 先比较队列长度（短的在前），再比较按集群资源归一化后的可用资源（可用多的在前）。
     * @param o1 第一个集群节点
     * @param o2 第二个集群节点
     * @return 比较结果，用于排序
     */
    private int compareQueueLengthThenResources(
        final ClusterNode o1, final ClusterNode o2) {
      // 先比较队列长度
      int diff = o1.getQueueLength() - o2.getQueueLength();
      if (diff != 0) {
        return diff;
      }

      // 队列长度相同，比较可用资源
      final Resource availableResource1 = o1.getAvailableResource();
      final Resource availableResource2 = o2.getAvailableResource();

      // 集群资源有效则使用归一化最小比例比较，否则使用原始值比较
      if (shouldPerformMinRatioComputation()) {
        // 计算节点最小可用资源占集群总资源的比例
        final float availableRatio1 =
            resourceCalculator.minRatio(availableResource1, clusterResource);
        final float availableRatio2 =
            resourceCalculator.minRatio(availableResource2, clusterResource);

        // 可用资源比例更高的节点排在前面
        diff = Precision.compareTo(
            availableRatio2, availableRatio1, Precision.EPSILON);
      }

      // 比例相同比较vcpu绝对值
      if (diff == 0) {
        diff = availableResource2.getVirtualCores() - availableResource1.getVirtualCores();
      }

      // vcpu相同比较内存绝对值
      if (diff == 0) {
        diff = Long.compare(availableResource2.getMemorySize(),
            availableResource1.getMemorySize());
      }

      return diff;
    }

    @Override
    public int compare(ClusterNode o1, ClusterNode o2) {
      int diff;
      // 根据当前策略选择比较方法
      switch (this) {
      case QUEUE_LENGTH_THEN_RESOURCES:
        diff = compareQueueLengthThenResources(o1, o2);
        break;
      case QUEUE_WAIT_TIME:
      case QUEUE_LENGTH:
      default:
        diff = getMetric(o1) - getMetric(o2);
        break;
      }

      // 指标相同，按更新时间排序，新更新的排在前面
      if (diff == 0) {
        return (int) (o2.getTimestamp() - o1.getTimestamp());
      }
      return diff;
    }

    @VisibleForTesting
    void setClusterResource(Resource clusterResource) {
      this.clusterResource = clusterResource;
    }

    public ResourceCalculator getResourceCalculator() {
      return resourceCalculator;
    }

    /**
     * 获取当前策略对应的节点负载指标。
     * @param c 集群节点
     * @return 指标值
     */
    public int getMetric(ClusterNode c) {
      switch (this) {
      case QUEUE_WAIT_TIME:
        return c.getQueueWaitTime();
      case QUEUE_LENGTH:
      case QUEUE_LENGTH_THEN_RESOURCES:
      default:
        return c.getQueueLength();
      }
    }

    /**
     * 如果指标低于阈值，则增加指标计数。
     * @param c 集群节点
     * @param incrementSize 增量大小
     * @param requested 请求的资源
     * @return 若低于阈值并成功增加则返回true，否则返回false
     */
    public boolean compareAndIncrement(
        ClusterNode c, int incrementSize, Resource requested) {
      switch (this) {
      case QUEUE_LENGTH_THEN_RESOURCES:
        return c.compareAndIncrementAllocation(
            incrementSize, resourceCalculator, requested);
      case QUEUE_WAIT_TIME:
        // 等待时间策略没有阈值，总是允许分配
        return true;
      case QUEUE_LENGTH:
      default:
        return c.compareAndIncrementAllocation(incrementSize);
      }
    }

    /**
     * 检查节点是否还有容量容纳新的机会容器。
     * @param cn 集群节点
     * @return 节点可用返回true，否则返回false
     */
    public boolean isNodeAvailable(final ClusterNode cn) {
      int queueCapacity = cn.getQueueCapacity();
      int queueLength = cn.getQueueLength();
      if (this == LoadComparator.QUEUE_LENGTH_THEN_RESOURCES) {
        if (queueCapacity <= 0) {
          return queueLength <= 0;
        } else {
          return queueLength < queueCapacity;
        }
      }
      // 队列容量为0时允许分配，分配失败由节点侧处理
      return queueCapacity <= 0 || queueLength < queueCapacity;
    }
  }

  // 定时排序任务执行器
  private final ScheduledExecutorService scheduledExecutor;

  // 按负载从小到大排序后的节点ID列表
  protected final List<NodeId> sortedNodes;
  // 存储所有集群节点信息，key为节点ID
  protected final Map<NodeId, ClusterNode> clusterNodes =
      new ConcurrentHashMap<>();
  // 按主机名映射RMNode，用于本地化分配
  protected final Map<String, RMNode> nodeByHostName =
      new ConcurrentHashMap<>();
  // 按机架名映射节点ID集合，用于机架本地化分配
  protected final Map<String, Set<NodeId>> nodeIdsByRack =
      new ConcurrentHashMap<>();
  // 当前使用的负载比较策略
  protected final LoadComparator comparator;
  // 队列限额计算器，用于动态调整队列长度阈值
  protected QueueLimitCalculator thresholdCalculator;
  // 排序节点列表读写锁
  protected ReentrantReadWriteLock sortedNodesLock = new ReentrantReadWriteLock();
  // 集群节点信息读写锁
  protected ReentrantReadWriteLock clusterNodesLock =
      new ReentrantReadWriteLock();
  // 节点负载重新计算间隔，单位毫秒
  private long nodeComputationInterval;

  // 定期更新排序节点列表的任务
  Runnable computeTask = new Runnable() {
    @Override
    public void run() {
      // 获取写锁更新排序结果
      ReentrantReadWriteLock.WriteLock writeLock = sortedNodesLock.writeLock();
      writeLock.lock();
      try {
        try {
          // 更新排序后的节点列表
          updateSortedNodes();
        } catch (Exception ex) {
          LOG.warn("Got Exception while sorting nodes..", ex);
        }
        // 更新队列阈值
        if (thresholdCalculator != null) {
          thresholdCalculator.update();
        }
      } finally {
        writeLock.unlock();
      }
    }
  };

  @VisibleForTesting
  NodeQueueLoadMonitor(LoadComparator comparator) {
    this.sortedNodes = new ArrayList<>();
    this.comparator = comparator;
    this.scheduledExecutor = null;
  }

  /**
   * 构造节点队列负载监控器。
   * @param nodeComputationInterval 节点负载重新计算间隔，单位毫秒
   * @param comparator 负载比较策略
   * @param numNodes 随机分配节点时，从最负载最低节点中选择的候选数量
   */
  public NodeQueueLoadMonitor(long nodeComputationInterval,
      LoadComparator comparator, int numNodes) {
    this.sortedNodes = new ArrayList<>();
    this.scheduledExecutor = Executors.newScheduledThreadPool(1);
    this.comparator = comparator;
    this.nodeComputationInterval = nodeComputationInterval;
    numNodesForAnyAllocation = numNodes;
  }

  /**
   * 启动监控器，开始定时排序任务。
   */
  public void start() {
    this.scheduledExecutor.scheduleAtFixedRate(computeTask, nodeComputationInterval,
        nodeComputationInterval, TimeUnit.MILLISECONDS);
  }

  /**
   * 更新排序后的节点列表。
   */
  protected void updateSortedNodes() {
    // 对节点排序并提取节点ID
    List<NodeId> nodeIds = sortNodes(true).stream()
        .map(n -> n.nodeId)
        .collect(Collectors.toList());
    sortedNodes.clear();
    sortedNodes.addAll(nodeIds);
  }

  @VisibleForTesting
  List<NodeId> getSortedNodes() {
    return sortedNodes;
  }

  public QueueLimitCalculator getThresholdCalculator() {
    return thresholdCalculator;
  }

  /**
   * 停止监控器，关闭定时任务。
   */
  public void stop() {
    if (scheduledExecutor != null) {
      scheduledExecutor.shutdown();
    }
  }

  @VisibleForTesting
  Map<NodeId, ClusterNode> getClusterNodes() {
    return clusterNodes;
  }

  @VisibleForTesting
  Comparator<ClusterNode> getComparator() {
    return comparator;
  }

  /**
   * 初始化队列限额计算器。
   * @param sigma 标准差阈值系数
   * @param limitMin 最小队列限额
   * @param limitMax 最大队列限额
   */
  public void initThresholdCalculator(float sigma, int limitMin, int limitMax) {
    this.thresholdCalculator =
        new QueueLimitCalculator(this, sigma, limitMin, limitMax);
  }

  @Override
  public void addNode(List<NMContainerStatus> containerStatuses,
      RMNode rmNode) {
    // 添加节点到主机名映射
    this.nodeByHostName.put(rmNode.getHostName(), rmNode);
    // 添加节点到机架映射
    addIntoNodeIdsByRack(rmNode);
    // 首次添加不立即加入负载监控，需要等待第一次心跳更新后才会被纳入
  }

  @Override
  public void removeNode(RMNode removedRMNode) {
    LOG.info("Node delete event for: {}", removedRMNode.getNode().getName());
    // 从主机名映射移除
    this.nodeByHostName.remove(removedRMNode.getHostName());
    // 从机架映射移除
    removeFromNodeIdsByRack(removedRMNode);
    // 获取写锁更新节点集合
    ReentrantReadWriteLock.WriteLock writeLock = clusterNodesLock.writeLock();
    writeLock.lock();
    ClusterNode node;
    try {
      // 从集群节点集合移除
      node = this.clusterNodes.remove(removedRMNode.getNodeID());
      // 节点移除后钩子，供子类扩展
      onNodeRemoved(node);
    } finally {
      writeLock.unlock();
    }
    if (LOG.isDebugEnabled()) {
      if (node != null) {
        LOG.debug("Delete ClusterNode: " + removedRMNode.getNodeID());
      } else {
        LOG.debug("Node not in list!");
      }
    }
  }

  /**
   * 节点移除后的扩展点，供子类重写。
   * @param node 被移除的节点
   */
  protected void onNodeRemoved(ClusterNode node) {
  }

  @Override
  public void updateNode(RMNode rmNode) {
    LOG.debug("Node update event from: {}", rmNode.getNodeID());
    // 获取节点机会容器状态信息
    OpportunisticContainersStatus opportunisticContainersStatus =
        rmNode.getOpportunisticContainersStatus();
    // 状态为空则创建空实例
    if (opportunisticContainersStatus == null) {
      opportunisticContainersStatus =
          OpportunisticContainersStatus.newInstance();
    }

    // 获取写锁更新节点信息
    ReentrantReadWriteLock.WriteLock writeLock = clusterNodesLock.writeLock();
    writeLock.lock();
    try {
      ClusterNode clusterNode = this.clusterNodes.get(rmNode.getNodeID());
      if (clusterNode == null) {
        // 新增节点处理
        onNewNodeAdded(rmNode, opportunisticContainersStatus);
      } else {
        // 已有节点更新处理
        onExistingNodeUpdated(rmNode, clusterNode, opportunisticContainersStatus);
      }
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * 处理新节点添加，根据节点状态和负载指标决定是否加入监控。
   * @param rmNode RM节点信息
   * @param status 机会容器状态
   */
  protected void onNewNodeAdded(
      RMNode rmNode, OpportunisticContainersStatus status) {
    int opportQueueCapacity = status.getOpportQueueCapacity();
    int estimatedQueueWaitTime = status.getEstimatedQueueWaitTime();
    int waitQueueLength = status.getWaitQueueLength();

    // 节点不在退役中，且负载指标有效，则加入监控
    if (rmNode.getState() != NodeState.DECOMMISSIONING &&
        (estimatedQueueWaitTime != -1 ||
            comparator == LoadComparator.QUEUE_LENGTH ||
            comparator == LoadComparator.QUEUE_LENGTH_THEN_RESOURCES)) {
      // 构建节点属性
      final ClusterNode.Properties properties =
          ClusterNode.Properties.newInstance()
              .setQueueWaitTime(estimatedQueueWaitTime)
              .setQueueLength(waitQueueLength)
              .setNodeLabels(rmNode.getNodeLabels())
              .setCapability(rmNode.getTotalCapability())
              .setAllocatedResource(rmNode.getAllocatedContainerResource())
              .setQueueCapacity(opportQueueCapacity)
              .updateTimestamp();

      // 添加到集群节点集合
      this.clusterNodes.put(rmNode.getNodeID(),
          new ClusterNode(rmNode.getNodeID()).setProperties(properties));

      LOG.info(
          "Inserting ClusterNode [{}] with queue wait time [{}] and "
              + "wait queue length [{}]",
          rmNode.getNode(),
          estimatedQueueWaitTime,
          waitQueueLength
      );
    } else {
      // 忽略不符合条件的节点
      LOG.warn(
          "IGNORING ClusterNode [{}