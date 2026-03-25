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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.classification.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger.AuditConstants;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerFinishedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ActiveUsersManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.ContainerRequest;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.PendingAsk;
import org.apache.hadoop.yarn.server.scheduler.SchedulerRequestKey;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

/**
 * 公平调度器视角下的应用尝试表示，维护应用尝试在公平调度器中的状态、资源使用、调度信息
 * 实现了Schedulable接口，可被公平调度器直接调度
 */
@Private
@Unstable
public class FSAppAttempt extends SchedulerApplicationAttempt
    implements Schedulable {

  private static final Logger LOG =
      LoggerFactory.getLogger(FSAppAttempt.class);
  private static final DefaultResourceCalculator RESOURCE_CALCULATOR
      = new DefaultResourceCalculator();

  private final long startTime;
  private final Priority appPriority;
  private Resource demand = Resources.createResource(0);
  private final FairScheduler scheduler;
  private Resource fairShare = Resources.createResource(0, 0);

  // 抢占相关变量锁，保证多线程并发安全
  private final Object preemptionVariablesLock = new Object();
  // 待抢占容器集合
  private final Set<RMContainer> containersToBePreempted = new HashSet<>();
  // 待抢占总资源量
  private final Resource resourcesToBePreempted =
      Resources.clone(Resources.none());

  // 公平份额不足导致的资源饥饿量
  private Resource fairshareStarvation = Resources.none();
  // 上次满足公平份额的时间点
  private long lastTimeAtFairShare;
  // 下次饥饿检查时间点
  private long nextStarvationCheck;

  // 叶子队列分配给本应用的最小份额饥饿量
  private Resource minshareStarvation = Resources.none();

  // 记录应用的节点预留信息，键: 机架名，值: 该机架上被应用预留的节点名集合
  private final Map<String, Set<String>> reservations = new HashMap<>();

  // 黑名单节点列表
  private final List<FSSchedulerNode> blacklistNodeIds = new ArrayList<>();

  // 是否允许抢占AM容器
  private boolean enableAMPreemption;

  /**
   * 延迟调度: 优先调度节点局部性容器，再放宽到机架局部性、跨交换机调度。
   * 通过统计错过的调度机会或等待时间，逐步放松局部性限制，提升数据局部性
   * 存储每个调度请求密钥允许的最大局部性级别
   */
  private final Map<SchedulerRequestKey, NodeType> allowedLocalityLevel =
      new HashMap<>();

  /**
   * 构造公平调度器应用尝试对象
   * @param scheduler 所属公平调度器
   * @param applicationAttemptId 应用尝试ID
   * @param user 提交用户
   * @param queue 所属叶子队列
   * @param activeUsersManager 活跃用户管理器
   * @param rmContext RM上下文
   */
  public FSAppAttempt(FairScheduler scheduler,
      ApplicationAttemptId applicationAttemptId, String user, FSLeafQueue queue,
      ActiveUsersManager activeUsersManager, RMContext rmContext) {
    super(applicationAttemptId, user, queue, activeUsersManager, rmContext);

    this.scheduler = scheduler;
    this.startTime = scheduler.getClock().getTime();
    this.lastTimeAtFairShare = this.startTime;
    this.appPriority = Priority.newInstance(1);
    this.enableAMPreemption = scheduler.getConf()
            .getAMPreemptionEnabled(getQueue().getQueueName());
  }

  /**
   * 获取所属队列的指标对象
   * @return 队列指标
   */
  public QueueMetrics getMetrics() {
    return queue.getMetrics();
  }

  /**
   * 处理容器完成事件，更新各类资源使用统计和状态
   * @param rmContainer 已完成的RM容器对象
   * @param containerStatus 容器状态
   * @param event 事件类型
   */
  void containerCompleted(RMContainer rmContainer,
      ContainerStatus containerStatus, RMContainerEventType event) {
    writeLock.lock();
    try {
      Container container = rmContainer.getContainer();
      ContainerId containerId = container.getId();

      // 从活跃容器列表移除
      if (liveContainers.remove(containerId) == null) {
        LOG.info("Additional complete request on completed container " +
            rmContainer.getContainerId());
        return;
      }

      // 从新分配容器列表移除
      newlyAllocatedContainers.remove(rmContainer);

      // 通知容器状态完成
      rmContainer.handle(
          new RMContainerFinishedEvent(containerId, containerStatus, event));
      LOG.debug("Completed container: {} in state: {} event:{}",
          rmContainer.getContainerId(), rmContainer.getState(), event);


      untrackContainerForPreemption(rmContainer);
      if (containerStatus.getDiagnostics().
          equals(SchedulerUtils.PREEMPTED_CONTAINER)) {
        queue.getMetrics().preemptContainer();
      }

      Resource containerResource = rmContainer.getContainer().getResource();
      RMAuditLogger.logSuccess(getUser(), AuditConstants.RELEASE_CONTAINER,
          "SchedulerApp", getApplicationId(), containerId, containerResource,
          rmContainer.getQueueName(), null);

      // 更新使用指标
      queue.getMetrics().releaseResources(
          rmContainer.getNodeLabelExpression(),
          getUser(), 1, containerResource);
      this.attemptResourceUsage.decUsed(containerResource);
      getQueue().decUsedResource(containerResource);

      // 清空资源聚合缓存
      lastMemoryAggregateAllocationUpdateTime = -1;
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * 内部取消预留方法，更新预留状态和资源统计
   * @param schedulerKey 调度请求密钥
   * @param node 目标节点
   */
  private void unreserveInternal(
      SchedulerRequestKey schedulerKey, FSSchedulerNode node) {
    writeLock.lock();
    try {
      Map<NodeId, RMContainer> reservedContainers = this.reservedContainers.get(
          schedulerKey);
      RMContainer reservedContainer = reservedContainers.remove(
          node.getNodeID());
      if (reservedContainers.isEmpty()) {
        this.reservedContainers.remove(schedulerKey);
      }

      // 重置重预留计数
      resetReReservations(schedulerKey);

      Resource resource = reservedContainer.getContainer().getResource();
      this.attemptResourceUsage.decReserved(resource);

      LOG.info(
          "Application " + getApplicationId() + " unreserved " + " on node "
              + node + ", currently has " + reservedContainers.size()
              + " at priority " + schedulerKey.getPriority()
              + "; currentReservation " + this.attemptResourceUsage
              .getReserved());
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * 从可用资源中扣除黑名单节点上的未分配资源
   * @param availableResources 待更新的可用资源对象
   */
  private void subtractResourcesOnBlacklistedNodes(
      Resource availableResources) {
    if (appSchedulingInfo.getAndResetBlacklistChanged()) {
      blacklistNodeIds.clear();
      blacklistNodeIds.addAll(scheduler.getBlacklistedNodes(this));
    }
    // 遍历所有黑名单节点，扣除其未分配资源
    for (FSSchedulerNode node: blacklistNodeIds) {
      Resources.subtractFromNonNegative(availableResources,
          node.getUnallocatedResource());
    }
  }

  /**
   * 计算应用可分配的剩余资源（headroom），结合集群可用资源、队列公平份额和队列最大份额计算
   */
  @Override
  public Resource getHeadroom() {
    final FSQueue fsQueue = getQueue();
    SchedulingPolicy policy = fsQueue.getPolicy();

    Resource queueFairShare = fsQueue.getFairShare();
    Resource queueUsage = fsQueue.getResourceUsage();
    Resource clusterResource = this.scheduler.getClusterResource();
    Resource clusterUsage = this.scheduler.getRootQueueMetrics()
        .getAllocatedResources();

    // 计算集群整体可用资源 = 集群总资源 - 已使用资源
    Resource clusterAvailableResources =
        Resources.subtract(clusterResource, clusterUsage);
    // 扣除黑名单节点资源
    subtractResourcesOnBlacklistedNodes(clusterAvailableResources);

    // 计算队列最大可用资源 = 队列最大份额 - 队列已使用资源
    Resource queueMaxAvailableResources =
        Resources.subtract(fsQueue.getMaxShare(), queueUsage);
    // 取集群可用和队列最大可用的组件最小值，得到本应用可使用的最大资源
    Resource maxAvailableResource = Resources.componentwiseMin(
        clusterAvailableResources, queueMaxAvailableResources);

    // 由调度策略计算最终剩余资源
    Resource headroom = policy.getHeadroom(queueFairShare,
        queueUsage, maxAvailableResource);
    LOG.debug("Headroom calculation for {}:Min((queueFairShare={} -"
        + " queueUsage={}), maxAvailableResource={} Headroom={}",
        this.getName(), queueFairShare, queueUsage, maxAvailableResource,
        headroom);

    return headroom;
  }

  /**
   * 根据错过的调度机会数量计算当前允许的调度局部性级别，超过阈值则放宽局部性限制
   * @param schedulerKey 调度请求密钥
   * @param numNodes 集群总节点数
   * @param nodeLocalityThreshold 节点局部性阈值（占集群节点比例）
   * @param rackLocalityThreshold 机架局部性阈值（占集群节点比例）
   * @return 当前允许的最大局部性级别
   */
  NodeType getAllowedLocalityLevel(
      SchedulerRequestKey schedulerKey, int numNodes,
      double nodeLocalityThreshold, double rackLocalityThreshold) {
    // 阈值上限修正，最大不超过1.0
    if (nodeLocalityThreshold > 1.0) {
      nodeLocalityThreshold = 1.0;
    }
    if (rackLocalityThreshold > 1.0) {
      rackLocalityThreshold = 1.0;
    }

    // 未启用延迟调度，允许任意位置调度
    if (nodeLocalityThreshold < 0.0 || rackLocalityThreshold < 0.0) {
      return NodeType.OFF_SWITCH;
    }

    writeLock.lock();
    try {

      // 默认初始级别为节点局部性
      if (!allowedLocalityLevel.containsKey(schedulerKey)) {
        allowedLocalityLevel.put(schedulerKey, NodeType.NODE_LOCAL);
        return NodeType.NODE_LOCAL;
      }

      NodeType allowed = allowedLocalityLevel.get(schedulerKey);

      // 已经是最宽松级别，直接返回
      if (allowed.equals(NodeType.OFF_SWITCH)) {
        return NodeType.OFF_SWITCH;
      }

      // 根据当前级别获取对应阈值
      double threshold = allowed.equals(NodeType.NODE_LOCAL) ?
          nodeLocalityThreshold :
          rackLocalityThreshold;

      // 超过阈值则放宽局部性限制
      int schedulingOpportunities = getSchedulingOpportunities(schedulerKey);
      double thresholdNum = numNodes * threshold;
      if (schedulingOpportunities > thresholdNum) {
        if (allowed.equals(NodeType.NODE_LOCAL)) {
          if (LOG.isTraceEnabled()) {
            LOG.trace("SchedulingOpportunities: " + schedulingOpportunities
                + ", nodeLocalityThreshold: " + thresholdNum
                + ", change allowedLocality from NODE_LOCAL to RACK_LOCAL"
                + ", priority: " + schedulerKey.getPriority()
                + ", app attempt id: " + this.attemptId);
          }
          allowedLocalityLevel.put(schedulerKey, NodeType.RACK_LOCAL);
          resetSchedulingOpportunities(schedulerKey);
        } else if (allowed.equals(NodeType.RACK_LOCAL)) {
          if (LOG.isTraceEnabled()) {
            LOG.trace("SchedulingOpportunities: " + schedulingOpportunities
                + ", rackLocalityThreshold: " + thresholdNum
                + ", change allowedLocality from RACK_LOCAL to OFF_SWITCH"
                + ", priority: " + schedulerKey.getPriority()
                + ", app attempt id: " + this.attemptId);
          }
          allowedLocalityLevel.put(schedulerKey, NodeType.OFF_SWITCH);
          resetSchedulingOpportunities(schedulerKey);
        }
      }
      return allowedLocalityLevel.get(schedulerKey);
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * 根据等待时间计算当前允许的调度局部性级别，超过时间阈值则放宽局部性限制
   * @param schedulerKey 调度请求密钥
   * @param nodeLocalityDelayMs 节点局部性等待阈值（毫秒）
   * @param rackLocalityDelayMs 机架局部性等待阈值（毫秒）
   * @param currentTimeMs 当前时间（毫秒）
   * @return 当前允许的最大局部性级别
   */
  NodeType getAllowedLocalityLevelByTime(
      SchedulerRequestKey schedulerKey, long nodeLocalityDelayMs,
      long rackLocalityDelayMs, long currentTimeMs) {
    // 未启用延迟调度，允许任意位置调度
    if (nodeLocalityDelayMs < 0 || rackLocalityDelayMs < 0) {
      return NodeType.OFF_SWITCH;
    }

    writeLock.lock();
    try {

      // 默认初始级别为节点局部性
      if (!allowedLocalityLevel.containsKey(schedulerKey)) {
        // 记录初始时间，避免用应用启动时间计算导致提前降级
        lastScheduledContainer.put(schedulerKey, currentTimeMs);
        LOG.debug("Init the lastScheduledContainer time, priority: {},"
            + " time: {}", schedulerKey.getPriority(), currentTimeMs);
        allowedLocalityLevel.put(schedulerKey, NodeType.NODE_LOCAL);
        return NodeType.NODE_LOCAL;
      }

      NodeType allowed = allowedLocalityLevel.get(schedulerKey);

      // 已经是最宽松级别，直接返回
      if (allowed.equals(NodeType.OFF_SWITCH)) {
        return NodeType.OFF_SWITCH;
      }

      // 计算从上一次成功调度到现在的等待时间
      long waitTime = currentTimeMs;
      if (lastScheduledContainer.containsKey(schedulerKey)) {
        waitTime -= lastScheduledContainer.get(schedulerKey);
      } else{
        waitTime -= getStartTime();
      }

      // 根据当前级别获取对应时间阈值
      long thresholdTime = allowed.equals(NodeType.NODE_LOCAL) ?
          nodeLocalityDelayMs :
          rackLocalityDelayMs;

      // 超过时间阈值则放宽局部性限制
      if (waitTime > thresholdTime) {
        if (allowed.equals(NodeType.NODE_LOCAL)) {