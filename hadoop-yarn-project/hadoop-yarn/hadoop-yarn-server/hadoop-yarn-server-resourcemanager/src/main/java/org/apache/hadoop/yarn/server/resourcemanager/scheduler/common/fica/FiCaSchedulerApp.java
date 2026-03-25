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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NMToken;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger.AuditConstants;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerFinishedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerReservedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractUsersManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AppSchedulingInfo;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueResourceQuotas;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceLimits;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedContainerChangeRequest;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivitiesManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AbstractCSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AbstractLeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSAMContainerLaunchDiagnosticsConstants;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSAssignment;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityHeadroomProvider;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacities;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.SchedulingMode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.allocator.AbstractContainerAllocator;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.allocator.ContainerAllocator;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.ApplicationSchedulingConfig;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.ContainerAllocationProposal;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.ContainerRequest;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.PendingAsk;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.ResourceCommitRequest;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.SchedulerContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.AppPlacementAllocator;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.CandidateNodeSet;
import org.apache.hadoop.yarn.server.scheduler.SchedulerRequestKey;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * 表示FIFO或Capacity调度器视角下的应用尝试(Application Attempt)
 * 封装了应用尝试在调度过程中的状态信息和调度操作
 */
@Private
@Unstable
public class FiCaSchedulerApp extends SchedulerApplicationAttempt {
  private static final Logger LOG =
      LoggerFactory.getLogger(FiCaSchedulerApp.class);

  // 待抢占容器ID集合
  private final Set<ContainerId> containersToPreempt =
    new HashSet<ContainerId>();

  private CapacityHeadroomProvider headroomProvider;

  // 资源计算器实例
  private ResourceCalculator rc = new DefaultResourceCalculator();

  // 资源调度器实例
  private ResourceScheduler scheduler;

  // 容器分配器实例
  private AbstractContainerAllocator containerAllocator;

  // 应用是否可运行
  private boolean runnable;

  /**
   * 存储应用无法从节点分配容器时的诊断信息
   */
  private String appSkipNodeDiagnostics;

  // 待移除的容器资源增量请求集合
  private Map<ContainerId, SchedContainerChangeRequest> toBeRemovedIncRequests =
      new ConcurrentHashMap<>();

  /**
   * 构造FiCaSchedulerApp实例
   * @param applicationAttemptId 应用尝试ID
   * @param user 提交应用的用户
   * @param queue 应用所属队列
   * @param abstractUsersManager 用户管理器
   * @param rmContext RM上下文
   */
  public FiCaSchedulerApp(ApplicationAttemptId applicationAttemptId,
      String user, Queue queue, AbstractUsersManager abstractUsersManager,
      RMContext rmContext) {
    this(applicationAttemptId, user, queue, abstractUsersManager, rmContext,
        Priority.newInstance(0), false);
  }

  /**
   * 构造FiCaSchedulerApp实例
   * @param applicationAttemptId 应用尝试ID
   * @param user 提交应用的用户
   * @param queue 应用所属队列
   * @param abstractUsersManager 用户管理器
   * @param rmContext RM上下文
   * @param appPriority 应用优先级
   * @param isAttemptRecovering 是否是恢复的应用尝试
   */
  public FiCaSchedulerApp(ApplicationAttemptId applicationAttemptId,
      String user, Queue queue, AbstractUsersManager abstractUsersManager,
      RMContext rmContext, Priority appPriority, boolean isAttemptRecovering) {
    this(applicationAttemptId, user, queue, abstractUsersManager, rmContext,
        appPriority, isAttemptRecovering, null);
  }

  /**
   * 完整构造FiCaSchedulerApp实例，初始化AM资源、分区和容器分配器
   * @param applicationAttemptId 应用尝试ID
   * @param user 提交应用的用户
   * @param queue 应用所属队列
   * @param abstractUsersManager 用户管理器
   * @param rmContext RM上下文
   * @param appPriority 应用优先级
   * @param isAttemptRecovering 是否是恢复的应用尝试
   * @param activitiesManager 调度活动管理器
   */
  public FiCaSchedulerApp(ApplicationAttemptId applicationAttemptId,
      String user, Queue queue, AbstractUsersManager abstractUsersManager,
      RMContext rmContext, Priority appPriority, boolean isAttemptRecovering,
      ActivitiesManager activitiesManager) {
    super(applicationAttemptId, user, queue, abstractUsersManager, rmContext);
    this.runnable = true;

    // 获取RM中对应的应用实例
    RMApp rmApp = rmContext.getRMApps().get(getApplicationId());

    Resource amResource;
    String partition;

    // 如果没有找到应用或AM资源请求为空
    if (rmApp == null || rmApp.getAMResourceRequests() == null
        || rmApp.getAMResourceRequests().isEmpty()) {
      // 无法获取AM请求，使用调度器最小资源作为默认AM资源
      amResource = rmContext.getScheduler().getMinimumResourceCapability();
      partition = CommonNodeLabelsManager.NO_LABEL;
    } else {
      // 从应用获取AM请求中提取资源和节点标签分区
      amResource = rmApp.getAMResourceRequests().get(0).getCapability();
      partition =
          (rmApp.getAMResourceRequests().get(0)
              .getNodeLabelExpression() == null)
          ? CommonNodeLabelsManager.NO_LABEL
          : rmApp.getAMResourceRequests().get(0).getNodeLabelExpression();
    }

    // 设置AM节点分区和资源
    setAppAMNodePartitionName(partition);
    setAMResource(partition, amResource);
    // 设置应用优先级和恢复状态
    setPriority(appPriority);
    setAttemptRecovering(isAttemptRecovering);

    // 从RM上下文获取调度器实例
    scheduler = rmContext.getScheduler();

    // 使用调度器的资源计算器，如果存在的话
    if (scheduler.getResourceCalculator() != null) {
      rc = scheduler.getResourceCalculator();
    }

    // 更新应用调度环境中的多节点排序策略
    updateMultiNodeSortingPolicy(rmApp);

    // 创建容器分配器实例
    containerAllocator = new ContainerAllocator(this, rc, rmContext,
        activitiesManager);
  }

  /**
   * 从叶子队列获取多节点排序策略，更新到应用调度环境中
   * @param rmApp RM应用实例
   */
  private void updateMultiNodeSortingPolicy(RMApp rmApp) {
    if (rmApp == null) {
      return;
    }

    String policyClassName = null;
    // 如果是容量调度器，从叶子队列获取排序策略类名
    if (scheduler instanceof CapacityScheduler) {
      policyClassName = getCSLeafQueue().getMultiNodeSortingPolicyClassName();
    }

    // 如果应用调度环境中尚未设置且策略类名不为空，添加到调度环境
    if (!appSchedulingInfo.getApplicationSchedulingEnvs().containsKey(
        ApplicationSchedulingConfig.ENV_MULTI_NODE_SORTING_POLICY_CLASS)
        && policyClassName != null) {
      appSchedulingInfo.getApplicationSchedulingEnvs().put(
          ApplicationSchedulingConfig.ENV_MULTI_NODE_SORTING_POLICY_CLASS,
          policyClassName);
    }
  }

  /**
   * 处理容器完成事件，清理容器，更新资源使用统计
   * @param rmContainer 已完成的RM容器
   * @param containerStatus 容器状态
   * @param event 事件类型
   * @param partition 节点分区
   * @return 是否成功处理完成事件
   */
  public boolean containerCompleted(RMContainer rmContainer,
      ContainerStatus containerStatus, RMContainerEventType event,
      String partition) {
    writeLock.lock();
    try {
      ContainerId containerId = rmContainer.getContainerId();

      // 从活跃容器集合中移除
      if (null == liveContainers.remove(containerId)) {
        return false;
      }

      // 如果在新分配容器列表中也移除
      newlyAllocatedContainers.remove(rmContainer);

      // 通知RMContainer状态变更
      rmContainer.handle(
          new RMContainerFinishedEvent(containerId, containerStatus, event));

      // 从待抢占列表中移除
      containersToPreempt.remove(containerId);

      // 非默认分区才记录分区信息，节省审计日志空间
      String containerPartition = null;
      if (partition != null && !partition.isEmpty()) {
        containerPartition = partition;
      }
      Resource containerResource = rmContainer.getContainer().getResource();
      // 记录容器释放审计日志
      RMAuditLogger.logSuccess(getUser(), AuditConstants.RELEASE_CONTAINER,
          "SchedulerApp", getApplicationId(), containerId, containerResource,
          getQueueName(), containerPartition);

      // 更新队列指标和应用资源使用统计
      queue.getMetrics().releaseResources(partition,
          getUser(), 1, containerResource);
      attemptResourceUsage.decUsed(partition, containerResource);

      // 清空聚合分配统计缓存
      lastMemoryAggregateAllocationUpdateTime = -1;

      return true;
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * 在指定节点分配容器，创建RMContainer实例
   * @param node 目标节点
   * @param schedulerKey 调度请求键
   * @param container YARN容器实例
   * @return 创建的RMContainer实例，分配失败返回null
   */
  public RMContainer allocate(FiCaSchedulerNode node,
      SchedulerRequestKey schedulerKey, Container container) {
    readLock.lock();
    try {

      // 如果应用已经停止，返回null
      if (isStopped) {
        return null;
      }

      // 检查是否还有未满足的资源请求
      if (getOutstandingAsksCount(schedulerKey) <= 0) {
        return null;
      }

      // 获取对应调度键的应用位置分配器
      AppPlacementAllocator<FiCaSchedulerNode> ps =
          appSchedulingInfo.getAppPlacementAllocator(schedulerKey);
      if (null == ps) {
        LOG.warn("Failed to get " + AppPlacementAllocator.class.getName()
            + " for application=" + getApplicationId() + " schedulerRequestKey="
            + schedulerKey);
        return null;
      }

      // 创建新的RMContainer实例
      RMContainer rmContainer = new RMContainerImpl(container, schedulerKey,
          this.getApplicationAttemptId(), node.getNodeID(),
          appSchedulingInfo.getUser(), this.rmContext,
          ps.getPrimaryRequestedNodePartition());

      // 设置队列名称，容量调度器需要做名称规范化
      String qn = this.getQueueName();
      if (this.scheduler instanceof CapacityScheduler) {
        qn = ((CapacityScheduler)this.scheduler).normalizeQueueName(qn);
      }
      ((RMContainerImpl) rmContainer).setQueueName(qn);

      // 更新AM容器分配诊断信息
      updateAMContainerDiagnostics(AMState.ASSIGNED, null);

      return rmContainer;
    } finally {
      readLock.unlock();
    }
  }

  /**
   * 检查RM容器是否已经处于最终状态(已完成)
   * @param rmContainer RM容器实例
   * @return 是否为最终状态
   */
  private boolean rmContainerInFinalState(RMContainer rmContainer) {
    if (null == rmContainer) {
      return false;
    }

    return rmContainer.completed();
  }

  /**
   * 检查资源提交请求中是否有容器已处于最终状态
   * @param request 资源提交请求
   * @return 是否存在任意容器处于最终状态
   */
  private boolean anyContainerInFinalState(
      ResourceCommitRequest<FiCaSchedulerApp, FiCaSchedulerNode> request) {
    // 检查待释放容器
    for (SchedulerContainer<FiCaSchedulerApp, FiCaSchedulerNode> c : request
        .getContainersToRelease()) {
      if (rmContainerInFinalState(c.getRmContainer())) {
        LOG.debug("To-release container={} is in final state",
            c.getRmContainer());
        return true;
      }
    }

    // 检查待分配容器关联的待释放容器
    for (ContainerAllocationProposal<FiCaSchedulerApp, FiCaSchedulerNode> c : request
        .getContainersToAllocate()) {
      for (SchedulerContainer<FiCaSchedulerApp, FiCaSchedulerNode> r : c
            .getToRelease()) {
        if (rmContainerInFinalState(r.getRmContainer())) {
          LOG.debug("To-release container={}, for to a new allocated"
              + " container, is in final state", r.getRmContainer());
          return true;
        }
      }

      // 检查从预留容器分配时的预留容器本身
      if (null != c.getAllocateFromReservedContainer()) {
        if (rmContainerInFinalState(
            c.getAllocateFromReservedContainer().getRmContainer())) {
          LOG.debug("Allocate from reserved container {} is in final state",
              c.getAllocateFromReservedContainer().getRmContainer());
          return true;
        }
      }
    }

    // 检查待预留容器关联的待释放容器
    for (ContainerAllocationProposal<FiCaSchedulerApp, FiCaSchedulerNode> c : request
        .getContainersToReserve()) {
      for (SchedulerContainer<FiCaSchedulerApp, FiCaSchedulerNode> r : c
          .getToRelease()) {
        if (rmContainerInFinalState(r