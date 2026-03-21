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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerUpdateType;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.LogAggregationContext;
import org.apache.hadoop.yarn.api.records.NMToken;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.SchedulingRequest;
import org.apache.hadoop.yarn.api.records.UpdateContainerError;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.server.api.ContainerType;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.RMServerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.AggregateAppResourceUsage;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerReservedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerUpdatesAcquiredEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeCleanContainerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeUpdateContainerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.SchedulingMode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.ContainerRequest;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.PendingAsk;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.AppPlacementAllocator;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy.SchedulableEntity;
import org.apache.hadoop.yarn.server.scheduler.OpportunisticContainerContext;
import org.apache.hadoop.yarn.server.scheduler.SchedulerRequestKey;
import org.apache.hadoop.yarn.state.InvalidStateTransitionException;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.collect.ConcurrentHashMultiset;

/**
 * 从调度器视角表示一个应用尝试，RM中每个运行的应用尝试对应一个该类实例
 * 核心职责：维护应用尝试在调度器层面的状态信息、资源使用、容器生命周期管理
 */
@Private
@Unstable
public class SchedulerApplicationAttempt implements SchedulableEntity {
  
  private static final Logger LOG = LoggerFactory
      .getLogger(SchedulerApplicationAttempt.class);

  private FastDateFormat fdf =
      FastDateFormat.getInstance("EEE MMM dd HH:mm:ss Z yyyy");

  // 内存聚合分配缓存有效期（毫秒），缓存过期后才重新计算全量聚合资源
  private static final long MEM_AGGREGATE_ALLOCATION_CACHE_MSECS = 3000;
  protected long lastMemoryAggregateAllocationUpdateTime = 0;
  private Map<String, Long> lastResourceSecondsMap = new HashMap<>();
  protected final AppSchedulingInfo appSchedulingInfo;
  protected ApplicationAttemptId attemptId;
  protected Map<ContainerId, RMContainer> liveContainers =
      new ConcurrentHashMap<>();
  protected final Map<SchedulerRequestKey, Map<NodeId, RMContainer>>
      reservedContainers = new HashMap<>();

  private final ConcurrentHashMultiset<SchedulerRequestKey> reReservations =
      ConcurrentHashMultiset.create();
  
  private volatile Resource resourceLimit = Resource.newInstance(0, 0);
  private boolean unmanagedAM = true;
  private boolean amRunning = false;
  private LogAggregationContext logAggregationContext;

  private volatile Priority appPriority = null;
  private boolean isAttemptRecovering;

  protected ResourceUsage attemptResourceUsage = new ResourceUsage();
  /** 机会容器的资源使用情况 */
  protected ResourceUsage attemptOpportunisticResourceUsage =
      new ResourceUsage();
  /** 远程调度器分配的资源使用情况 */
  protected ResourceUsage attemptResourceUsageAllocatedRemotely =
      new ResourceUsage();
  private AtomicLong firstAllocationRequestSentTime = new AtomicLong(0);
  private AtomicLong firstContainerAllocatedTime = new AtomicLong(0);

  protected List<RMContainer> newlyAllocatedContainers = new ArrayList<>();
  protected List<RMContainer> tempContainerToKill = new ArrayList<>();
  protected Map<ContainerId, RMContainer> newlyPromotedContainers = new HashMap<>();
  protected Map<ContainerId, RMContainer> newlyDemotedContainers = new HashMap<>();
  protected Map<ContainerId, RMContainer> newlyDecreasedContainers = new HashMap<>();
  protected Map<ContainerId, RMContainer> newlyIncreasedContainers = new HashMap<>();
  protected Set<NMToken> updatedNMTokens = new HashSet<>();

  protected List<UpdateContainerError> updateContainerErrors = new ArrayList<>();

  // 记录来自前一次尝试、尚未上报给AM的已恢复容器
  private List<Container> recoveredPreviousAttemptContainers =
      new ArrayList<>();

  // 工作保留恢复场景下，跟踪AM未完成的容器释放请求
  // 恢复过程中AM可能先于NM状态到达，此时NM恢复的对应容器不应被恢复
  private Set<ContainerId> pendingRelease = null;

  private OpportunisticContainerContext oppContainerContext;

  /**
   * 统计每个优先级已给出的调度机会次数
   * 每次调度询问该优先级任务时计数递增，成功调度节点/机架局部性任务后重置为0
   * 用于延迟调度策略控制局部性阈值放松
   */
  private ConcurrentHashMultiset<SchedulerRequestKey> schedulingOpportunities =
      ConcurrentHashMultiset.create();

  /**
   * 统计每个优先级未满足的非分区资源请求调度机会次数
   * 每次调度询问时递增，成功调度任何对应优先级任务后重置为0
   */
  private ConcurrentHashMultiset<SchedulerRequestKey>
      missedNonPartitionedReqSchedulingOpportunity =
      ConcurrentHashMultiset.create();
  
  // 各优先级最后一次成功调度容器的时间，用于连续调度优化
  protected Map<SchedulerRequestKey, Long> lastScheduledContainer =
      new ConcurrentHashMap<>();

  protected volatile Queue queue;
  protected volatile boolean isStopped = false;

  protected String appAMNodePartitionName = CommonNodeLabelsManager.NO_LABEL;

  protected final RMContext rmContext;

  private RMAppAttempt appAttempt;

  protected ReentrantReadWriteLock.ReadLock readLock;
  protected ReentrantReadWriteLock.WriteLock writeLock;

  private Map<String, String> applicationSchedulingEnvs = new HashMap<>();

  // 未确认分配资源，用于避免重复分配导致过多提案被拒绝
  private AtomicLong unconfirmedAllocatedMem = new AtomicLong();
  private AtomicInteger unconfirmedAllocatedVcores = new AtomicInteger();

  private String nodeLabelExpression;

  private final long startTime;

  /**
   * 构造调度器层面的应用尝试对象
   * @param applicationAttemptId 应用尝试ID
   * @param user 提交应用的用户名
   * @param queue 应用所属调度队列
   * @param abstractUsersManager 用户管理器
   * @param rmContext RM上下文对象
   */
  public SchedulerApplicationAttempt(ApplicationAttemptId applicationAttemptId, 
      String user, Queue queue, AbstractUsersManager abstractUsersManager,
      RMContext rmContext) {
    Preconditions.checkNotNull(rmContext, "RMContext should not be null");
    this.rmContext = rmContext;
    this.queue = queue;
    this.pendingRelease = Collections.newSetFromMap(
        new ConcurrentHashMap<ContainerId, Boolean>());
    this.attemptId = applicationAttemptId;
    // 从RM上下文获取应用提交信息，初始化应用元数据
    if (rmContext.getRMApps() != null &&
        rmContext.getRMApps()
            .containsKey(applicationAttemptId.getApplicationId())) {
      RMApp rmApp = rmContext.getRMApps().get(applicationAttemptId.getApplicationId());
      ApplicationSubmissionContext appSubmissionContext =
          rmApp
              .getApplicationSubmissionContext();
      appAttempt = rmApp.getCurrentAppAttempt();
      if (appSubmissionContext != null) {
        unmanagedAM = appSubmissionContext.getUnmanagedAM();
        this.logAggregationContext =
            appSubmissionContext.getLogAggregationContext();
        this.nodeLabelExpression =
            appSubmissionContext.getNodeLabelExpression();
      }
      applicationSchedulingEnvs = rmApp.getApplicationSchedulingEnvs();
    }

    this.appSchedulingInfo = new AppSchedulingInfo(applicationAttemptId, user,
        queue, abstractUsersManager, rmContext.getEpoch(), attemptResourceUsage,
        applicationSchedulingEnvs, rmContext, unmanagedAM);
    // 初始化读写锁，保护并发访问
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    readLock = lock.readLock();
    writeLock = lock.writeLock();
    startTime = SystemClock.getInstance().getTime();
  }

  /**
   * 设置机会容器上下文，用于机会容器调度管理
   * @param oppContext 机会容器上下文
   */
  public void setOpportunisticContainerContext(
      OpportunisticContainerContext oppContext) {
    this.oppContainerContext = oppContext;
  }

  /**
   * 获取机会容器上下文
   * @return 机会容器上下文
   */
  public OpportunisticContainerContext
      getOpportunisticContainerContext() {
    return this.oppContainerContext;
  }

  /**
   * 获取应用当前所有运行中容器
   * @return 运行中容器集合
   */
  public Collection<RMContainer> getLiveContainers() {
    readLock.lock();
    try {
      return new ArrayList<>(liveContainers.values());
    } finally {
      readLock.unlock();
    }
  }

  public AppSchedulingInfo getAppSchedulingInfo() {
    return this.appSchedulingInfo;
  }

  public ContainerUpdateContext getUpdateContext() {
    return this.appSchedulingInfo.getUpdateContext();
  }

  /**
   * 判断应用是否处于待调度状态
   * @return 是否待调度
   */
  public boolean isPending() {
    return appSchedulingInfo.isPending();
  }
  
  /**
   * 获取应用尝试ID
   * @return 应用尝试ID
   */
  public ApplicationAttemptId getApplicationAttemptId() {
    return appSchedulingInfo.getApplicationAttemptId();
  }
  
  public ApplicationId getApplicationId() {
    return appSchedulingInfo.getApplicationId();
  }
  
  public String getUser() {
    return appSchedulingInfo.getUser();
  }

  public Set<ContainerId> getPendingRelease() {
    return this.pendingRelease;
  }

  public long getNewContainerId() {
    return appSchedulingInfo.getNewContainerId();
  }

  public Collection<SchedulerRequestKey> getSchedulerKeys() {
    return appSchedulingInfo.getSchedulerKeys();
  }

  public PendingAsk getPendingAsk(
      SchedulerRequestKey schedulerKey, String resourceName) {
    readLock.lock();
    try {
      return appSchedulingInfo.getPendingAsk(schedulerKey, resourceName);
    } finally {
      readLock.unlock();
    }
  }

  public int getOutstandingAsksCount(SchedulerRequestKey schedulerKey) {
    return getOutstandingAsksCount(schedulerKey, ResourceRequest.ANY);
  }

  public int getOutstandingAsksCount(SchedulerRequestKey schedulerKey,
      String resourceName) {
    readLock.lock();
    try {
      AppPlacementAllocator ap = appSchedulingInfo.getAppPlacementAllocator(
          schedulerKey);
      return ap == null ? 0 : ap.getOutstandingAsksCount(resourceName);
    } finally {
      readLock.unlock();
    }
  }

  public String getQueueName() {
    return appSchedulingInfo.getQueueName();
  }
  
  public Resource getAMResource() {
    return attemptResourceUsage.getAMUsed();
  }

  public Resource getAMResource(String label) {
    return attemptResourceUsage.getAMUsed(label);
  }

  public void setAMResource(Resource amResource) {
    attemptResourceUsage.setAMUsed(amResource);
  }

  public void setAMResource(String label, Resource amResource) {
    attemptResourceUsage.setAMUsed(label, amResource);
  }

  public boolean isAmRunning() {
    return amRunning;
  }

  public void setAmRunning(boolean bool) {
    amRunning = bool;
  }

  public boolean getUnmanagedAM() {
    return unmanagedAM;
  }

  public RMContainer getRMContainer(ContainerId id) {
    return liveContainers.get(id);
  }

  /**
   * 添加RM容器到运行容器集合，更新对应资源统计
   * @param id 容器ID
   * @param rmContainer RM容器对象
   */
  public void addRMContainer(
      ContainerId id, RMContainer rmContainer) {
    writeLock.lock();
    try {
      // 记录来自前一次尝试的已恢复容器
      if (!getApplicationAttemptId().equals(
          rmContainer.getApplicationAttemptId()) &&
          !liveContainers.containsKey(id)) {
        LOG.info("recovered container " + id +
            " from previous attempt " + rmContainer.getApplicationAttemptId());
        recoveredPreviousAttemptContainers.add(rmContainer.getContainer());
      }
      liveContainers.put(id, rmContainer);
      // 机会容器单独统计资源
      if (rmContainer.getExecutionType() == ExecutionType.OPPORTUNISTIC) {
        this.attemptOpportunisticResourceUsage.incUsed(
            rmContainer.getAllocatedResource());
      }
      // 远程分配容器单独统计资源
      if (rmContainer.isRemotelyAllocated()) {
        this.attemptResourceUsageAllocatedRemotely.incUsed(
            rmContainer.getAllocatedResource());
      }
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * 从运行容器集合移除RM容器，更新对应资源统计
   * @param containerId 待移除容器ID
   * @return 是否成功移除
   */
  public boolean removeRMContainer(ContainerId containerId) {
    writeLock.lock();
    try {
      RMContainer rmContainer = liveContainers.remove(containerId);
      if (rmContainer != null) {
        if (rmContainer.getExecutionType() == ExecutionType.OPPORTUNISTIC) {
          this.attemptOpportunisticResourceUsage
              .decUsed(rmContainer.getAllocatedResource());
        }
        if (rmContainer.is