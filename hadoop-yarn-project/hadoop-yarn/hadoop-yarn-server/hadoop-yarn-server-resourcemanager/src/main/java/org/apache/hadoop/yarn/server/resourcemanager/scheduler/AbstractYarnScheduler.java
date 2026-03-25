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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.ContainerUpdateType;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceOption;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.SchedulingRequest;
import org.apache.hadoop.yarn.api.records.UpdateContainerError;
import org.apache.hadoop.yarn.api.records.UpdateContainerRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.InvalidResourceRequestException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.SchedulerResourceTypes;
import org.apache.hadoop.yarn.server.api.protocolrecords.NMContainerStatus;
import org.apache.hadoop.yarn.server.metrics.OpportunisticSchedulerMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.RMAppManagerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.RMAppManagerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger.AuditConstants;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.RMCriticalThreadUncaughtExceptionHandler;
import org.apache.hadoop.yarn.server.resourcemanager.RMServerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.monitor.SchedulingMonitorManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerFinishedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer
    .RMContainerNMDoneChangeResourceEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerRecoverEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeCleanContainerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeFinishedContainersPulledByAMEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeResourceUpdateEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.UpdatedContainerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivitiesManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.ContainerRequest;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.QueueEntitlement;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.ContainerPreemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.ReleaseContainerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairSchedulerConfiguration;
import org.apache.hadoop.yarn.server.scheduler.OpportunisticContainerContext;
import org.apache.hadoop.yarn.server.scheduler.SchedulerRequestKey;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.server.utils.Lock;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.apache.hadoop.yarn.util.resource.Resources;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.SettableFuture;
import org.apache.hadoop.util.concurrent.SubjectInheritingThread;

/**
 * YARN资源调度器抽象基类，定义了调度器核心公共能力，为具体调度器实现提供基础骨架
 */
@SuppressWarnings("unchecked")
@Private
@Unstable
public abstract class AbstractYarnScheduler
    <T extends SchedulerApplicationAttempt, N extends SchedulerNode>
    extends AbstractService implements ResourceScheduler {

  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractYarnScheduler.class);

  private static final Resource ZERO_RESOURCE = Resource.newInstance(0, 0);

  // 集群节点追踪器，管理所有调度节点
  protected final ClusterNodeTracker<N> nodeTracker =
      new ClusterNodeTracker<>();

  // 最小资源分配量
  protected Resource minimumAllocation;

  // RM上下文对象，保存RM全局状态
  protected volatile RMContext rmContext;

  // 集群允许的最高应用优先级
  private volatile Priority maxClusterLevelAppPriority;

  // 调度活动追踪管理器，用于调度诊断
  protected ActivitiesManager activitiesManager;
  // 调度健康状态记录
  protected SchedulerHealth schedulerHealth = new SchedulerHealth();
  // 上一次节点更新时间戳
  protected volatile long lastNodeUpdateTime;

  // 服务停止时线程join超时时间
  protected final long THREAD_JOIN_TIMEOUT_MS = 1000;

  // 时钟对象，用于计时，可测试注入
  private volatile Clock clock;

  /**
   * To enable the update thread, subclasses should set updateInterval to a
   * positive value during {@link #serviceInit(Configuration)}.
   */
  // 定期更新线程执行间隔，-1表示不启动更新线程
  protected long updateInterval = -1L;
  @VisibleForTesting
  // 定期更新线程实例
  Thread updateThread;
  // 更新线程同步监视器对象
  private final Object updateThreadMonitor = new Object();
  // 延迟清理定时器，用于清理待释放容器缓存
  private Timer releaseCache;
  // 是否自动纠正容器分配超额
  private boolean autoCorrectContainerAllocation;

  /*
   * All schedulers which are inheriting AbstractYarnScheduler should use
   * concurrent version of 'applications' map.
   */
  // 调度应用映射表，key为应用ID，value为调度应用对象
  protected ConcurrentMap<ApplicationId, SchedulerApplication<T>> applications;
  // NodeManager过期间隔
  protected int nmExpireInterval;
  // NodeManager心跳间隔
  protected long nmHeartbeatInterval;
  // 跳过不健康节点的间隔
  private long skipNodeInterval;

  private final static List<Container> EMPTY_CONTAINER_LIST =
      new ArrayList<Container>();
  protected static final Allocation EMPTY_ALLOCATION = new Allocation(
    EMPTY_CONTAINER_LIST, Resources.createResource(0), null, null, null);

  // 调度器读锁，用于保护共享资源并发读
  protected final ReentrantReadWriteLock.ReadLock readLock;

  /*
   * Use writeLock for any of operations below:
   * - queue change (hierarchy / configuration / container allocation)
   * - application(add/remove/allocate-container, but not include container
   *   finish)
   * - node (add/remove/change-resource/container-allocation, but not include
   *   container finish)
   */
  // 调度器写锁，用于保护结构变更操作
  protected final ReentrantReadWriteLock.WriteLock writeLock;

  // If set to true, then ALL container updates will be automatically sent to
  // the NM in the next heartbeat.
  // 是否自动推送容器更新到NodeManager
  private boolean autoUpdateContainers = false;

  // 调度监控管理器，管理各类调度监控规则
  protected SchedulingMonitorManager schedulingMonitorManager =
      new SchedulingMonitorManager();

  // 是否为迁移模式
  private boolean migration;

  /**
   * Construct the service.
   *
   * @param name service name
   */
  public AbstractYarnScheduler(String name) {
    super(name);
    clock = SystemClock.getInstance();
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    readLock = lock.readLock();
    writeLock = lock.writeLock();
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    // 从配置读取是否开启迁移模式
    migration =
        conf.getBoolean(FairSchedulerConfiguration.MIGRATION_MODE, false);

    nmExpireInterval =
        conf.getInt(YarnConfiguration.RM_NM_EXPIRY_INTERVAL_MS,
          YarnConfiguration.DEFAULT_RM_NM_EXPIRY_INTERVAL_MS);
    nmHeartbeatInterval =
        conf.getLong(YarnConfiguration.RM_NM_HEARTBEAT_INTERVAL_MS,
            YarnConfiguration.DEFAULT_RM_NM_HEARTBEAT_INTERVAL_MS);
    skipNodeInterval = YarnConfiguration.getSkipNodeInterval(conf);
    autoCorrectContainerAllocation =
        conf.getBoolean(YarnConfiguration.RM_SCHEDULER_AUTOCORRECT_CONTAINER_ALLOCATION,
            YarnConfiguration.DEFAULT_RM_SCHEDULER_AUTOCORRECT_CONTAINER_ALLOCATION);
    long configuredMaximumAllocationWaitTime =
        conf.getLong(YarnConfiguration.RM_WORK_PRESERVING_RECOVERY_SCHEDULING_WAIT_MS,
          YarnConfiguration.DEFAULT_RM_WORK_PRESERVING_RECOVERY_SCHEDULING_WAIT_MS);
    nodeTracker.setConfiguredMaxAllocationWaitTime(
        configuredMaximumAllocationWaitTime);
    // 从配置读取集群最大应用优先级
    maxClusterLevelAppPriority = getMaxPriorityFromConf(conf);
    // 非迁移模式下创建延迟清理定时器
    if (!migration) {
      this.releaseCache = new Timer("Pending Container Clear Timer");
    }

    autoUpdateContainers =
        conf.getBoolean(YarnConfiguration.RM_AUTO_UPDATE_CONTAINERS,
            YarnConfiguration.DEFAULT_RM_AUTO_UPDATE_CONTAINERS);

    // 如果配置了更新间隔，启动定期更新线程
    if (updateInterval > 0) {
      updateThread = new UpdateThread();
      updateThread.setName("SchedulerUpdateThread");
      updateThread.setUncaughtExceptionHandler(
          new RMCriticalThreadUncaughtExceptionHandler(rmContext));
      updateThread.setDaemon(true);
    }
    super.serviceInit(conf);

  }

  @Override
  protected void serviceStart() throws Exception {
    // 非迁移模式下启动线程和监控
    if (!migration) {
      if (updateThread != null) {
        updateThread.start();
      }
      schedulingMonitorManager.startAll();
      createReleaseCache();
    }

    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    // 停止更新线程
    if (updateThread != null) {
      updateThread.interrupt();
      updateThread.join(THREAD_JOIN_TIMEOUT_MS);
    }

    // 停止定时器
    if (releaseCache != null) {
      releaseCache.cancel();
      releaseCache = null;
    }
    schedulingMonitorManager.stop();
    super.serviceStop();
  }

  @VisibleForTesting
  public ClusterNodeTracker<N> getNodeTracker() {
    return nodeTracker;
  }

  @VisibleForTesting
  public SchedulingMonitorManager getSchedulingMonitorManager() {
    return schedulingMonitorManager;
  }

  /*
   * YARN-3136 removed synchronized lock for this method for performance
   * purposes
   */
  /**
   * 获取需要转移给新尝试的容器列表
   * @param currentAttempt 当前应用尝试ID
   * @return 可转移容器列表
   */
  public List<Container> getTransferredContainers(
      ApplicationAttemptId currentAttempt) {
    ApplicationId appId = currentAttempt.getApplicationId();
    SchedulerApplication<T> app = applications.get(appId);
    List<Container> containerList = new ArrayList<Container>();
    if (app == null) {
      return containerList;
    }
    Collection<RMContainer> liveContainers = app.getCurrentAppAttempt()
        .pullContainersToTransfer();
    ContainerId amContainerId = null;
    // For UAM, amContainer would be null
    if (rmContext.getRMApps().get(appId).getCurrentAppAttempt()
        .getMasterContainer() != null) {
      amContainerId = rmContext.getRMApps().get(appId).getCurrentAppAttempt()
          .getMasterContainer().getId();
    }
    // 排除AM容器，只返回普通容器
    for (RMContainer rmContainer : liveContainers) {
      if (!rmContainer.getContainerId().equals(amContainerId)) {
        containerList.add(rmContainer.getContainer());
      }
    }
    return containerList;
  }

  public Map<ApplicationId, SchedulerApplication<T>>
      getSchedulerApplications() {
    return applications;
  }

  /**
   * Add blacklisted NodeIds to the list that is passed.
   *
   * @param app application attempt.
   * @return blacklisted NodeIds.
   */
  /**
   * 获取应用黑名单中的节点列表
   * @param app 目标应用尝试
   * @return 黑名单节点列表
   */
  public List<N> getBlacklistedNodes(final SchedulerApplicationAttempt app) {

    NodeFilter nodeFilter = new NodeFilter() {
      @Override
      public boolean accept(SchedulerNode node) {
        return SchedulerAppUtils.isPlaceBlacklisted(app, node, LOG);
      }
    };
    return nodeTracker.getNodes(nodeFilter);
  }

  public List<N> getNodes(final NodeFilter filter) {
    return nodeTracker.getNodes(filter);
  }

  public boolean shouldContainersBeAutoUpdated() {
    return this.autoUpdateContainers;
  }

  @Override
  public Resource getClusterResource() {
    return nodeTracker.getClusterCapacity();
  }

  @Override
  public Resource getMinimumResourceCapability() {
    return minimumAllocation;
  }

  @Override
  public Resource getMaximumResourceCapability() {
    return nodeTracker.getMaxAllowedAllocation();
  }

  @Override
  public Resource getMaximumResourceCapability(String queueName) {
    return getMaximumResourceCapability();
  }

  protected void initMaximumResourceCapability(Resource maximumAllocation) {
    nodeTracker.setConfiguredMaxAllocation(maximumAllocation);
  }

  public SchedulerHealth getSchedulerHealth() {
    return this.schedulerHealth;
  }

  protected void setLastNodeUpdateTime(long time) {
    this.lastNodeUpdateTime = time;
  }

  public long getLastNodeUpdateTime() {
    return lastNodeUpdateTime;
  }

  public long getSkipNodeInterval(){
    return skipNodeInterval;
  }

  /**
   * 处理节点上新启动容器事件
   * @param containerId 容器ID
   * @param node 节点对象
   */
  protected void containerLaunchedOnNode(
      ContainerId containerId, SchedulerNode node) {
    readLock.lock();
    try {
      // 获取容器所属应用尝试
      SchedulerApplicationAttempt application =
          getCurrentAttemptForContainer(containerId);
      if (application == null) {
        LOG.info