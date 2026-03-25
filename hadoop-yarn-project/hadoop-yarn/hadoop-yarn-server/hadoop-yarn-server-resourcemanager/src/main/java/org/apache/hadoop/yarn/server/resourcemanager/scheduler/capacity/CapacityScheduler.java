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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.server.resourcemanager.RMCriticalThreadUncaughtExceptionHandler;
import org.apache.hadoop.yarn.server.resourcemanager.placement.ApplicationPlacementContext;
import org.apache.hadoop.yarn.server.resourcemanager.placement.CSMappingPlacementRule;
import org.apache.hadoop.yarn.server.resourcemanager.placement.PlacementFactory;
import org.apache.hadoop.yarn.server.resourcemanager.placement.PlacementRule;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;
import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.concurrent.SubjectInheritingThread;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.NodeAttribute;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceOption;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.ResourceSizing;
import org.apache.hadoop.yarn.api.records.SchedulingRequest;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.SchedulerResourceTypes;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.RMState;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.ApplicationStateData;

import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationConstants;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AppSchedulingInfo;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ContainerUpdates;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.MutableConfScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.MutableConfigurationProvider;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.PreemptableResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueInvalidException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceLimits;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceUsage;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplication;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerDynamicEditException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivitiesLogger;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivitiesManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivityDiagnosticConstant;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivityState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.AllocationState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf.CSConfigurationProvider;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf.FileBasedCSConfigurationProvider;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf.MutableCSConfigurationProvider;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.preemption.KillableContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.preemption.PreemptionManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.AssignmentInformation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.ContainerAllocationProposal;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.QueueEntitlement;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.ResourceAllocationCommitter;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.ResourceCommitRequest;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.SchedulerContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.InvalidAllocationTagsQueryException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.PlacementConstraintsUtil;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.ContainerExpiredSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.ContainerPreemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAttributesUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeLabelsUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeResourceUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event
    .QueueManagementChangeEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AutoCreatedQueueDeletionEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.ReleaseContainerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.CandidateNodeSet;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.CandidateNodeSetUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.SimpleCandidateNodeSet;
import org.apache.hadoop.yarn.server.resourcemanager.security.AppPriorityACLsManager;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.scheduler.SchedulerRequestKey;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.server.utils.Lock;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.apache.hadoop.yarn.util.resource.Resources;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.SettableFuture;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.QUEUE_MAPPING;

/**
 * 容量调度器实现，YARN默认的多租户资源调度器，按队列容量比例分配集群资源
 * 支持层级队列配置、资源弹性共享、多租户隔离、抢占、动态队列修改等能力
 */
@LimitedPrivate("yarn")
@Evolving
@SuppressWarnings("unchecked")
public class CapacityScheduler extends
    AbstractYarnScheduler<FiCaSchedulerApp, FiCaSchedulerNode> implements
    PreemptableResourceScheduler, CapacitySchedulerContext, Configurable,
    ResourceAllocationCommitter, MutableConfScheduler {

  private static final Marker FATAL =
      MarkerFactory.getMarker("FATAL");
  private static final Logger LOG =
      LoggerFactory.getLogger(CapacityScheduler.class);

  /** 队列管理器，负责队列的初始化、查询和管理 */
  private CapacitySchedulerQueueManager queueManager;

  /** 队列上下文，保存调度器上下文信息供队列使用 */
  private CapacitySchedulerQueueContext queueContext;

  /** 工作流优先级映射管理器，负责管理基于工作流的优先级映射规则 */
  private WorkflowPriorityMappingsManager workflowPriorityMappingsMgr;

  /** 抢占管理器，负责处理容器抢占逻辑 */
  private PreemptionManager preemptionManager = new PreemptionManager();

  /** 是否启用懒抢占，懒抢占会先标记容器可杀死，后续统一回收 */
  private volatile boolean isLazyPreemptionEnabled = false;

  /** 每次心跳允许分配的off-switch容器最大数量 */
  private int offswitchPerHeartbeatLimit;

  /** 是否启用一次心跳分配多个容器 */
  private boolean assignMultipleEnabled;

  /** 每次心跳最多分配容器数量 */
  private int maxAssignPerHeartbeat;

  /** 调度器配置提供者，负责加载和提供容量调度器配置 */
  private CSConfigurationProvider csConfProvider;

  /** 异步调度线程计数器，用于生成线程名 */
  private int threadNum = 0;

  /** 待处理应用比较器，按提交时间排序 */
  private final PendingApplicationComparator applicationComparator =
      new PendingApplicationComparator();

  @Override
  public void setConf(Configuration conf) {
      yarnConf = conf;
  }

  /**
   * 验证调度器配置合法性
   * @param conf 配置对象
   */
  private void validateConf(Configuration conf) {
    // 验证内存分配配置
    CapacitySchedulerConfigValidator.validateMemoryAllocation(conf);
    // 验证虚拟核分配配置
    CapacitySchedulerConfigValidator.validateVCores(conf);
  }

  @Override
  public Configuration getConf() {
    return yarnConf;
  }

  /** 容量调度器配置对象 */
  private CapacitySchedulerConfiguration conf;
  /** YARN基础配置对象 */
  private Configuration yarnConf;

  /** 资源计算器，用于资源比较和计算 */
  private ResourceCalculator calculator;
  /** 是否在节点名中使用端口 */
  private boolean usePortForNodeName;

  /** 异步调度配置 */
  private AsyncSchedulingConfiguration asyncSchedulingConf;
  /** 节点标签管理器 */
  private RMNodeLabelsManager labelManager;
  /** 应用优先级ACL管理器，控制用户修改优先级权限 */
  private AppPriorityACLsManager appPriorityACLManager;
  /** 是否启用多节点放置策略 */
  private boolean multiNodePlacementEnabled;

  /** 是否已打印过异步调度详细日志，用于控制日志冗余 */
  private boolean printedVerboseLoggingForAsyncScheduling;
  /** 应用遇到队列不存在问题是否快速失败 */
  private boolean appShouldFailFast;

  /** 最大运行应用数限制器，负责控制每个队列最大运行应用数量 */
  private CSMaxRunningAppsEnforcer maxRunningEnforcer;

  public CapacityScheduler() {
    super(CapacityScheduler.class.getName());
    this.maxRunningEnforcer = new CSMaxRunningAppsEnforcer(this);
  }

  @Override
  public QueueMetrics getRootQueueMetrics() {
    return getRootQueue().getMetrics();
  }

  public CSQueue getRootQueue() {
    return queueManager.getRootQueue();
  }

  @Override
  public CapacitySchedulerConfiguration getConfiguration() {
    return conf;
  }

  @Override
  public CapacitySchedulerQueueContext getQueueContext() {
    return queueContext;
  }

  @Override
  public RMContainerTokenSecretManager getContainerTokenSecretManager() {
    return this.rmContext.getContainerTokenSecretManager();
  }

  @Override
  public ResourceCalculator getResourceCalculator() {
    return calculator;
  }

  @VisibleForTesting
  public void setResourceCalculator(ResourceCalculator rc) {
    this.calculator = rc;
  }

  @Override
  public int getNumClusterNodes() {
    return nodeTracker.nodeCount();
  }

  @Override
  public RMContext getRMContext() {
    return this.rmContext;
  }

  @Override
  public void setRMContext(RMContext rmContext) {
    this.rmContext = rmContext;
  }

  /**
   * 初始化调度器核心组件和配置
   * @param configuration 配置对象
   * @throws IOException IO异常
   * @throws YarnException YARN异常
   */
  @VisibleForTesting
  void initScheduler(Configuration configuration) throws
      IOException, YarnException {
    writeLock.lock();
    try {
      this.csConfProvider = getCsConfProvider(configuration);
      this.csConfProvider.init(configuration);
      this.conf = this.csConfProvider.loadConfiguration(configuration);
      validateConf(this.conf);
      this.minimumAllocation = super.getMinimumAllocation();
      initMaximumResourceCapability(super.getMaximumAllocation());
      this.calculator = initResourceCalculator();
      this.usePortForNodeName = this.conf.getUsePortForNodeName();
      this.applications = new ConcurrentHashMap<>();
      this.labelManager = rmContext.getNodeLabelManager();
      this.appPriorityACLManager = new AppPriorityACLsManager(conf);
      this.queueManager = new CapacitySchedulerQueueManager(yarnConf,
          this.labelManager, this.appPriorityACLManager);
      this.queueManager.setCapacitySchedulerContext(this);
      this.workflowPriorityMappingsMgr = new WorkflowPriorityMappingsManager();
      this.activitiesManager = new ActivitiesManager(rmContext);
      activitiesManager.init(conf);
      this.queueContext = new CapacitySchedulerQueueContext(this);
      initializeQueues(this.conf);
      this.isLazyPreemptionEnabled = conf.getLazyPreemptionEnabled();
      this.assignMultipleEnabled = this.conf.getAssignMultipleEnabled();
      this.maxAssignPerHeartbeat = this.conf.getMaxAssignPerHeartbeat();
      this.appShouldFailFast = CapacitySchedulerConfiguration.shouldAppFailFast(getConfig());
      initAsyncSchedulingProperties();

      // 设置每轮调度可分配的off-switch容器数量限制
      offswitchPerHeartbeatLimit = this.conf.getOffSwitchPerHeartbeatLimit();

      initMultiNodePlacement();
      printSchedulerInitialized();
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * 根据配置获取对应的配置提供者实现
   * @param configuration 配置对象
   * @return 配置提供者实例
   * @throws IOException 非法配置类抛出IO异常
   */
  private CSConfigurationProvider getCsConfProvider(Configuration configuration)
      throws IOException {
    String confProviderStr = configuration.get(
        YarnConfiguration