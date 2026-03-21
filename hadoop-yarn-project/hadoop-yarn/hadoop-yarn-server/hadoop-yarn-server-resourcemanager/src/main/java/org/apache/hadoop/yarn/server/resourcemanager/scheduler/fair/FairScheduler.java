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

import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.util.Lists;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NMToken;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceOption;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.SchedulingRequest;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.SchedulerInvalidResourceRequestException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.SchedulerResourceTypes;
import org.apache.hadoop.yarn.security.AccessType;
import org.apache.hadoop.yarn.security.Permission;
import org.apache.hadoop.yarn.security.PrivilegedEntity;
import org.apache.hadoop.yarn.security.PrivilegedEntity.EntityType;
import org.apache.hadoop.yarn.security.YarnAuthorizationProvider;
import org.apache.hadoop.yarn.server.api.protocolrecords.NMContainerStatus;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.RMCriticalThreadUncaughtExceptionHandler;
import org.apache.hadoop.yarn.server.resourcemanager.placement.ApplicationPlacementContext;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.RMState;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationConstants;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ActiveUsersManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ContainerUpdates;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplication;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils.MaxResourceValidationResult;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.QueueEntitlement;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.ContainerExpiredSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.ContainerPreemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeResourceUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.ReleaseContainerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.DominantResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.apache.hadoop.yarn.util.resource.Resources;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.util.concurrent.SubjectInheritingThread;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.SettableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;

/**
 * 公平调度器实现，在多级队列之间进行资源公平分配。
 * 调度器跟踪每个队列的资源使用情况，通过优先调度偏离理想公平分配最远的队列，
 * 实现整体资源分配的公平性。
 * 
 * 公平调度器支持层级队列结构，所有队列都从名为"root"的根队列派生而来。
 * 可用资源按照公平调度方式在根队列的子队列之间分配，然后每个子队列再用同样的方式
 * 将分配到的资源分给自己的子队列。应用只能在叶子队列上调度。
 * 在配置文件中通过将队列作为父队列的子元素来指定层级关系。
 *
 * 队列名称以父队列名称为前缀，用点号分隔。例如根下的queue1会被表示为"root.queue1"，
 * parent1下的queue2会被表示为"root.parent1.queue2"。
 */
@LimitedPrivate("yarn")
@Unstable
@SuppressWarnings("unchecked")
public class FairScheduler extends
    AbstractYarnScheduler<FSAppAttempt, FSSchedulerNode> {
  /** 公平调度器配置对象 */
  private FairSchedulerConfiguration conf;

  /** 公平调度器上下文对象 */
  private FSContext context;
  /** YARN权限认证提供者 */
  private YarnAuthorizationProvider authorizer;
  /** 资源增量分配步长 */
  private Resource incrAllocation;
  /** 队列管理器 */
  private QueueManager queueMgr;
  /** 节点名称是否使用端口标识 */
  private boolean usePortForNodeName;

  private static final Logger LOG =
      LoggerFactory.getLogger(FairScheduler.class);
  private static final Logger STATE_DUMP_LOG =
      LoggerFactory.getLogger(FairScheduler.class.getName() + ".statedump");

  private static final ResourceCalculator RESOURCE_CALCULATOR =
      new DefaultResourceCalculator();
  private static final ResourceCalculator DOMINANT_RESOURCE_CALCULATOR =
      new DominantResourceCalculator();
  
  // 容器分配方法返回该值表示容器已被预留
  public static final Resource CONTAINER_RESERVED = Resources.createResource(-1);

  /** 调试日志输出间隔，每UPDATE_DEBUG_FREQUENCY次更新输出一次状态 */
  private final int UPDATE_DEBUG_FREQUENCY = 25;
  private int updatesToSkipForDebug = UPDATE_DEBUG_FREQUENCY;

  @Deprecated
  @VisibleForTesting
  Thread schedulingThread;

  /** 抢占线程 */
  Thread preemptionThread;

  // 聚合指标
  /** 根队列聚合指标 */
  FSQueueMetrics rootMetrics;
  /** 调度操作耗时统计 */
  FSOpDurations fsOpDurations;

  private float reservableNodesRatio; // 应用可预留资源的可用节点比例

  protected boolean sizeBasedWeight; // 是否根据作业大小调整权重，给大作业更高权重
  // 是否启用持续调度
  @Deprecated
  protected boolean continuousSchedulingEnabled;
  // 持续调度每轮休眠时间
  @Deprecated
  protected volatile int continuousSchedulingSleepMs;
  // 节点可用资源比较器
  private Comparator<FSSchedulerNode> nodeAvailableResourceComparator =
          new NodeAvailableResourceComparator();
  protected double nodeLocalityThreshold; // 节点局部性调度集群阈值
  protected double rackLocalityThreshold; // 机架局部性调度集群阈值
  @Deprecated
  protected long nodeLocalityDelayMs; // 节点局部性延迟等待时间
  @Deprecated
  protected long rackLocalityDelayMs; // 机架局部性延迟等待时间
  protected boolean assignMultiple; // 是否在一次心跳中分配多个容器

  @VisibleForTesting
  boolean maxAssignDynamic;
  protected int maxAssign; // 每次心跳最多分配容器数量

  @VisibleForTesting
  final MaxRunningAppsEnforcer maxRunningEnforcer;

  /** 分配配置文件加载服务 */
  private AllocationFileLoaderService allocsLoader;
  @VisibleForTesting
  volatile AllocationConfiguration allocConf;

  // 触发容器预留的容器大小阈值
  @VisibleForTesting
  Resource reservationThreshold;

  /** 是否迁移模式 */
  private boolean migration;
  /** 是否跳过终端规则检查 */
  private boolean noTerminalRuleCheck;

  /**
   * 构造公平调度器实例，初始化核心组件
   */
  public FairScheduler() {
    super(FairScheduler.class.getName());
    context = new FSContext(this);
    allocsLoader = new AllocationFileLoaderService(this);
    queueMgr = new QueueManager(this);
    maxRunningEnforcer = new MaxRunningAppsEnforcer(this);
  }

  public FSContext getContext() {
    return context;
  }

  public RMContext getRMContext() {
    return rmContext;
  }

  /**
   * 检查给定资源是否达到预留阈值
   */
  public boolean isAtLeastReservationThreshold(
      ResourceCalculator resourceCalculator, Resource resource) {
    return Resources.greaterThanOrEqual(resourceCalculator,
        getClusterResource(), resource, reservationThreshold);
  }

  /**
   * 验证调度器配置合法性，检查最小/最大/增量资源配置是否正确
   */
  private void validateConf(FairSchedulerConfiguration config) {
    // validate scheduler memory allocation setting
    int minMem =
        config.getInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
            YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB);
    int maxMem =
        config.getInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
            YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB);

    if (minMem < 0 || minMem > maxMem) {
      throw new YarnRuntimeException("Invalid resource scheduler memory"
        + " allocation configuration: "
        + YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB
        + "=" + minMem
        + ", " + YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB
        + "=" + maxMem + ".  Both values must be greater than or equal to 0"
        + "and the maximum allocation value must be greater than or equal to"
        + "the minimum allocation value.");
    }

    long incrementMem = config.getIncrementAllocation().getMemorySize();
    if (incrementMem <= 0) {
      throw new YarnRuntimeException("Invalid resource scheduler memory"
          + " allocation configuration: "
          + FairSchedulerConfiguration.RM_SCHEDULER_INCREMENT_ALLOCATION_MB
          + "=" + incrementMem + ". Values must be greater than 0.");
    }

    // validate scheduler vcores allocation setting
    int minVcores =
        config.getInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES,
            YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES);
    int maxVcores =
        config.getInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES,
            YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES);

    if (minVcores < 0 || minVcores > maxVcores) {
      throw new YarnRuntimeException("Invalid resource scheduler vcores"
        + " allocation configuration: "
        + YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES
        + "=" + minVcores
        + ", " + YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES
        + "=" + maxVcores + ".  Both values must be greater than or equal to 0"
          + "and the maximum allocation value must be greater than or equal to"
          + "the minimum allocation value.");
    }

    int incrementVcore = config.getIncrementAllocation().getVirtualCores();
    if (incrementVcore <= 0) {
      throw new YarnRuntimeException("Invalid resource scheduler vcores"
          + " allocation configuration: "
          + FairSchedulerConfiguration.RM_SCHEDULER_INCREMENT_ALLOCATION_VCORES
          + "=" + incrementVcore + ". Values must be greater than 0.");
    }
  }

  public FairSchedulerConfiguration getConf() {
    return conf;
  }

  public int getNumNodesInRack(String rackName) {
    return nodeTracker.nodeCount(rackName);
  }

  public QueueManager getQueueManager() {
    return queueMgr;
  }

  /**
   * 已废弃的持续调度线程，异步于节点心跳持续尝试调度
   */
  @Deprecated
  private class ContinuousSchedulingThread extends SubjectInheritingThread {

    @Override
    public void work() {
      while (!Thread.currentThread().isInterrupted()) {
        try {
          continuousSchedulingAttempt();
          Thread.sleep(getContinuousSchedulingSleepMs());
        } catch (InterruptedException e) {
          LOG.warn("Continuous scheduling thread interrupted. Exiting.", e);
          return;
        }
      }
    }
  }

  /**
   * 输出调度器完整状态到调试日志，包含集群容量、分配情况、队列状态
   */
  private void dumpSchedulerState() {
    FSQueue rootQueue = queueMgr.getRootQueue();
    Resource clusterResource = getClusterResource();
    STATE_DUMP_LOG.debug(
        "FairScheduler state: Cluster Capacity: " + clusterResource +
        "  Allocations: " + rootMetrics.getAllocatedResources() +
        "  Availability: " + Resource.newInstance(
            rootMetrics.getAvailableMB(),
            rootMetrics.getAvailableVirtualCores()) +
        "  Demand: " + rootQueue.getDemand());

    STATE_DUMP_LOG.debug(rootQueue.dumpState());
  }

  /**
   * 重新计算调度器内部状态：更新每个作业的权重、公平份额、资源缺口、最小分配，
   * 以及每个作业的已用资源和需求资源。该方法定期被更新线程调用。
   */
  @VisibleForTesting
  @Override
  public void update() {
    // 记录更新开始时间用于耗时统计
    long start = getClock().getTime();
    FSQueue rootQueue = queueMgr.getRootQueue();

    // 更新需求和公平份额
    writeLock.lock();
    try {
      // 递归更新所有队列的资源需求
      rootQueue.updateDemand();
      rootQueue.update(getClusterResource());

      // 更新根队列指标
      updateRootQueueMetrics();
    } finally {
      writeLock.unlock();
    }

    readLock.lock();
    try {
      // 更新饥饿统计，识别饥饿应用
      if (shouldAttemptPreemption()) {
        for (FSLeafQueue queue : queueMgr.getLeafQueues()) {
          queue.updateStarvedApps();
        }
      }

      // 按间隔输出调试日志
      if (STATE_DUMP_LOG.isDebugEnabled()) {
        if (--