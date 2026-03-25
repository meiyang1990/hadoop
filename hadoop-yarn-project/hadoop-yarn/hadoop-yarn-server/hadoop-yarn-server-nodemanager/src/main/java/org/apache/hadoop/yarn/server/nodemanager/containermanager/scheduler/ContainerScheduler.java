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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.scheduler;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceUtilization;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.api.records.ContainerQueuingLimit;
import org.apache.hadoop.yarn.server.api.records.OpportunisticContainersStatus;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;

import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandlerChain;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandlerModule;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor
    .ChangeMonitoringContainerResourceEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerState;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainersMonitor;


import org.apache.hadoop.yarn.server.nodemanager.metrics.NodeManagerMetrics;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService
        .RecoveredContainerState;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService.RecoveredContainerStatus;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * NodeManager本地容器调度器，管理等待运行的容器集合，确保容器仅在满足启动条件时启动，
 * 并会主动抢占/杀死机会容器来保障保证容器的资源需求。
 */
public class ContainerScheduler extends AbstractService implements
    EventHandler<ContainerSchedulerEvent> {

  private static final Logger LOG =
      LoggerFactory.getLogger(ContainerScheduler.class);

  private final Context context;
  // 机会容器等待队列最大长度
  private final int maxOppQueueLength;
  private final boolean forceStartGuaranteedContainers;

  // 等待资源的保证容器队列
  private final LinkedHashMap<ContainerId, Container>
      queuedGuaranteedContainers = new LinkedHashMap<>();
  // 等待资源的机会容器队列
  private final LinkedHashMap<ContainerId, Container>
      queuedOpportunisticContainers = new LinkedHashMap<>();

  // 记录为了给保证容器腾空间而被标记为杀死或暂停的机会容器
  private final Map<ContainerId, Container> oppContainersToKill =
      new HashMap<>();

  // 容器启动后需要一段时间才会进入RUNNING状态，该集合保存已经调度但尚未真正运行的容器
  // 包含已经RUNNING和已标记调度但未RUNNING的容器，方便抢占时识别
  private final LinkedHashMap<ContainerId, Container> runningContainers =
      new LinkedHashMap<>();

  private final ContainerQueuingLimit queuingLimit =
      ContainerQueuingLimit.newInstance();

  private final OpportunisticContainersStatus opportunisticContainersStatus;

  // 资源利用率追踪器，根据容器启动/完成更新节点资源使用情况
  private ResourceUtilizationTracker utilizationTracker;

  private final AsyncDispatcher dispatcher;
  private final NodeManagerMetrics metrics;
  private final OpportunisticContainersQueuePolicy oppContainersQueuePolicy;

  private Boolean usePauseEventForPreemption = false;

  /**
   * 从配置中读取机会容器队列最大长度配置，无配置则返回默认值。
   * @param context NodeManager上下文
   * @return 机会容器队列最大长度
   */
  private static int getMaxOppQueueLengthFromConf(final Context context) {
    if (context == null || context.getConf() == null) {
      return YarnConfiguration
          .DEFAULT_NM_OPPORTUNISTIC_CONTAINERS_MAX_QUEUE_LENGTH;
    }

    return context.getConf().getInt(
        YarnConfiguration.NM_OPPORTUNISTIC_CONTAINERS_MAX_QUEUE_LENGTH,
        YarnConfiguration.DEFAULT_NM_OPPORTUNISTIC_CONTAINERS_MAX_QUEUE_LENGTH
    );
  }

  /**
   * 从配置中读取机会容器队列排队策略，无配置则返回默认值。
   * @param context NodeManager上下文
   * @return 机会容器排队策略
   */
  private static OpportunisticContainersQueuePolicy
      getOppContainersQueuePolicyFromConf(final Context context) {
    final OpportunisticContainersQueuePolicy queuePolicy;
    if (context == null || context.getConf() == null) {
      queuePolicy = OpportunisticContainersQueuePolicy.DEFAULT;
    } else {
      queuePolicy = context.getConf().getEnum(
          YarnConfiguration.NM_OPPORTUNISTIC_CONTAINERS_QUEUE_POLICY,
          OpportunisticContainersQueuePolicy.DEFAULT
      );
    }

    return queuePolicy;
  }

  @VisibleForTesting
  ResourceHandlerChain resourceHandlerChain = null;

  /**
   * 构造容器调度器。
   * @param context NodeManager上下文
   * @param dispatcher 异步事件分发器
   * @param metrics NodeManager指标统计
   */
  public ContainerScheduler(Context context, AsyncDispatcher dispatcher,
      NodeManagerMetrics metrics) {
    this(context, dispatcher, metrics,
        getOppContainersQueuePolicyFromConf(context),
        getMaxOppQueueLengthFromConf(context));
  }


  @Override
  public void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    // 初始化资源处理器链
    if (resourceHandlerChain == null) {
      resourceHandlerChain = ResourceHandlerModule
          .getConfiguredResourceHandlerChain(conf, context);
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Resource handler chain enabled = " + (resourceHandlerChain
          != null));

    }
    // 读取抢占是否使用暂停事件的配置
    this.usePauseEventForPreemption =
        conf.getBoolean(
            YarnConfiguration.NM_CONTAINER_QUEUING_USE_PAUSE_FOR_PREEMPTION,
            YarnConfiguration.
                DEFAULT_NM_CONTAINER_QUEUING_USE_PAUSE_FOR_PREEMPTION);
  }

  @VisibleForTesting
  public ContainerScheduler(Context context, AsyncDispatcher dispatcher,
      NodeManagerMetrics metrics, int qLength) {
    this(context, dispatcher, metrics,
        getOppContainersQueuePolicyFromConf(context), qLength);
  }

  @VisibleForTesting
  public ContainerScheduler(Context context, AsyncDispatcher dispatcher,
      NodeManagerMetrics metrics,
      OpportunisticContainersQueuePolicy oppContainersQueuePolicy,
      int qLength) {
    super(ContainerScheduler.class.getName());
    this.context = context;
    this.dispatcher = dispatcher;
    this.metrics = metrics;
    // 初始化基于分配的资源利用率追踪器
    this.utilizationTracker =
        new AllocationBasedResourceUtilizationTracker(this);
    this.oppContainersQueuePolicy = oppContainersQueuePolicy;
    // 根据排队策略初始化队列参数
    switch (oppContainersQueuePolicy) {
    case BY_RESOURCES:
      this.maxOppQueueLength = 0;
      this.forceStartGuaranteedContainers = false;
      LOG.info("Setting max opportunistic queue length to 0,"
              + " as {} is incompatible with queue length",
          oppContainersQueuePolicy);
      break;
    case BY_QUEUE_LEN:
    default:
      this.maxOppQueueLength = qLength;
      this.forceStartGuaranteedContainers = (maxOppQueueLength <= 0);
    }
    this.opportunisticContainersStatus =
        OpportunisticContainersStatus.newInstance();
  }

  /**
   * 处理容器调度相关事件。
   * @param event 容器调度事件
   */
  @Override
  public void handle(ContainerSchedulerEvent event) {
    switch (event.getType()) {
    case SCHEDULE_CONTAINER:
      // 调度新容器入队
      scheduleContainer(event.getContainer());
      break;
    // 容器已暂停，等待资源释放后重新调度
    case CONTAINER_PAUSED:
    // 容器已完成，回收资源并重新调度等待容器
    case CONTAINER_COMPLETED:
      onResourcesReclaimed(event.getContainer());
      break;
    case UPDATE_CONTAINER:
      // 处理容器更新事件（资源或执行类型变更）
      if (event instanceof UpdateContainerSchedulerEvent) {
        onUpdateContainer((UpdateContainerSchedulerEvent) event);
      } else {
        LOG.error("Unknown event type on UpdateCOntainer: " + event.getType());
      }
      break;
    case SHED_QUEUED_CONTAINERS:
      // 清理超出排队限制的机会容器
      shedQueuedOpportunisticContainers();
      break;
    case RECOVERY_COMPLETED:
      // 恢复完成后启动所有等待容器，更新指标
      startPendingContainers(forceStartGuaranteedContainers);
      metrics.setQueuedContainers(queuedOpportunisticContainers.size(),
          queuedGuaranteedContainers.size());
      break;
    default:
      LOG.error("Unknown event arrived at ContainerScheduler: "
          + event.toString());
    }
  }

  /**
   * 处理容器更新事件，支持资源变更和执行类型变更。
   */
  private void onUpdateContainer(UpdateContainerSchedulerEvent updateEvent) {
    ContainerId containerId = updateEvent.getContainer().getContainerId();
    // 处理资源变更
    if (updateEvent.isResourceChange()) {
      if (runningContainers.containsKey(containerId)) {
        // 扣除旧资源，添加新资源到利用率统计
        this.utilizationTracker.subtractContainerResource(
            new ContainerImpl(getConfig(), null, null, null, null,
                updateEvent.getOriginalToken(), context));
        this.utilizationTracker.addContainerResources(
            updateEvent.getContainer());
        // 更新监控指标
        getContainersMonitor().handle(
            new ChangeMonitoringContainerResourceEvent(containerId,
                updateEvent.getUpdatedToken().getResource()));
      }
    }

    // 处理执行类型变更（升级/降级）
    if (updateEvent.isExecTypeUpdate()) {
      // 升级（机会容器->保证容器）或资源增加
      if (updateEvent.isIncrease()) {
        // 从机会队列移除，加入保证队列
        if (queuedOpportunisticContainers.remove(containerId) != null) {
          queuedGuaranteedContainers.put(containerId,
              updateEvent.getContainer());
          // 抢占机会容器资源来满足升级后的保证容器需求
          reclaimOpportunisticContainerResources(updateEvent.getContainer());
        }
      } else {
        // 降级（保证容器->机会容器）
        if (queuedGuaranteedContainers.remove(containerId) != null) {
          queuedOpportunisticContainers.put(containerId,
              updateEvent.getContainer());
        }
      }
      // 更新资源处理器中的容器信息
      try {
        resourceHandlerChain.updateContainer(updateEvent.getContainer());
      } catch (Exception ex) {
        LOG.warn(String.format("Could not update resources on " +
            "continer update of %s", containerId), ex);
      }
      // 重新尝试启动等待容器
      startPendingContainers(forceStartGuaranteedContainers);
      // 更新排队指标
      metrics.setQueuedContainers(queuedOpportunisticContainers.size(),
          queuedGuaranteedContainers.size());
    }
  }

  /**
   * 恢复过程中，将已恢复的容器信息加入调度器数据结构。
   * @param container 恢复的容器
   * @param rcs 恢复的容器状态
   */
  public void recoverActiveContainer(Container container,
      RecoveredContainerState rcs) {
    ExecutionType execType =
        container.getContainerTokenIdentifier().getExecutionType();
    if (rcs.getStatus() == RecoveredContainerStatus.QUEUED
        || rcs.getStatus() == RecoveredContainerStatus.PAUSED) {
      // 根据执行类型放入对应等待队列
      if (execType == ExecutionType.GUARANTEED) {
        queuedGuaranteedContainers.put(container.getContainerId(), container);
      } else if (execType == ExecutionType.OPPORTUNISTIC) {
        queuedOpportunisticContainers
            .put(container.getContainerId(), container);
      } else {
        LOG.error(
            "UnKnown execution type received " + container.getContainerId()
                + ", execType " + execType);
      }
      metrics.setQueuedContainers(queuedOpportunisticContainers.size(),
          queuedGuaranteedContainers.size());
    } else if (rcs.getStatus() == RecoveredContainerStatus.LAUNCHED) {
      // 已启动容器放入运行集合，添加资源统计
      runningContainers.put(container.getContainerId(), container);
      utilizationTracker.addContainerResources(container);
    }
    // 更新指标统计
    if (rcs.getStatus() != RecoveredContainerStatus.COMPLETED
            && rcs.getCapability() != null) {
      metrics.launchedContainer();
      metrics.allocateContainer(rcs.getCapability());
    }
  }

  /**
   * 获取当前所有排队容器总数。
   * @return 排队容器总数
   */
  public int getNumQueuedContainers() {
    return this.queuedGuaranteedContainers.size()
        + this.queuedOpportunisticContainers.size();
  }

  /**
   * 获取当前节点机会容器队列容量。
   * @return 机会容器队列容量
   */
  public int getOpportunisticQueueCapacity() {
    return this.maxOppQueueLength;
  }

  @VisibleForTesting
  public int getNumQueuedGuaranteedContainers() {
    return this.queuedGuaranteedContainers.size();
  }

  @VisibleForTesting
  public int getNumQueuedOpportunisticContainers() {
    return this.queuedOpportunisticContainers.size();
  }

  @VisibleForTesting
  public int getNumRunningContainers() {
    return this.runningContainers.size();
  }

  @VisibleForTesting
  public void setUsePauseEventForPreemption(
      boolean usePauseEventForPreemption) {
    this.usePauseEventForPreemption = usePauseEventForPreemption;
  }

  /**
   * 获取机会容器状态信息，包含排队、运行、资源使用等统计。
   * @return 机会容器状态
   */
  public OpportunisticContainersStatus getOpportunisticContainersStatus() {
    this.opportunisticContainersStatus.setQueuedOpportContainers(
        getNumQueuedOpportunisticContainers());
    this.opportunisticContainersStatus.setWaitQueueLength(
        getNumQueuedContainers());
    this.opportunisticContainersStatus.setOpportMemoryUsed(
        metrics.getAllocatedOpportunisticGB());
    this.opportunisticContainersStatus.setOpportCoresUsed(
        metrics.getAllocatedOpportunisticVCores());
    this.opportunisticContainersStatus.setRunningOpportContainers(
        metrics.getRunningOpportunisticContainers());
    this.opportunisticContainersStatus.setOpportQueueCapacity(
        getOpportunisticQueueCapacity());
    return this.opportunisticContainersStatus;
  }

  /**
   * 容器完成或暂停后，回收资源并重新调度等待容器。
   * @param container 已完成/暂停的容器
   */
  private void onResourcesReclaimed(Container container) {
    // 从待杀死集合移除
    oppContainersToKill.remove(container.getContainerId());

    // 从排队队列移除（容器可能在排队时被外部杀死）
    Container queued =
        queuedOpportunisticContainers.remove(container.getContainerId());
    if (queued == null) {
      queuedGuaranteedContainers.remove(container.getContainerId());
    }

    // 将暂停容器重新放回对应排队队列
    if (container.getContainerState() == ContainerState.PAUSED) {
      if (container.getContainerTokenIdentifier