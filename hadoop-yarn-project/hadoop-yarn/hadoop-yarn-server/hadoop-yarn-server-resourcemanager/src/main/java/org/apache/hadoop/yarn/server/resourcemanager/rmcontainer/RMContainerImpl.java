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

package org.apache.hadoop.yarn.server.resourcemanager.rmcontainer;

import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.hadoop.classification.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.ContainerUpdateType;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.api.protocolrecords.NMContainerStatus;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppRunningOnNodeEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptContainerFinishedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeCleanContainerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeUpdateContainerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.ContainerRequest;
import org.apache.hadoop.yarn.server.scheduler.SchedulerRequestKey;
import org.apache.hadoop.yarn.state.InvalidStateTransitionException;
import org.apache.hadoop.yarn.state.MultipleArcTransition;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;

/**
 * ResourceManager端容器实现，维护容器生命周期状态，处理容器相关事件流转
 * 代表YARN集群中一个已分配给应用尝试的容器，基于状态机实现容器生命周期管理
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class RMContainerImpl implements RMContainer {

  private static final Logger LOG =
      LoggerFactory.getLogger(RMContainerImpl.class);

  /**
   * 容器状态机工厂，定义所有容器状态和事件的转换规则
   */
  private static final StateMachineFactory<RMContainerImpl, RMContainerState, 
                                           RMContainerEventType, RMContainerEvent> 
   stateMachineFactory = new StateMachineFactory<RMContainerImpl, 
       RMContainerState, RMContainerEventType, RMContainerEvent>(
      RMContainerState.NEW)

    // Transitions from NEW state
    .addTransition(RMContainerState.NEW, RMContainerState.ALLOCATED,
        RMContainerEventType.START, new ContainerStartedTransition())
    .addTransition(RMContainerState.NEW, RMContainerState.KILLED,
        RMContainerEventType.KILL)
    .addTransition(RMContainerState.NEW, RMContainerState.RESERVED,
        RMContainerEventType.RESERVED, new ContainerReservedTransition())
    .addTransition(RMContainerState.NEW, RMContainerState.ACQUIRED,
        RMContainerEventType.ACQUIRED, new AcquiredTransition())
    .addTransition(RMContainerState.NEW,
        EnumSet.of(RMContainerState.RUNNING, RMContainerState.COMPLETED),
        RMContainerEventType.RECOVER, new ContainerRecoveredTransition())

    // Transitions from RESERVED state
    .addTransition(RMContainerState.RESERVED, RMContainerState.RESERVED,
        RMContainerEventType.RESERVED, new ContainerReservedTransition())
    .addTransition(RMContainerState.RESERVED, RMContainerState.ALLOCATED,
        RMContainerEventType.START, new ContainerStartedTransition())
    .addTransition(RMContainerState.RESERVED, RMContainerState.KILLED,
        RMContainerEventType.KILL) // nothing to do
    .addTransition(RMContainerState.RESERVED, RMContainerState.RELEASED,
        RMContainerEventType.RELEASED) // nothing to do
       

    // Transitions from ALLOCATED state
    .addTransition(RMContainerState.ALLOCATED, RMContainerState.ACQUIRED,
        RMContainerEventType.ACQUIRED, new AcquiredTransition())
    .addTransition(RMContainerState.ALLOCATED, RMContainerState.EXPIRED,
        RMContainerEventType.EXPIRE, new FinishedTransition())
    .addTransition(RMContainerState.ALLOCATED, RMContainerState.KILLED,
        RMContainerEventType.KILL, new FinishedTransition())

    // Transitions from ACQUIRED state
    .addTransition(RMContainerState.ACQUIRED, RMContainerState.RUNNING,
        RMContainerEventType.LAUNCHED)
    .addTransition(RMContainerState.ACQUIRED, RMContainerState.ACQUIRED,
        RMContainerEventType.ACQUIRED)
    .addTransition(RMContainerState.ACQUIRED, RMContainerState.COMPLETED,
        RMContainerEventType.FINISHED, new FinishedTransition())
    .addTransition(RMContainerState.ACQUIRED, RMContainerState.RELEASED,
        RMContainerEventType.RELEASED, new KillTransition())
    .addTransition(RMContainerState.ACQUIRED, RMContainerState.EXPIRED,
        RMContainerEventType.EXPIRE, new KillTransition())
    .addTransition(RMContainerState.ACQUIRED, RMContainerState.KILLED,
        RMContainerEventType.KILL, new KillTransition())

    // Transitions from RUNNING state
    .addTransition(RMContainerState.RUNNING, RMContainerState.COMPLETED,
        RMContainerEventType.FINISHED, new FinishedTransition())
    .addTransition(RMContainerState.RUNNING, RMContainerState.KILLED,
        RMContainerEventType.KILL, new KillTransition())
    .addTransition(RMContainerState.RUNNING, RMContainerState.RELEASED,
        RMContainerEventType.RELEASED, new KillTransition())
    .addTransition(RMContainerState.RUNNING, RMContainerState.RUNNING,
        RMContainerEventType.ACQUIRED)
    .addTransition(RMContainerState.RUNNING, RMContainerState.RUNNING,
        RMContainerEventType.RESERVED, new ContainerReservedTransition())
    .addTransition(RMContainerState.RUNNING, RMContainerState.RUNNING,
        RMContainerEventType.ACQUIRE_UPDATED_CONTAINER, 
        new ContainerAcquiredWhileRunningTransition())
    .addTransition(RMContainerState.RUNNING, RMContainerState.RUNNING,
        RMContainerEventType.NM_DONE_CHANGE_RESOURCE, 
        new NMReportedContainerChangeIsDoneTransition())

    // Transitions from COMPLETED state
    .addTransition(RMContainerState.COMPLETED, RMContainerState.COMPLETED,
        EnumSet.of(RMContainerEventType.EXPIRE, RMContainerEventType.RELEASED,
            RMContainerEventType.KILL))

    // Transitions from EXPIRED state
    .addTransition(RMContainerState.EXPIRED, RMContainerState.EXPIRED,
        EnumSet.of(RMContainerEventType.RELEASED, RMContainerEventType.KILL))

    // Transitions from RELEASED state
    .addTransition(RMContainerState.RELEASED, RMContainerState.RELEASED,
        EnumSet.of(RMContainerEventType.EXPIRE, RMContainerEventType.RELEASED,
            RMContainerEventType.KILL, RMContainerEventType.FINISHED))

    // Transitions from KILLED state
    .addTransition(RMContainerState.KILLED, RMContainerState.KILLED,
        EnumSet.of(RMContainerEventType.EXPIRE, RMContainerEventType.RELEASED,
            RMContainerEventType.KILL, RMContainerEventType.ACQUIRED,
            RMContainerEventType.FINISHED))

    // create the topology tables
    .installTopology();

  /** 容器当前状态机实例 */
  private final StateMachine<RMContainerState, RMContainerEventType,
                                                 RMContainerEvent> stateMachine;
  /** 读锁，用于保护状态查询 */
  private final ReadLock readLock;
  /** 写锁，用于保护状态变更 */
  private final WriteLock writeLock;
  /** 所属应用尝试ID */
  private final ApplicationAttemptId appAttemptId;
  /** 分配节点ID */
  private final NodeId nodeId;
  /** RM上下文，获取全局资源和服务 */
  private final RMContext rmContext;
  /** 事件处理器，用于发送事件到其他组件 */
  private final EventHandler eventHandler;
  /** 容器分配过期管理器 */
  private final ContainerAllocationExpirer containerAllocationExpirer;
  /** 容器对应用户 */
  private final String user;
  /** 节点标签表达式 */
  private final String nodeLabelExpression;

  /** 当前容器信息 */
  private volatile Container container;
  /** 预留资源 */
  private Resource reservedResource;
  /** 预留节点 */
  private NodeId reservedNode;
  /** 预留调度请求key */
  private SchedulerRequestKey reservedSchedulerKey;
  /** 创建时间 */
  private long creationTime;
  /** 完成时间 */
  private long finishTime;
  /** 完成状态信息 */
  private ContainerStatus finishedStatus;
  /** 是否是ApplicationMaster容器 */
  private boolean isAMContainer;
  /** 恢复用容器请求 */
  private ContainerRequest containerRequestForRecovery;

  /** 回滚用的最后确认资源，用于容器资源动态调整 */
  private Resource lastConfirmedResource;
  /** 所属队列名称 */
  private volatile String queueName;

  /** 是否是外部分配容器 */
  private boolean isExternallyAllocated;
  /** 分配调度请求key */
  private SchedulerRequestKey allocatedSchedulerKey;

  /** 分配标签集合 */
  private volatile Set<String> allocationTags = null;

  public RMContainerImpl(Container container, SchedulerRequestKey schedulerKey,
      ApplicationAttemptId appAttemptId, NodeId nodeId, String user,
      RMContext rmContext) {
    this(container, schedulerKey, appAttemptId, nodeId, user, rmContext, System
        .currentTimeMillis(), "");
  }

  public RMContainerImpl(Container container, SchedulerRequestKey schedulerKey,
      ApplicationAttemptId appAttemptId, NodeId nodeId, String user,
      RMContext rmContext, boolean isExternallyAllocated) {
    this(container, schedulerKey, appAttemptId, nodeId, user, rmContext, System
        .currentTimeMillis(), "", isExternallyAllocated);
  }

  private boolean saveNonAMContainerMetaInfo;

  public RMContainerImpl(Container container, SchedulerRequestKey schedulerKey,
      ApplicationAttemptId appAttemptId, NodeId nodeId, String user,
      RMContext rmContext, String nodeLabelExpression) {
    this(container, schedulerKey, appAttemptId, nodeId, user, rmContext, System
      .currentTimeMillis(), nodeLabelExpression);
  }

  public RMContainerImpl(Container container, SchedulerRequestKey schedulerKey,
      ApplicationAttemptId appAttemptId, NodeId nodeId, String user,
      RMContext rmContext, long creationTime, String nodeLabelExpression) {
    this(container, schedulerKey, appAttemptId, nodeId, user, rmContext,
        creationTime, nodeLabelExpression, false);
  }

  /**
   * 完整构造方法，初始化容器状态与依赖
   */
  public RMContainerImpl(Container container, SchedulerRequestKey schedulerKey,
      ApplicationAttemptId appAttemptId, NodeId nodeId, String user,
      RMContext rmContext, long creationTime, String nodeLabelExpression,
      boolean isExternallyAllocated) {
    this.stateMachine = stateMachineFactory.make(this);
    this.nodeId = nodeId;
    this.container = container;
    this.allocatedSchedulerKey = schedulerKey;
    this.appAttemptId = appAttemptId;
    this.user = user;
    this.creationTime = creationTime;
    this.rmContext = rmContext;
    this.eventHandler = rmContext.getDispatcher().getEventHandler();
    this.containerAllocationExpirer = rmContext.getContainerAllocationExpirer();
    this.isAMContainer = false;
    this.nodeLabelExpression = nodeLabelExpression;
    this.lastConfirmedResource = container.getResource();
    this.isExternallyAllocated = isExternallyAllocated;

    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    this.readLock = lock.readLock();
    this.writeLock = lock.writeLock();

    saveNonAMContainerMetaInfo =
        shouldPublishNonAMContainerEventstoATS(rmContext);

    if (container.getId() != null) {
      // 向应用历史写入器记录容器启动事件
      rmContext.getRMApplicationHistoryWriter().containerStarted(this);
    }

    if (this.container != null) {
      this.allocationTags = this.container.getAllocationTags();
    }
  }

  @Override
  public ContainerId getContainerId() {
    return this.container.getId();
  }

  @Override
  public ApplicationAttemptId getApplicationAttemptId() {
    return this.appAttemptId;
  }

  @Override
  public Container getContainer() {
    return this.container;
  }

  public void setContainer(Container container) {
    this.container = container;
  }

  @Override
  public RMContainerState getState() {
    this.readLock.lock();
    try {
      return this.stateMachine.getCurrentState();
    } finally {
      this.readLock.unlock();
    }
  }

  @Override
  public Resource getReservedResource() {
    return reservedResource;
  }

  @Override
  public NodeId getReservedNode() {
    return reservedNode;
  }

  @Override
  public SchedulerRequestKey getReservedSchedulerKey() {
    return reservedSchedulerKey;
  }

  @Override
  public Resource getAllocatedResource() {
    readLock.lock();
    try {
      return container.getResource();
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public Resource getLastConfirmedResource() {
    readLock.lock();
    try {
      return this.lastConfirmedResource;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public NodeId getAllocatedNode() {
    return container.getNodeId();
  }

  @Override
  public SchedulerRequestKey getAllocatedSchedulerKey() {
    return allocatedSchedulerKey;
  }

  @Override
  public Priority getAllocatedPriority() {
    return container.getPriority();
  }

  @Override
  public long getCreationTime() {
    return creationTime;
  }

  @Override
  public long getFinishTime() {
    readLock.lock();
    try {
      return finishTime;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public String getDiagnosticsInfo() {
    readLock.lock();
    try {
      if (finishedStatus != null) {
        return finishedStatus.getDiagnostics();
      } else {
        return null;
      }
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public String getLogURL() {
    readLock.lock();
    try {
      // 构造容器日志访问URL
      StringBuilder logURL = new StringBuilder();
      logURL.append(WebAppUtils.getHttpSchemePrefix(rmContext
          .getYarnConfiguration()));
      logURL.append(WebAppUtils.getRunningLogURL(
          container.getNodeHttpAddress(), getContainerId().toString(),
          user));
      return logURL.toString();
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public int getContainerExitStatus() {
    readLock.lock();
    try {
      if (finishedStatus != null) {
        return finishedStatus.getExitStatus();
      } else {
        return 0;
      }
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public ContainerState getContainerState() {