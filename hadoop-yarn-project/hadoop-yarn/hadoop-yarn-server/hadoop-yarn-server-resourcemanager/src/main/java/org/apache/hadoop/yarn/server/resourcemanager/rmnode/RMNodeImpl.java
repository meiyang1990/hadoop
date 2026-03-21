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

package org.apache.hadoop.yarn.server.resourcemanager.rmnode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.commons.collections4.keyvalue.DefaultMapEntry;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.api.protocolrecords.SignalContainerRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.ContainerUpdateType;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.NodeAttribute;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceOption;
import org.apache.hadoop.yarn.api.records.ResourceUtilization;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.nodelabels.AttributeValue;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.nodelabels.NodeAttributesManager;
import org.apache.hadoop.yarn.server.api.protocolrecords.LogAggregationReport;
import org.apache.hadoop.yarn.server.api.protocolrecords.NMContainerStatus;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.records.OpportunisticContainersStatus;
import org.apache.hadoop.yarn.server.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.server.resourcemanager.ClusterMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.NodesListManagerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.NodesListManagerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.NodesListManager;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppRunningOnNodeEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.ContainerAllocationExpirer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.AllocationExpirationInfo;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeResourceUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.utils.BuilderUtils.ContainerIdComparator;
import org.apache.hadoop.yarn.state.InvalidStateTransitionException;
import org.apache.hadoop.yarn.state.MultipleArcTransition;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;
import org.apache.hadoop.yarn.util.resource.Resources;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * YARN ResourceManager中维护NM节点状态和其上运行的应用/容器信息的实现类，
 * 跟踪记录节点生命周期、容器运行情况和资源使用状况。
 */
@Private
@Unstable
@SuppressWarnings("unchecked")
public class RMNodeImpl implements RMNode, EventHandler<RMNodeEvent> {

  private static final Logger LOG =
      LoggerFactory.getLogger(RMNodeImpl.class);

  private static final RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(null);

  private final ReadLock readLock;
  private final WriteLock writeLock;

  // 等待通知调度器的容器更新队列
  private final ConcurrentLinkedQueue<UpdatedContainerInfo> nodeUpdateQueue;
  // 标记是否需要在下次心跳后触发调度器更新
  private volatile boolean nextHeartBeat = true;

  private final NodeId nodeId;
  private final RMContext context;
  private final String hostName;
  private final int commandPort;
  private int httpPort;
  private final String nodeAddress; // ContainerManager服务地址
  private String httpAddress;
  /* 接收下架命令前的节点总资源快照 */
  private volatile Resource originalTotalCapability;
  // 当前节点总可用资源
  private volatile Resource totalCapability;
  // 已分配容器占用的总资源
  private volatile Resource allocatedContainerResource =
      Resource.newInstance(Resources.none());
  // 标记节点资源是否已更新
  private volatile boolean updatedCapability = false;
  private final Node node;

  private String healthReport;
  private long lastHealthReportTime;
  private String nodeManagerVersion;
  // 下架超时时间
  private Integer decommissioningTimeout;

  private long timeStamp;
  /* 节点上所有容器的聚合资源使用率 */
  private ResourceUtilization containersUtilization;
  /* 节点整体资源使用率 */
  private ResourceUtilization nodeUtilization;

  /* 跟踪最近一次资源使用率指标增量 */
  private Resource lastUtilIncr = Resources.none();

  /** 节点物理总资源 */
  private volatile Resource physicalResource;

  /* 节点上机会容器状态，用于分布式调度 */
  private OpportunisticContainersStatus opportunisticContainersStatus;

  private final ContainerAllocationExpirer containerAllocationExpirer;
  /* 刚刚启动完成的容器集合 */
  private final Set<ContainerId> launchedContainers =
    new HashSet<ContainerId>();

  /* 全局跟踪已完成容器 */
  private final Set<ContainerId> completedContainers =
      new HashSet<ContainerId>();

  /* 需要清理的容器集合 */
  private final Set<ContainerId> containersToClean = new TreeSet<ContainerId>(
      new ContainerIdComparator());

  /* 需要发送信号的容器集合 */
  private final List<SignalContainerRequest> containersToSignal =
      new ArrayList<SignalContainerRequest>();

  /*
   * 需要通知NM从上下文中移除的容器集合，目前包含那些AM已经收到完成通知的容器
   */
  private final Set<ContainerId> containersToBeRemovedFromNM =
      new HashSet<ContainerId>();

  /* 已经完成需要清理的应用列表 */
  private final List<ApplicationId> finishedApplications =
      new ArrayList<ApplicationId>();

  /* 当前节点上运行的应用列表 */
  private final List<ApplicationId> runningApplications =
      new ArrayList<ApplicationId>();
  
  // 等待更新资源的容器集合
  private final Map<ContainerId, Container> toBeUpdatedContainers =
      new HashMap<>();

  /*
   * Docker容器的IP、端口映射等属性是容器启动后才生成的，需要更新这些属性到RM中的应用信息
   */
  private final Map<ContainerId, ContainerStatus> updatedExistContainers =
      new HashMap<>();

  // NOTE: This is required for backward compatibility.
  // 等待减少资源的容器集合，用于向后兼容
  private final Map<ContainerId, Container> toBeDecreasedContainers =
      new HashMap<>();

  // NM上报的已经完成资源增加的容器集合
  private final Map<ContainerId, Container> nmReportedIncreasedContainers =
      new HashMap<>();

  // 最近一次心跳响应对象
  private NodeHeartbeatResponse latestNodeHeartBeatResponse = recordFactory
      .newRecordInstance(NodeHeartbeatResponse.class);

  // 节点状态机工厂，定义所有状态转换规则
  private static final StateMachineFactory<RMNodeImpl,
                                           NodeState,
                                           RMNodeEventType,
                                           RMNodeEvent> stateMachineFactory 
                 = new StateMachineFactory<RMNodeImpl,
                                           NodeState,
                                           RMNodeEventType,
                                           RMNodeEvent>(NodeState.NEW)
      // NEW状态转换
      .addTransition(NodeState.NEW,
          EnumSet.of(NodeState.RUNNING, NodeState.UNHEALTHY),
          RMNodeEventType.STARTED, new AddNodeTransition())
      .addTransition(NodeState.NEW, NodeState.NEW,
          RMNodeEventType.RESOURCE_UPDATE,
          new UpdateNodeResourceWhenUnusableTransition())
      .addTransition(NodeState.NEW, NodeState.DECOMMISSIONED,
          RMNodeEventType.DECOMMISSION,
          new DeactivateNodeTransition(NodeState.DECOMMISSIONED))
      .addTransition(NodeState.NEW, NodeState.LOST,
          RMNodeEventType.EXPIRE,
          new DeactivateNodeTransition(NodeState.LOST))
      .addTransition(NodeState.NEW, NodeState.NEW,
          RMNodeEventType.FINISHED_CONTAINERS_PULLED_BY_AM,
          new AddContainersToBeRemovedFromNMTransition())

      // RUNNING状态转换
      .addTransition(NodeState.RUNNING,
          EnumSet.of(NodeState.RUNNING, NodeState.UNHEALTHY),
          RMNodeEventType.STATUS_UPDATE,
          new StatusUpdateWhenHealthyTransition())
      .addTransition(NodeState.RUNNING, NodeState.DECOMMISSIONED,
          RMNodeEventType.DECOMMISSION,
          new DeactivateNodeTransition(NodeState.DECOMMISSIONED))
      .addTransition(NodeState.RUNNING, NodeState.DECOMMISSIONING,
          RMNodeEventType.GRACEFUL_DECOMMISSION,
          new DecommissioningNodeTransition(NodeState.RUNNING,
              NodeState.DECOMMISSIONING))
      .addTransition(NodeState.RUNNING, NodeState.LOST,
          RMNodeEventType.EXPIRE,
          new DeactivateNodeTransition(NodeState.LOST))
      .addTransition(NodeState.RUNNING, NodeState.REBOOTED,
          RMNodeEventType.REBOOTING,
          new DeactivateNodeTransition(NodeState.REBOOTED))
      .addTransition(NodeState.RUNNING, NodeState.RUNNING,
          RMNodeEventType.CLEANUP_APP, new CleanUpAppTransition())
      .addTransition(NodeState.RUNNING, NodeState.RUNNING,
          RMNodeEventType.CLEANUP_CONTAINER, new CleanUpContainerTransition())
      .addTransition(NodeState.RUNNING, NodeState.RUNNING,
          RMNodeEventType.FINISHED_CONTAINERS_PULLED_BY_AM,
          new AddContainersToBeRemovedFromNMTransition())
      .addTransition(NodeState.RUNNING, EnumSet.of(NodeState.RUNNING),
          RMNodeEventType.RECONNECTED, new ReconnectNodeTransition())
      .addTransition(NodeState.RUNNING, NodeState.RUNNING,
          RMNodeEventType.RESOURCE_UPDATE, new UpdateNodeResourceWhenRunningTransition())
      .addTransition(NodeState.RUNNING, NodeState.RUNNING,
          RMNodeEventType.UPDATE_CONTAINER,
          new UpdateContainersTransition())
      .addTransition(NodeState.RUNNING, NodeState.RUNNING,
          RMNodeEventType.SIGNAL_CONTAINER, new SignalContainerTransition())
      .addTransition(NodeState.RUNNING, NodeState.SHUTDOWN,
          RMNodeEventType.SHUTDOWN,
          new DeactivateNodeTransition(NodeState.SHUTDOWN))

      // REBOOTED状态转换
      .addTransition(NodeState.REBOOTED, NodeState.REBOOTED,
          RMNodeEventType.RESOURCE_UPDATE,
          new UpdateNodeResourceWhenUnusableTransition())

      // DECOMMISSIONED状态转换
      .addTransition(NodeState.DECOMMISSIONED, NodeState.DECOMMISSIONED,
          RMNodeEventType.RESOURCE_UPDATE,
          new UpdateNodeResourceWhenUnusableTransition())
      .addTransition(NodeState.DECOMMISSIONED, NodeState.DECOMMISSIONED,
          RMNodeEventType.FINISHED_CONTAINERS_PULLED_BY_AM,
          new AddContainersToBeRemovedFromNMTransition())

       // DECOMMISSIONING状态转换
      .addTransition(NodeState.DECOMMISSIONING, NodeState.DECOMMISSIONED,
          RMNodeEventType.DECOMMISSION,
          new DeactivateNodeTransition(NodeState.DECOMMISSIONED))
      .addTransition(NodeState.DECOMMISSIONING, NodeState.RUNNING,
          RMNodeEventType.RECOMMISSION,
          new RecommissionNodeTransition(NodeState.RUNNING))
      .addTransition(NodeState.DECOMMISSIONING, NodeState.DECOMMISSIONING,
          RMNodeEventType.RESOURCE_UPDATE,
          new UpdateNodeResourceWhenRunningTransition())
      .addTransition(NodeState.DECOMMISSIONING,
          EnumSet.of(NodeState.DECOMMISSIONING, NodeState.DECOMMISSIONED),
          RMNodeEventType.STATUS_UPDATE,
          new StatusUpdateWhenHealthyTransition())
      .addTransition(NodeState.DECOMMISSIONING, NodeState.DECOMMISSIONING,
          RMNodeEventType.GRACEFUL_DECOMMISSION,
          new DecommissioningNodeTransition(NodeState.DECOMMISSIONING,
              NodeState.DECOMMISSIONING))
      .addTransition(NodeState.DECOMMISSIONING, NodeState.LOST,
          RMNodeEventType.EXPIRE,
          new DeactivateNodeTransition(NodeState.LOST))
      .addTransition(NodeState.DECOMMISSIONING, NodeState.REBOOTED,
          RMNodeEventType.REBOOTING,
          new DeactivateNodeTransition(NodeState.REBOOTED))
      .addTransition(NodeState.DECOMMISSIONING, NodeState.DECOMMISSIONING,
         RMNodeEventType.FINISHED_CONTAINERS_PULLED_BY_AM,
         new AddContainersToBeRemovedFromNMTransition())

      .addTransition(NodeState.DECOMMISSIONING, NodeState.DECOMMISSIONING,
          RMNodeEventType.CLEANUP_APP, new CleanUpAppTransition())
      .addTransition(NodeState.DECOMMISSIONING, NodeState.SHUTDOWN,
          RMNodeEventType.SHUTDOWN,
          new DeactivateNodeTransition(NodeState.SHUTDOWN))

      // TODO (in YARN-3223) update resource when container finished.
      .addTransition(NodeState.DECOMMISSIONING, NodeState.DECOMMISSIONING,
          RMNodeEventType.CLEANUP_CONTAINER, new CleanUpContainerTransition())
      // TODO (in YARN-3223) update resource when container finished.
      .addTransition(NodeState.DECOMMISSIONING, NodeState.DECOMMISSIONING,
          RMNodeEventType.FINISHED_CONTAINERS_PULLED_BY_AM,
          new AddContainersToBeRemovedFromNMTransition())
      .addTransition(NodeState.DECOMMISSIONING, EnumSet.of(
          NodeState.DECOMMISSIONING, NodeState.DECOMMISSIONED),
          RMNodeEventType.RECONNECTED, new ReconnectNodeTransition())

      // LOST状态转换
      .addTransition(NodeState.LOST, NodeState.LOST,
          RMNodeEventType.RESOURCE_UPDATE,
          new UpdateNodeResourceWhenUnusableTransition())
      .addTransition(NodeState.LOST, NodeState.LOST,
          RMNodeEventType.FINISHED_CONTAINERS_PULLED_BY_AM,
          new AddContainersToBeRemovedFromNMTransition())

      // UNHEALTHY状态转换
      .addTransition(NodeState.UNHEALTHY,
          EnumSet.of(NodeState.UNHEALTHY, NodeState.RUNNING),
          RMNodeEventType.STATUS_UPDATE,
          new StatusUpdateWhenUnHealthyTransition())
      .addTransition(NodeState.UNHEALTHY, NodeState.DECOMMISSIONED,
          RMNodeEventType.DECOMMISSION,
          new DeactivateNodeTransition(NodeState.DECOMMISSIONED))
      .addTransition(NodeState.UNHEALTHY, NodeState.DECOMMISSIONING,
          RMNodeEventType.GRACEFUL_DECOMMISSION,
          new DecommissioningNodeTransition(NodeState.UNHEALTHY,
              NodeState.DECOMMISSIONING))
      .addTransition(NodeState.UNHEALTHY, NodeState.LOST,
          RMNodeEventType.EXPIRE,
          new DeactivateNodeTransition(NodeState.LOST))
      .addTransition(NodeState.UNHEALTHY, NodeState.REBOOTED,
          RMNodeEventType.REBOOTING,
          new De