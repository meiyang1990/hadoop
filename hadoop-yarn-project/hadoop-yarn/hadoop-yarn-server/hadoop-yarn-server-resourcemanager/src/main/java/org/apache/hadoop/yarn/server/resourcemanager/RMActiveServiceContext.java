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

package org.apache.hadoop.yarn.server.resourcemanager;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.nodelabels.NodeAttributesManager;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.SystemCredentialsForAppsProto;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMDelegatedNodeLabelsUpdater;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.placement.PlacementManager;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.NullRMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationSystem;
import org.apache.hadoop.yarn.server.resourcemanager.resource.ResourceProfilesManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.AMLivelinessMonitor;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.monitor.RMAppLifetimeMonitor;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.ContainerAllocationExpirer;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.AllocationTagsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.PlacementConstraintManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.distributed.QueueLimitCalculator;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.MultiNodeSortingManager;
import org.apache.hadoop.yarn.server.resourcemanager.security.AMRMTokenSecretManager;
import org.apache.hadoop.yarn.server.resourcemanager.security.ClientToAMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.DelegationTokenRenewer;
import org.apache.hadoop.yarn.server.resourcemanager.security.NMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.ProxyCAManager;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMDelegationTokenSecretManager;
import org.apache.hadoop.yarn.server.resourcemanager.volume.csi.VolumeManager;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.SystemClock;

/**
 * 活性ResourceManager服务上下文，仅在Active RM上维护需要运行的核心服务与运行时数据，仅被RMContext使用。
 */
@Private
@Unstable
public class RMActiveServiceContext {

  private static final Logger LOG = LoggerFactory
      .getLogger(RMActiveServiceContext.class);

  // 存储所有运行中应用，key为应用ID，value为RMApp对象
  private final ConcurrentMap<ApplicationId, RMApp> applications =
      new ConcurrentHashMap<ApplicationId, RMApp>();

  // 存储所有活跃节点，key为节点ID，value为RMNode对象
  private final ConcurrentMap<NodeId, RMNode> nodes =
      new ConcurrentHashMap<NodeId, RMNode>();

  // 存储所有非活跃节点，key为节点ID，value为RMNode对象
  private final ConcurrentMap<NodeId, RMNode> inactiveNodes =
      new ConcurrentHashMap<NodeId, RMNode>();

  // 存储各应用的系统凭证信息
  private final ConcurrentMap<ApplicationId, SystemCredentialsForAppsProto> systemCredentials =
    new ConcurrentHashMap<ApplicationId, SystemCredentialsForAppsProto>();

  // 是否启用工作保留恢复（故障转移时不杀死已运行任务）
  private boolean isWorkPreservingRecoveryEnabled;

  private AMLivelinessMonitor amLivelinessMonitor;
  private AMLivelinessMonitor amFinishingMonitor;
  private RMStateStore stateStore = null;
  private ContainerAllocationExpirer containerAllocationExpirer;
  private DelegationTokenRenewer delegationTokenRenewer;
  private AMRMTokenSecretManager amRMTokenSecretManager;
  private RMContainerTokenSecretManager containerTokenSecretManager;
  private NMTokenSecretManagerInRM nmTokenSecretManager;
  private ClientToAMTokenSecretManagerInRM clientToAMTokenSecretManager;
  private ClientRMService clientRMService;
  private RMDelegationTokenSecretManager rmDelegationTokenSecretManager;
  private ResourceScheduler scheduler;
  private ReservationSystem reservationSystem;
  private NodesListManager nodesListManager;
  private ResourceTrackerService resourceTrackerService;
  private ApplicationMasterService applicationMasterService;

  private RMNodeLabelsManager nodeLabelManager;
  private NodeAttributesManager nodeAttributesManager;
  private RMDelegatedNodeLabelsUpdater rmDelegatedNodeLabelsUpdater;
  // RM启动纪元，标识当前RM主节点任期
  private long epoch;
  private Clock systemClock = SystemClock.getInstance();
  // 调度器恢复开始时间
  private long schedulerRecoveryStartTime = 0;
  // 调度器恢复最大等待时间
  private long schedulerRecoveryWaitTime = 0;
  // 日志打印标记，避免重复打印
  private boolean printLog = true;
  // 标记调度器是否已完成恢复，可分配容器
  private boolean isSchedulerReady = false;
  private PlacementManager queuePlacementManager = null;

  private RMAppLifetimeMonitor rmAppLifetimeMonitor;
  // 分布式调度中队列资源限制计算器
  private QueueLimitCalculator queueLimitCalculator;
  // 分配标签管理器，支持基于标签的放置约束
  private AllocationTagsManager allocationTagsManager;
  // 放置约束管理器
  private PlacementConstraintManager placementConstraintManager;
  // 资源配置文件管理器
  private ResourceProfilesManager resourceProfilesManager;
  // 多节点排序管理器，用于容器放置节点选择排序
  private MultiNodeSortingManager<SchedulerNode> multiNodeSortingManager;

  // 代理CA管理器，用于AM证书签名
  private ProxyCAManager proxyCAManager;
  // CSI卷管理器
  private VolumeManager volumeManager;

  // Token序列号生成器，保证Token唯一性
  private AtomicLong tokenSequenceNo = new AtomicLong(1);

  /**
   * 构造函数，初始化放置管理器。
   */
  public RMActiveServiceContext() {
    queuePlacementManager = new PlacementManager();
  }

  /**
   * 带参数构造函数，初始化核心服务组件。
   * @param rmDispatcher 事件分发器
   * @param containerAllocationExpirer 容器分配过期器
   * @param amLivelinessMonitor AM存活监控器
   * @param amFinishingMonitor 完成阶段AM存活监控器
   * @param delegationTokenRenewer 委托令牌续订器
   * @param appTokenSecretManager AM-RM令牌密钥管理器
   * @param containerTokenSecretManager 容器令牌密钥管理器
   * @param nmTokenSecretManager NM令牌密钥管理器
   * @param clientToAMTokenSecretManager 客户端-AM令牌密钥管理器
   * @param scheduler 资源调度器
   */
  @Private
  @Unstable
  public RMActiveServiceContext(Dispatcher rmDispatcher,
      ContainerAllocationExpirer containerAllocationExpirer,
      AMLivelinessMonitor amLivelinessMonitor,
      AMLivelinessMonitor amFinishingMonitor,
      DelegationTokenRenewer delegationTokenRenewer,
      AMRMTokenSecretManager appTokenSecretManager,
      RMContainerTokenSecretManager containerTokenSecretManager,
      NMTokenSecretManagerInRM nmTokenSecretManager,
      ClientToAMTokenSecretManagerInRM clientToAMTokenSecretManager,
      ResourceScheduler scheduler) {
    this();
    this.setContainerAllocationExpirer(containerAllocationExpirer);
    this.setAMLivelinessMonitor(amLivelinessMonitor);
    this.setAMFinishingMonitor(amFinishingMonitor);
    this.setDelegationTokenRenewer(delegationTokenRenewer);
    this.setAMRMTokenSecretManager(appTokenSecretManager);
    this.setContainerTokenSecretManager(containerTokenSecretManager);
    this.setNMTokenSecretManager(nmTokenSecretManager);
    this.setClientToAMTokenSecretManager(clientToAMTokenSecretManager);
    this.setScheduler(scheduler);

    // 初始化空状态存储，默认实现
    RMStateStore nullStore = new NullRMStateStore();
    nullStore.setRMDispatcher(rmDispatcher);
    try {
      nullStore.init(new YarnConfiguration());
      setStateStore(nullStore);
    } catch (Exception e) {
      assert false;
    }
  }

  @Private
  @Unstable
  public void setStateStore(RMStateStore store) {
    stateStore = store;
  }

  @Private
  @Unstable
  public ClientRMService getClientRMService() {
    return clientRMService;
  }

  @Private
  @Unstable
  public ApplicationMasterService getApplicationMasterService() {
    return applicationMasterService;
  }

  @Private
  @Unstable
  public ResourceTrackerService getResourceTrackerService() {
    return resourceTrackerService;
  }

  @Private
  @Unstable
  public RMStateStore getStateStore() {
    return stateStore;
  }

  @Private
  @Unstable
  public ConcurrentMap<ApplicationId, RMApp> getRMApps() {
    return this.applications;
  }

  @Private
  @Unstable
  public ConcurrentMap<NodeId, RMNode> getRMNodes() {
    return this.nodes;
  }

  @Private
  @Unstable
  public ConcurrentMap<NodeId, RMNode> getInactiveRMNodes() {
    return this.inactiveNodes;
  }

  @Private
  @Unstable
  public ContainerAllocationExpirer getContainerAllocationExpirer() {
    return this.containerAllocationExpirer;
  }

  @Private
  @Unstable
  public AMLivelinessMonitor getAMLivelinessMonitor() {
    return this.amLivelinessMonitor;
  }

  @Private
  @Unstable
  public AMLivelinessMonitor getAMFinishingMonitor() {
    return this.amFinishingMonitor;
  }

  @Private
  @Unstable
  public DelegationTokenRenewer getDelegationTokenRenewer() {
    return delegationTokenRenewer;
  }

  @Private
  @Unstable
  public AMRMTokenSecretManager getAMRMTokenSecretManager() {
    return this.amRMTokenSecretManager;
  }

  @Private
  @Unstable
  public RMContainerTokenSecretManager getContainerTokenSecretManager() {
    return this.containerTokenSecretManager;
  }

  @Private
  @Unstable
  public NMTokenSecretManagerInRM getNMTokenSecretManager() {
    return this.nmTokenSecretManager;
  }

  @Private
  @Unstable
  public ResourceScheduler getScheduler() {
    return this.scheduler;
  }

  @Private
  @Unstable
  public ReservationSystem getReservationSystem() {
    return this.reservationSystem;
  }

  @Private
  @Unstable
  public NodesListManager getNodesListManager() {
    return this.nodesListManager;
  }

  @Private
  @Unstable
  public ClientToAMTokenSecretManagerInRM getClientToAMTokenSecretManager() {
    return this.clientToAMTokenSecretManager;
  }

  @Private
  @Unstable
  public void setClientRMService(ClientRMService clientRMService) {
    this.clientRMService = clientRMService;
  }

  @Private
  @Unstable
  public RMDelegationTokenSecretManager getRMDelegationTokenSecretManager() {
    return this.rmDelegationTokenSecretManager;
  }

  @Private
  @Unstable
  public void setRMDelegationTokenSecretManager(
      RMDelegationTokenSecretManager delegationTokenSecretManager) {
    this.rmDelegationTokenSecretManager = delegationTokenSecretManager;
  }

  @Private
  @Unstable
  void setContainerAllocationExpirer(
      ContainerAllocationExpirer containerAllocationExpirer) {
    this.containerAllocationExpirer = containerAllocationExpirer;
  }

  @Private
  @Unstable
  void setAMLivelinessMonitor(AMLivelinessMonitor amLivelinessMonitor) {
    this.amLivelinessMonitor = amLivelinessMonitor;
  }

  @Private
  @Unstable
  void setAMFinishingMonitor(AMLivelinessMonitor amFinishingMonitor) {
    this.amFinishingMonitor = amFinishingMonitor;
  }

  @Private
  @Unstable
  void setContainerTokenSecretManager(
      RMContainerTokenSecretManager containerTokenSecretManager) {
    this.containerTokenSecretManager = containerTokenSecretManager;
  }

  @Private
  @Unstable
  void setNMTokenSecretManager(NMTokenSecretManagerInRM nmTokenSecretManager) {
    this.nmTokenSecretManager = nmTokenSecretManager;
  }

  @Private
  @Unstable
  void setScheduler(ResourceScheduler scheduler) {
    this.scheduler = scheduler;
  }

  @Private
  @Unstable
  void setReservationSystem(ReservationSystem reservationSystem) {
    this.reservationSystem = reservationSystem;
  }

  @Private
  @Unstable
  void setDelegationTokenRenewer(DelegationTokenRenewer delegationTokenRenewer) {
    this.delegationTokenRenewer = delegationTokenRenewer;
  }

  @Private
  @Unstable
  void setClientToAMTokenSecretManager(
      ClientToAMTokenSecretManagerInRM clientToAMTokenSecretManager) {
    this.clientToAMTokenSecretManager = clientToAMTokenSecretManager;
  }

  @Private
  @Unstable
  void setAMRMTokenSecretManager(AMRMTokenSecretManager amRMTokenSecretManager) {
    this.amRMTokenSecretManager = amRMTokenSecretManager;
  }

  @Private
  @Unstable
  void setNodesListManager(NodesListManager nodesListManager) {
    this.nodesListManager = nodesListManager;
  }

  @Private
  @Unstable
  void setApplicationMasterService(
      ApplicationMasterService applicationMasterService) {
    this.applicationMasterService = applicationMasterService;
  }

  @Private
  @Unstable
  void setResourceTrackerService(ResourceTrackerService resourceTrackerService) {
    this.resourceTrackerService = resourceTrackerService;
  }

  @Private
  @Unstable
  public void setWorkPreservingRecoveryEnabled(boolean enabled) {
    this.isWorkPreservingRecoveryEnabled = enabled;
  }

  @Private
  @Unstable
  public boolean isWorkPreservingRecoveryEnabled() {
    return this.isWorkPreservingRecoveryEnabled;
  }

  @Private
  @Unstable
  public long getEpoch() {
    return this.epoch;
  }

  @Private
  @Unstable
  void setEpoch(long epoch) {
    this.epoch = epoch;
  }

  @Private
  @Unstable
  public RMNodeLabelsManager getNodeLabelManager() {
    return nodeLabelManager;
  }

  @Private
  @Unstable
  public void setNodeLabelManager(RMNodeLabelsManager mgr) {
    nodeLabelManager = mgr;
  }

  @Private
  @Unstable
  public NodeAttributesManager getNodeAttributesManager() {
    return nodeAttributesManager;
  }

  @Private
  @Unstable
  public void setNodeAttributesManager(NodeAttributesManager mgr) {
    nodeAttributesManager = mgr;
  }

  @Private
  @Unstable
  public AllocationTagsManager getAllocationTagsManager() {
    return allocationTagsManager;
  }

  @Private
  @Unstable
  public void setAllocationTagsManager(
      AllocationTagsManager allocationTagsManager) {
    this.allocationTagsManager = allocationTagsManager;
  }

  @Private
  @Unstable
  public PlacementConstraintManager getPlacementConstraintManager() {
    return placementConstraintManager;
  }

  @Private
  @Unstable
  public void setPlacementConstraintManager(
      PlacementConstraintManager placementConstraintManager) {
    this.placementConstraintManager = placementConstraintManager;
  }

  @Private
  @Unstable
  public RMDelegatedNodeLabelsUpdater getRMDelegatedNodeLabelsUpdater() {
    return rmDelegatedNodeLabelsUpdater;
  }

  @Private
  @Unstable
  public void setRMDelegatedNodeLabelsUpdater(
      RMDelegatedNodeLabels