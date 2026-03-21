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

import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.yarn.ams.ApplicationMasterServiceContext;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.ConfigurationProvider;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.nodelabels.NodeAttributesManager;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.SystemCredentialsForAppsProto;
import org.apache.hadoop.yarn.server.resourcemanager.ahs.RMApplicationHistoryWriter;
import org.apache.hadoop.yarn.server.resourcemanager.metrics.SystemMetricsPublisher;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMDelegatedNodeLabelsUpdater;
import org.apache.hadoop.yarn.server.resourcemanager.placement.PlacementManager;
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
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.PlacementConstraintManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.AllocationTagsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.distributed.QueueLimitCalculator;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.MultiNodeSortingManager;
import org.apache.hadoop.yarn.server.resourcemanager.security.AMRMTokenSecretManager;
import org.apache.hadoop.yarn.server.resourcemanager.security.ClientToAMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.DelegationTokenRenewer;
import org.apache.hadoop.yarn.server.resourcemanager.security.NMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.ProxyCAManager;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMDelegationTokenSecretManager;
import org.apache.hadoop.yarn.server.resourcemanager.timelineservice.RMTimelineCollectorManager;
import org.apache.hadoop.yarn.server.resourcemanager.volume.csi.VolumeManager;

/**
 * YARN ResourceManager 上下文接口，统一管理RM运行时核心组件与状态，提供全局访问入口。
 */
public interface RMContext extends ApplicationMasterServiceContext {

  /**
   * 获取RM事件分发器，用于事件驱动架构中各类事件的分发。
   * @return 事件分发器实例
   */
  Dispatcher getDispatcher();

  /**
   * 检查是否启用高可用(HA)模式。
   * @return true表示HA已启用，false表示单节点模式
   */
  boolean isHAEnabled();

  /**
   * 获取当前HA服务状态(活跃/待机/初始化/停止等)。
   * @return HA服务状态
   */
  HAServiceState getHAServiceState();

  /**
   * 获取RM状态存储，用于持久化RM运行状态支持故障恢复。
   * @return RM状态存储实例
   */
  RMStateStore getStateStore();

  /**
   * 获取所有运行中应用的映射表，key为应用ID，value为RM应用实例。
   * @return 所有RM应用的并发映射表
   */
  ConcurrentMap<ApplicationId, RMApp> getRMApps();

  /**
   * 获取所有应用的系统凭证映射表，用于应用访问安全认证。
   * @return 系统凭证并发映射表
   */
  ConcurrentMap<ApplicationId, SystemCredentialsForAppsProto>
      getSystemCredentialsForApps();

  /**
   * 获取所有已停用(已下线)节点的映射表。
   * @return 已停用节点的并发映射表
   */
  ConcurrentMap<NodeId, RMNode> getInactiveRMNodes();

  /**
   * 获取所有活跃节点的映射表。
   * @return 活跃节点的并发映射表
   */
  ConcurrentMap<NodeId, RMNode> getRMNodes();

  /**
   * 获取ApplicationMaster存活状态监视器，用于监控AM心跳判断是否存活。
   * @return AM存活监视器实例
   */
  AMLivelinessMonitor getAMLivelinessMonitor();

  /**
   * 获取完成状态AM的存活监视器，对即将结束的AM进行超时监控。
   * @return 完成态AM存活监视器实例
   */
  AMLivelinessMonitor getAMFinishingMonitor();

  /**
   * 获取容器分配过期处理器，清理超时未被使用的分配容器。
   * @return 容器分配过期处理器实例
   */
  ContainerAllocationExpirer getContainerAllocationExpirer();
  
  /**
   * 获取DelegationToken令牌 renew 管理器，负责令牌自动续期。
   * @return DelegationToken续期管理器实例
   */
  DelegationTokenRenewer getDelegationTokenRenewer();

  /**
   * 获取AM与RM之间通信的令牌密钥管理器。
   * @return AM-RM令牌密钥管理器实例
   */
  AMRMTokenSecretManager getAMRMTokenSecretManager();

  /**
   * 获取容器令牌密钥管理器，负责容器令牌的生成与验证。
   * @return 容器令牌密钥管理器实例
   */
  RMContainerTokenSecretManager getContainerTokenSecretManager();
  
  /**
   * 获取NodeManager令牌密钥管理器，负责NM节点令牌的生成与验证。
   * @return NM令牌密钥管理器实例
   */
  NMTokenSecretManagerInRM getNMTokenSecretManager();

  /**
   * 获取资源调度器实例，负责集群资源分配与调度。
   * @return 资源调度器实例
   */
  ResourceScheduler getScheduler();

  /**
   * 获取节点列表管理器，负责管理集群节点上下线与状态维护。
   * @return 节点列表管理器实例
   */
  NodesListManager getNodesListManager();

  /**
   * 获取客户端到AM的令牌密钥管理器，负责客户端访问AM的令牌认证。
   * @return 客户端-AM令牌密钥管理器实例
   */
  ClientToAMTokenSecretManagerInRM getClientToAMTokenSecretManager();

  /**
   * 获取RM管理服务实例，提供集群管理接口。
   * @return RM管理服务实例
   */
  AdminService getRMAdminService();

  /**
   * 获取客户端RM服务实例，处理客户端提交的应用管理请求。
   * @return 客户端RM服务实例
   */
  ClientRMService getClientRMService();

  /**
   * 获取ApplicationMaster服务实例，处理AM发来的请求。
   * @return AM服务实例
   */
  ApplicationMasterService getApplicationMasterService();

  /**
   * 获取资源追踪服务实例，处理NodeManager节点发来的心跳与状态上报。
   * @return 资源追踪服务实例
   */
  ResourceTrackerService getResourceTrackerService();

  /**
   * 设置客户端RM服务实例。
   * @param clientRMService 客户端RM服务实例
   */
  void setClientRMService(ClientRMService clientRMService);

  /**
   * 获取RM DelegationToken密钥管理器，负责 delegation 令牌的生成与验证。
   * @return RM DelegationToken密钥管理器实例
   */
  RMDelegationTokenSecretManager getRMDelegationTokenSecretManager();

  /**
   * 设置RM DelegationToken密钥管理器。
   * @param delegationTokenSecretManager 要设置的密钥管理器实例
   */
  void setRMDelegationTokenSecretManager(
      RMDelegationTokenSecretManager delegationTokenSecretManager);

  /**
   * 获取RM应用历史写入器，负责将应用运行信息写入历史存储。
   * @return RM应用历史写入器实例
   */
  RMApplicationHistoryWriter getRMApplicationHistoryWriter();

  /**
   * 设置RM应用历史写入器。
   * @param rmApplicationHistoryWriter 要设置的写入器实例
   */
  void setRMApplicationHistoryWriter(
      RMApplicationHistoryWriter rmApplicationHistoryWriter);

  /**
   * 设置系统指标发布器，用于发布RM系统 metrics 指标。
   * @param systemMetricsPublisher 要设置的指标发布器实例
   */
  void setSystemMetricsPublisher(SystemMetricsPublisher systemMetricsPublisher);

  /**
   * 获取系统指标发布器。
   * @return 系统指标发布器实例
   */
  SystemMetricsPublisher getSystemMetricsPublisher();

  /**
   * 设置时间线服务收集器管理器，用于收集应用运行指标。
   * @param timelineCollectorManager 要设置的收集器管理器实例
   */
  void setRMTimelineCollectorManager(
      RMTimelineCollectorManager timelineCollectorManager);

  /**
   * 获取时间线服务收集器管理器。
   * @return 时间线服务收集器管理器实例
   */
  RMTimelineCollectorManager getRMTimelineCollectorManager();

  /**
   * 获取配置提供者，用于提供动态配置更新能力。
   * @return 配置提供者实例
   */
  ConfigurationProvider getConfigurationProvider();

  /**
   * 检查是否启用工作保留恢复，故障恢复后保留已分配容器继续运行。
   * @return true表示启用工作保留恢复
   */
  boolean isWorkPreservingRecoveryEnabled();
  
  /**
   * 获取节点标签管理器，负责管理集群节点标签配置。
   * @return 节点标签管理器实例
   */
  RMNodeLabelsManager getNodeLabelManager();
  
  /**
   * 设置节点标签管理器。
   * @param mgr 要设置的节点标签管理器实例
   */
  public void setNodeLabelManager(RMNodeLabelsManager mgr);

  /**
   * 获取节点属性管理器，负责管理集群节点自定义属性。
   * @return 节点属性管理器实例
   */
  NodeAttributesManager getNodeAttributesManager();

  /**
   * 设置节点属性管理器。
   * @param mgr 要设置的节点属性管理器实例
   */
  void setNodeAttributesManager(NodeAttributesManager mgr);

  /**
   * 获取 delegated 节点标签更新器，支持节点标签的委托更新。
   * @return 委托节点标签更新器实例
   */
  RMDelegatedNodeLabelsUpdater getRMDelegatedNodeLabelsUpdater();

  /**
   * 设置委托节点标签更新器。
   * @param nodeLabelsUpdater 要设置的更新器实例
   */
  void setRMDelegatedNodeLabelsUpdater(
      RMDelegatedNodeLabelsUpdater nodeLabelsUpdater);

  /**
   * 获取RM当前 epoch 编号，用于HA切换后标识新版本，解决缓存一致性问题。
   * @return 当前 epoch 编号
   */
  long getEpoch();

  /**
   * 获取资源预约系统，负责资源提前预约的管理。
   * @return 资源预约系统实例
   */
  ReservationSystem getReservationSystem();

  /**
   * 检查调度器是否已就绪，可分配容器给应用。
   * @return true表示调度器就绪可分配容器
   */
  boolean isSchedulerReadyForAllocatingContainers();
  
  /**
   * 获取Yarn配置实例。
   * @return Yarn配置
   */
  Configuration getYarnConfiguration();
  
  /**
   * 获取应用队列放置管理器，负责将应用放置到对应队列。
   * @return 队列放置管理器实例
   */
  PlacementManager getQueuePlacementManager();
  
  /**
   * 设置队列放置管理器。
   * @param placementMgr 要设置的放置管理器实例
   */
  void setQueuePlacementManager(PlacementManager placementMgr);

  /**
   * 设置领导者选举服务，用于HA模式下选举主RM。
   * @param elector 领导者选举服务实例
   */
  void setLeaderElectorService(EmbeddedElector elector);

  /**
   * 获取领导者选举服务。
   * @return 领导者选举服务实例
   */
  EmbeddedElector getLeaderElectorService();

  /**
   * 获取NodeManager端队列配额计算器，用于分布式调度中计算队列资源限制。
   * @return 队列配额计算器实例
   */
  QueueLimitCalculator getNodeManagerQueueLimitCalculator();

  /**
   * 设置应用生命周期监视器，监控应用运行超时。
   * @param rmAppLifetimeMonitor 要设置的生命周期监视器实例
   */
  void setRMAppLifetimeMonitor(RMAppLifetimeMonitor rmAppLifetimeMonitor);

  /**
   * 获取应用生命周期监视器。
   * @return 应用生命周期监视器实例
   */
  RMAppLifetimeMonitor getRMAppLifetimeMonitor();

  /**
   * 获取HA模式下Zookeeper连接状态描述。
   * @return Zookeeper连接状态字符串
   */
  String getHAZookeeperConnectionState();

  /**
   * 获取ResourceManager实例。
   * @return ResourceManager实例
   */
  ResourceManager getResourceManager();

  /**
   * 获取资源配置文件管理器，管理资源配置定义。
   * @return 资源配置文件管理器实例
   */
  ResourceProfilesManager getResourceProfilesManager();

  /**
   * 设置资源配置文件管理器。
   * @param mgr 要设置的资源配置文件管理器实例
   */
  void setResourceProfilesManager(ResourceProfilesManager mgr);

  /**
   * 生成应用Proxy访问URL，用于UI跳转访问ApplicationMaster。
   * @param conf YARN配置
   * @param applicationId 应用ID
   * @return 应用Proxy访问URL字符串
   */
  String getAppProxyUrl(Configuration conf, ApplicationId applicationId);

  /**
   * 获取分配标签管理器，管理容器分配标签约束。
   * @return 分配标签管理器实例
   */
  AllocationTagsManager getAllocationTagsManager();

  /**
   * 设置分配标签管理器。
   * @param allocationTagsManager 要设置的分配标签管理器实例
   */
  void setAllocationTagsManager(AllocationTagsManager allocationTagsManager);

  /**
   * 获取放置约束管理器，处理容器放置位置约束。
   * @return 放置约束管理器实例
   */
  PlacementConstraintManager getPlacementConstraintManager();

  /**
   * 设置放置约束管理器。
   * @param placementConstraintManager 要设置的放置约束管理器实例
   */
  void setPlacementConstraintManager(
      PlacementConstraintManager placementConstraintManager);

  /**
   * 获取多节点排序管理器，用于调度中节点排序选址。
   * @return 多节点排序管理器实例
   */
  MultiNodeSortingManager<SchedulerNode> getMultiNodeSortingManager();

  /**
   * 设置多节点排序管理器。
   * @param multiNodeSortingManager 要设置的多节点排序管理器实例
   */
  void setMultiNodeSortingManager(
      MultiNodeSortingManager<SchedulerNode> multiNodeSortingManager);

  /**
   * 获取代理CA管理器，管理代理证书签发。
   * @return 代理CA管理器实例
   */
  ProxyCAManager getProxyCAManager();

  /**
   * 设置代理CA管理器。
   * @param proxyCAManager 要设置的代理CA管理器实例
   */
  void setProxyCAManager(ProxyCAManager proxyCAManager);

  /**
   * 获取CSI卷管理器，管理YARN容器使用CSI存储卷。
   * @return CSI卷管理器实例
   */
  VolumeManager getVolumeManager();

  /**
   * 设置CSI卷管理器。
   * @param volumeManager 要设置的CSI卷管理器实例
   */
  void setVolumeManager(VolumeManager volumeManager);

  /**
   * 获取当前令牌序列号，用于令牌生成时保证唯一性。
   * @return 当前令牌序列号
   */
  long getTokenSequenceNo();

  /**
   * 令牌序列号自增，生成下一个唯一序列号。
   */
  void incrTokenSequenceNo();
}