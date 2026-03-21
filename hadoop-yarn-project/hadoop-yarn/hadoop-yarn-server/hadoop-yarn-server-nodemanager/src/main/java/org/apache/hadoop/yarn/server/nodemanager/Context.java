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

package org.apache.hadoop.yarn.server.nodemanager;

import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.server.api.protocolrecords.LogAggregationReport;
import org.apache.hadoop.yarn.server.api.records.AppCollectorData;
import org.apache.hadoop.yarn.server.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.AuxServices;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.ContainerManager;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.resourceplugin.ResourcePluginManager;
import org.apache.hadoop.yarn.server.nodemanager.logaggregation.tracker.NMLogAggregationStatusTracker;
import org.apache.hadoop.yarn.server.nodemanager.metrics.NodeManagerMetrics;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService;
import org.apache.hadoop.yarn.server.scheduler.OpportunisticContainerAllocator;
import org.apache.hadoop.yarn.server.nodemanager.security.NMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.nodemanager.security.NMTokenSecretManagerInNM;
import org.apache.hadoop.yarn.server.nodemanager.timelineservice.NMTimelinePublisher;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;

/**
 * NodeManager内部上下文接口，用于在NodeManager各组件之间共享运行时状态信息。
 */
public interface Context {

  /**
   * 获取当前Node节点的ID，仅在ContainerManager启动后可用。
   * 
   * @return 当前Node的NodeId
   */
  NodeId getNodeId();

  /**
   * 获取当前Node的HTTP服务端口，仅在Web服务器启动后可用。
   * 
   * @return HTTP端口号
   */
  int getHttpPort();

  ConcurrentMap<ApplicationId, Application> getApplications();

  Map<ApplicationId, Credentials> getSystemCredentialsForApps();

  /**
   * 获取当前节点正在向ResourceManager注册的应用收集器列表。
   * @return 正在注册的收集器映射，如果未开启timeline service v2则返回null
   */
  ConcurrentMap<ApplicationId, AppCollectorData> getRegisteringCollectors();

  /**
   * 获取当前节点已知的、已在ResourceManager注册完成的应用收集器列表。
   * @return 已知收集器映射，如果未开启timeline service v2则返回null
   */
  ConcurrentMap<ApplicationId, AppCollectorData> getKnownCollectors();

  ConcurrentMap<ContainerId, Container> getContainers();

  ConcurrentMap<ContainerId, org.apache.hadoop.yarn.api.records.Container>
      getIncreasedContainers();

  NMContainerTokenSecretManager getContainerTokenSecretManager();
  
  NMTokenSecretManagerInNM getNMTokenSecretManager();

  NodeHealthStatus getNodeHealthStatus();

  ContainerManager getContainerManager();

  NodeResourceMonitor getNodeResourceMonitor();

  LocalDirsHandlerService getLocalDirsHandler();

  ApplicationACLsManager getApplicationACLsManager();

  NMStateStoreService getNMStateStore();

  boolean getDecommissioned();

  Configuration getConf();

  void setDecommissioned(boolean isDecommissioned);

  ConcurrentLinkedQueue<LogAggregationReport>
      getLogAggregationStatusForApps();

  NodeStatusUpdater getNodeStatusUpdater();

  boolean isDistributedSchedulingEnabled();

  OpportunisticContainerAllocator getContainerAllocator();

  void setNMTimelinePublisher(NMTimelinePublisher nmMetricsPublisher);

  NMTimelinePublisher getNMTimelinePublisher();

  NMLogAggregationStatusTracker getNMLogAggregationStatusTracker();

  ContainerExecutor getContainerExecutor();

  ContainerStateTransitionListener getContainerStateTransitionListener();

  ResourcePluginManager getResourcePluginManager();

  NodeManagerMetrics getNodeManagerMetrics();

  /**
   * 获取关联到当前NodeManager的删除服务实例。
   *
   * @return 当前NM的DeletionService实例
   */
  DeletionService getDeletionService();

  void setAuxServices(AuxServices auxServices);

  AuxServices getAuxServices();
}