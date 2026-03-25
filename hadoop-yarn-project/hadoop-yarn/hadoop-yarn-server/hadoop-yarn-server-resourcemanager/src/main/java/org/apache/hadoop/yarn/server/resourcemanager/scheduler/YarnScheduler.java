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
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.SchedulingRequest;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.QueueEntitlement;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.SchedulerResourceTypes;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;

import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.SettableFuture;

/**
 * YARN调度器核心接口，定义了资源管理器与调度组件之间的交互契约
 * 所有YARN调度器实现（容量调度、公平调度等）都需要实现该接口
 * 提供资源分配、队列管理、权限检查、应用管理等核心调度能力
 */
public interface YarnScheduler extends EventHandler<SchedulerEvent> {

  /**
   * 获取指定队列的详细信息
   *
   * @param queueName 队列名称
   * @param includeChildQueues 是否包含子队列信息
   * @param recursive 是否递归获取所有层级的子队列
   * @return 队列信息对象
   * @throws IOException IO异常
   */
  @Public
  @Stable
  public QueueInfo getQueueInfo(String queueName, boolean includeChildQuees,
      boolean recursive) throws IOException;

  /**
   * 获取当前用户对所有队列的ACL权限信息
   * @return 当前用户所有队列的ACL权限列表
   */
  @Public
  @Stable
  public List<QueueUserACLInfo> getQueueUserAclInfo();

  /**
   * 获取集群总资源容量
   * @return 集群总资源
   */
  @LimitedPrivate("yarn")
  @Unstable
  public Resource getClusterResource();

  /**
   * 获取可分配容器的最小资源量
   * @return 最小可分配资源
   */
  @Public
  @Stable
  public Resource getMinimumResourceCapability();
  
  /**
   * 获取集群级别可分配容器的最大资源量
   * @return 最大可分配资源
   */
  @Public
  @Stable
  public Resource getMaximumResourceCapability();

  /**
   * 获取指定队列可分配容器的最大资源量
   * @param queueName 队列名称
   * @return 指定队列最大可分配资源
   */
  @Public
  @Stable
  public Resource getMaximumResourceCapability(String queueName);

  /**
   * 获取调度器使用的资源计算器
   * @return 资源计算器实例
   */
  @LimitedPrivate("yarn")
  @Evolving
  ResourceCalculator getResourceCalculator();

  /**
   * 获取集群中可用节点数量
   * @return 可用节点数量
   */
  @Public
  @Stable
  public int getNumClusterNodes();
  
  /**
   * ApplicationMaster与调度器交互的核心API
   * ApplicationMaster通过该接口向调度器请求/更新容器资源、指定分配位置偏好等
   * @param appAttemptId 应用尝试ID
   * @param ask 应用的资源请求列表，包含位置、资源量、数量、位置宽松性等信息
   * @param schedulingRequests 增强型资源请求列表，支持分配标签等扩展能力
   * @param release 需要释放的容器ID列表
   * @param blacklistAdditions 需要添加到黑名单的节点/机架列表
   * @param blacklistRemovals 需要从黑名单移除的节点/机架列表
   * @param updateRequests 容器升降级更新请求
   * @return 应用的分配结果，包含分配给该应用的容器信息
   */
  @Public
  @Stable
  Allocation allocate(ApplicationAttemptId appAttemptId,
      List<ResourceRequest> ask, List<SchedulingRequest> schedulingRequests,
      List<ContainerId> release, List<String> blacklistAdditions,
      List<String> blacklistRemovals, ContainerUpdates updateRequests);

  /**
   * 获取指定节点的资源使用报告
   *
   * @param nodeId 节点ID
   * @return 节点调度信息报告，节点不存在则返回null
   */
  @LimitedPrivate("yarn")
  @Stable
  public SchedulerNodeReport getNodeReport(NodeId nodeId);
  
  /**
   * 获取指定应用尝试的调度信息
   * @param appAttemptId 应用尝试ID
   * @return 应用尝试调度信息报告
   */
  @LimitedPrivate("yarn")
  @Stable
  SchedulerAppReport getSchedulerAppInfo(ApplicationAttemptId appAttemptId);

  /**
   * 获取指定应用尝试的资源使用报告
   * @param appAttemptId 应用尝试ID
   * @return 应用尝试资源使用报告
   */
  @LimitedPrivate("yarn")
  @Evolving
  ApplicationResourceUsageReport getAppResourceUsageReport(
      ApplicationAttemptId appAttemptId);
  
  /**
   * 获取根队列的调度指标
   * @return 根队列指标
   */
  @LimitedPrivate("yarn")
  @Evolving
  QueueMetrics getRootQueueMetrics();

  /**
   * 检查用户对指定队列是否拥有指定操作权限
   * 如果用户拥有ADMINISTER_QUEUE权限，则可以查看/修改该队列中的所有应用
   *
   * @param callerUGI 调用者用户信息
   * @param acl 要检查的队列权限
   * @param queueName 队列名称
   * @return true表示有权限，false表示无权限
   */
  boolean checkAccess(UserGroupInformation callerUGI,
      QueueACL acl, String queueName);
  
  /**
   * 获取指定队列下所有应用尝试ID列表
   * @param queueName 队列名称
   * @return 指定队列中所有应用尝试ID列表
   */
  @LimitedPrivate("yarn")
  @Stable
  public List<ApplicationAttemptId> getAppsInQueue(String queueName);

  /**
   * 根据容器ID获取对应的RMContainer对象
   *
   * @param containerId 容器ID
   * @return 对应RMContainer对象
   */
  @LimitedPrivate("yarn")
  @Unstable
  public RMContainer getRMContainer(ContainerId containerId);

  /**
   * 将指定应用移动到目标队列
   * @param appId 应用ID
   * @param newQueue 目标队列名称
   * @return 应用实际移动到的队列名称
   * @throws YarnException 移动失败时抛出异常
   */
  @LimitedPrivate("yarn")
  @Evolving
  public String moveApplication(ApplicationId appId, String newQueue)
      throws YarnException;

  /**
   * 预校验应用移动队列操作是否合法
   * @param appId 应用ID
   * @param newQueue 目标队列名称
   * @throws YarnException 预校验失败时抛出异常
   */
  @LimitedPrivate("yarn")
  @Evolving
  public void preValidateMoveApplication(ApplicationId appId,
      String newQueue) throws YarnException;

  /**
   * 将源队列中所有应用移动到目标队列，清空源队列
   *
   * @param sourceQueue 源队列名称
   * @param destQueue 目标队列名称
   * @throws YarnException 移动失败时抛出异常
   */
  void moveAllApps(String sourceQueue, String destQueue) throws YarnException;

  /**
   * 终止指定队列中所有正在运行的应用
   *
   * @param queueName 要清空的队列名称
   * @throws YarnException 操作失败时抛出异常
   */
  void killAllAppsInQueue(String queueName) throws YarnException;

  /**
   * 移除已存在的队列
   * 具体实现可能会对移除条件进行限制（例如队列必须无运行应用、权限配额为零、必须是叶子队列等）
   *
   * @param queueName 要移除的队列名称
   * @throws YarnException 移除失败时抛出异常
   */
  void removeQueue(String queueName) throws YarnException;

  /**
   * 向调度器添加新队列
   * 具体实现可能会对动态添加条件进行限制（例如必须是叶子队列、必须挂载到已有父队列、权限配额为零等）
   *
   * @param newQueue 要添加的队列对象
   * @throws YarnException YARN异常
   * @throws IOException IO异常
   */
  void addQueue(Queue newQueue) throws YarnException, IOException;

  /**
   * 更新队列的资源配额
   * 需要满足不变量约束（例如父队列不超卖、配额不为负等）
   * 配额是通用概念，在公平调度中代表权重，在容量调度中代表容量占比
   *
   * @param queue 要更新配额的队列名称
   * @param entitlement 新的配额信息，包含容量、最大容量等
   * @throws YarnException 更新失败时抛出异常
   */
  void setEntitlement(String queue, QueueEntitlement entitlement)
      throws YarnException;

  /**
   * 获取Reservation系统管理的所有计划队列名称列表
   * @return 支持资源Reservation的队列列表
   * @throws YarnException 获取失败时抛出异常
   */
  public Set<String> getPlanQueues() throws YarnException;  

  /**
   * 获取调度过程中需要考虑的资源类型集合
   *
   * @return 调度资源类型枚举集合
   */
  public EnumSet<SchedulerResourceTypes> getSchedulingResourceTypes();

  /**
   * 根据队列配置校验应用提交的优先级是否合法，返回最终生效优先级
   *
   * @param priorityRequestedByApp 应用提交的优先级
   * @param user 提交应用的用户
   * @param queuePath 队列路径
   * @param applicationId 应用ID
   * @return 调度器最终生效的优先级
   * @throws YarnException 校验失败时抛出异常
   */
  public Priority checkAndGetApplicationPriority(Priority priorityRequestedByApp,
      UserGroupInformation user, String queuePath, ApplicationId applicationId)
      throws YarnException;

  /**
   * 运行时修改已提交应用的优先级
   *
   * @param newPriority 新的优先级
   * @param applicationId 应用ID
   * @param future 用于接收状态存储操作的异常结果
   * @param user 操作发起用户
   * @return 更新后的生效优先级
   * @throws YarnException 更新失败时抛出异常
   */
  public Priority updateApplicationPriority(Priority newPriority,
      ApplicationId applicationId, SettableFuture<Object> future,
      UserGroupInformation user) throws YarnException;

  /**
   * 获取应用保留的前一次尝试的活跃容器，用于工作保留式AM重启
   *
   * @param appAttemptId 当前应用尝试ID
   *
   * @return 前一次尝试保留的活跃容器列表
   */
  List<Container> getTransferredContainers(ApplicationAttemptId appAttemptId);

  /**
   * 根据配置设置集群最大应用优先级
   * 
   * @param conf 配置对象
   * @throws YarnException 设置失败时抛出异常
   */
  void setClusterMaxPriority(Configuration conf) throws YarnException;

  /**
   * 获取指定应用尝试的待处理资源请求列表
   *
   * @param attemptId 应用尝试ID
   * @return 待处理资源请求列表
   */
  List<ResourceRequest> getPendingResourceRequestsForAttempt(
      ApplicationAttemptId attemptId);

  /**
   * 获取指定应用尝试的待处理调度请求列表
   *
   * @param attemptId 应用尝试ID
   *
   * @return 待处理调度请求列表
   */
  List<SchedulingRequest> getPendingSchedulingRequestsForAttempt(
      ApplicationAttemptId attemptId);

  /**
   * 获取集群级别最大应用优先级
   * 
   * @return 集群最大应用优先级
   */
  Priority getMaxClusterLevelAppPriority();

  /**
   * 根据节点ID获取对应的SchedulerNode对象
   *
   * @param nodeId 节点ID
   *
   * @return 对应节点的SchedulerNode对象
   */
  SchedulerNode getSchedulerNode(NodeId nodeId);

  /**
   * 对资源请求进行归一化处理，使用调度器级别或队列级别的最大资源限制
   *
   * @param requestedResource 待归一化的资源
   * @param maxResourceCapability 最大容器分配值，如果为null或空则使用调度器级别的最大限制
   * @return 归一化后的资源
   */
  Resource getNormalizedResource(Resource requestedResource,
      Resource maxResourceCapability);

  /**
   * 根据队列配置校验应用提交的生命周期是否合法，返回最终生效生命周期
   * @param queueName 队列名称
   * @param lifetime 应用提交的生命周期
   * @param app 应用对象
   * @return 最终生效的生命周期
   */
  @Public
  @Evolving
  long checkAndGetApplicationLifetime(String queueName, long lifetime,
                                      RMAppImpl app);

  /**
   * 获取指定队列允许的最大应用生命周期
   * @param queueName 队列名称
   * @return 最大生命周期（秒）
   */
  @Public
  @Evolving
  long getMaximumApplicationLifetime(String queueName);
}