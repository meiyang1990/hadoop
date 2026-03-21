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
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.security.PrivilegedEntity;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractUsersManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueResourceQuotas;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceLimits;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceUsage;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AbstractCSQueue.CapacityConfigType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.ResourceCommitRequest;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.CandidateNodeSet;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * CSQueue 是容量调度器 CapacityScheduler 中层次队列树结构的节点抽象接口，
 * 定义了容量调度队列需要实现的核心能力，包括容量管理、应用生命周期管理、容器分配回收、权限检查等。
 */
@Stable
@Private
public interface CSQueue extends SchedulerQueue<CSQueue> {
  /**
   * 获取当前队列的父队列
   * @return 父队列实例
   */
  public CSQueue getParent();

  /**
   * 设置当前队列的父队列
   * @param newParentQueue 新的父队列实例
   */
  public void setParent(CSQueue newParentQueue);

  /**
   * 获取队列的内部引用名称
   * @return 队列名称
   */
  public String getQueueName();

  /**
   * 获取队列的短名称（兼容旧版命名格式）
   * @return 队列短名称
   */
  String getQueueShortName();

  /**
   * 获取队列的完整路径，包含层级结构
   * @return 队列完整路径
   */
  public String getQueuePath();

  /**
   * 获取队列路径对象
   * @return 队列路径对象
   */
  QueuePath getQueuePathObject();

  /**
   * 检查当前队列是否为动态队列（自动队列创建v2动态生成的队列）
   * @return true表示是动态队列，false表示不是
   */
  boolean isDynamicQueue();

  /**
   * 获取队列的特权实体，用于权限认证
   * @return 特权实体实例
   */
  public PrivilegedEntity getPrivilegedEntity();

  /**
   * 获取队列允许的单容器最大分配资源
   * @return 最大分配资源
   */
  Resource getMaximumAllocation();

  /**
   * 获取队列允许的单容器最小分配资源
   * @return 最小分配资源
   */
  Resource getMinimumAllocation();

  /**
   * 获取队列配置的容量百分比
   * @return 队列配置容量
   */
  public float getCapacity();

  /**
   * 获取当前队列相对于整个集群的绝对容量百分比，由父队列容量逐级累积计算得到
   * @return 队列的绝对容量百分比
   */
  public float getAbsoluteCapacity();

  /**
   * 获取队列配置的最大容量百分比
   * @return 队列配置的最大容量百分比
   */
  public float getMaximumCapacity();
  
  /**
   * 获取当前队列相对于整个集群的绝对最大容量百分比，由父队列最大容量逐级累积计算得到
   * @return 队列的绝对最大容量百分比
   */
  public float getAbsoluteMaximumCapacity();
  
  /**
   * 获取当前队列相对于整个集群的已使用绝对容量百分比
   * @return 队列已使用绝对容量
   */
  public float getAbsoluteUsedCapacity();

  /**
   * 获取当前队列（含所有子队列）在无标签节点上的已使用容量百分比
   * @return 队列已使用容量
   */
  public float getUsedCapacity();

  /**
   * 获取当前队列（含所有子队列）在集群无标签节点上已占用的资源总量
   * 
   * @return 队列及子队列已使用资源总量
   */
  public Resource getUsedResources();
  
  /**
   * 获取队列当前运行状态
   * @return 当前运行状态
   */
  public QueueState getState();

  /**
   * 获取队列允许同时运行的最大应用数
   * @return 最大并行应用数
   */
  public int getMaxParallelApps();

  /**
   * 获取当前队列的所有子队列
   * @return 子队列列表
   */
  public List<CSQueue> getChildQueues();

  /**
   * 通过尝试加锁获取子队列列表，不会阻塞等待锁
   * @return 加锁成功则返回子队列列表，否则返回null
   */
  List<CSQueue> getChildQueuesByTryLock();
  
  /**
   * 检查用户是否对当前队列有指定ACL权限
   * @param acl 需要检查的权限类型
   * @param user 待检查的用户信息
   * @return true表示用户拥有权限，false表示没有
   */
  public boolean hasAccess(QueueACL acl, UserGroupInformation user);
  
  /**
   * 将新应用提交到当前队列，更新队列统计信息
   * @param applicationId 提交应用的ID
   * @param user 提交应用的用户名
   * @param queue 目标队列名称
   * @throws AccessControlException 权限检查失败时抛出
   */
  public void submitApplication(ApplicationId applicationId, String user,
      String queue) throws AccessControlException;

  /**
   * 将应用尝试提交到当前队列，更新调度统计信息
   *
   * @param application 提交的应用实例
   * @param userName 提交应用的用户名
   */
  public void submitApplicationAttempt(FiCaSchedulerApp application,
      String userName);

  /**
   * 将应用尝试提交到当前队列，支持应用跨队列迁移场景
   * @param application 提交的应用实例
   * @param userName 提交应用尝试的用户名
   * @param isMoveApp 是否为跨队列迁移应用
   */
  public void submitApplicationAttempt(FiCaSchedulerApp application,
      String userName, boolean isMoveApp);

  /**
   * 处理应用完成的清理工作，更新队列统计信息
   * @param applicationId 完成应用的ID
   * @param user 提交应用的用户名
   */
  public void finishApplication(ApplicationId applicationId, String user);

  /**
   * 处理应用尝试完成的清理工作，更新队列统计信息
   *
   * @param application 完成的应用尝试实例
   * @param queue 队列名称
   */
  public void finishApplicationAttempt(FiCaSchedulerApp application,
      String queue);

  /**
   * 为当前队列（含子队列）中的应用分配容器
   * @param clusterResource 集群总资源
   * @param candidates 当前调度轮次可用于分配的候选节点集合
   * @param resourceLimits 当前队列允许使用的资源上限
   * @param schedulingMode 容器分配的调度模式（独占/共享）
   * @return 容器分配结果
   */
  public CSAssignment assignContainers(Resource clusterResource,
      CandidateNodeSet<FiCaSchedulerNode> candidates,
      ResourceLimits resourceLimits, SchedulingMode schedulingMode);
  
  /**
   * 处理容器完成的清理工作，更新队列资源使用统计
   * @param clusterResource 集群总资源
   * @param application 容器所属应用
   * @param node 容器运行所在节点
   * @param container 已完成的容器，如果是取消预约则为null
   * @param containerStatus 完成容器的状态信息
   * @param childQueue 需要重新插入队列的子队列
   * @param event 需要发送给容器的事件
   * @param sortQueues 是否需要重新排序队列
   */
  public void completedContainer(Resource clusterResource,
      FiCaSchedulerApp application, FiCaSchedulerNode node, 
      RMContainer container, ContainerStatus containerStatus, 
      RMContainerEventType event, CSQueue childQueue,
      boolean sortQueues);

  /**
   * 获取队列中当前的应用总数
   * @return 应用数量
   */
  public int getNumApplications();

  
  /**
   * 重新初始化队列，应用配置变更
   * @param newlyParsedQueue 重新解析后的新队列配置
   * @param clusterResource 集群当前总资源
   * @throws IOException 初始化过程中IO异常时抛出
   */
  public void reinitialize(CSQueue newlyParsedQueue, Resource clusterResource)
  throws IOException;

  /**
   * 资源计算完成后刷新队列状态
   * @param clusterResource 集群当前总资源
   * @param resourceLimits 当前队列资源限制
   */
  public void refreshAfterResourceCalculation(
      Resource clusterResource, ResourceLimits resourceLimits);

   /**
   * 当集群节点增减时更新队列关联的集群资源信息
   * @param clusterResource 更新后的集群总资源
   * @param resourceLimits 更新后的资源限制
   */
  public void updateClusterResource(Resource clusterResource,
      ResourceLimits resourceLimits);
  
  /**
   * 获取当前队列的用户管理器
   * @return 当前队列的用户管理器实例
   */
  public AbstractUsersManager getAbstractUsersManager();
  
  /**
   * 将当前队列及所有子队列中的所有应用尝试收集到目标集合
   * @param apps 用于收集应用尝试ID的集合
   */
  public void collectSchedulerApplications(Collection<ApplicationAttemptId> apps);

  /**
   * 从当前队列分离容器（用于应用跨队列迁移），减少当前队列资源使用
   * @param clusterResource 集群当前总资源
   * @param application 容器所属应用
   * @param container 需要分离的容器
   */
  public void detachContainer(Resource clusterResource,
               FiCaSchedulerApp application, RMContainer container);

  /**
   * 将容器附加到当前队列（用于应用跨队列迁移），增加当前队列资源使用
   * @param clusterResource 集群当前总资源
   * @param application 容器所属应用
   * @param container 需要附加的容器
   */
  public void attachContainer(Resource clusterResource,
               FiCaSchedulerApp application, RMContainer container);

  /**
   * 检查当前队列是否禁用了抢占功能
   * @return true表示禁用抢占，false表示启用
   */
  public boolean getPreemptionDisabled();

  /**
   * 检查当前队列是否禁用了队列内抢占功能
   * @return true表示队列内抢占或队列间抢占任意一项禁用，false表示都启用
   */
  public boolean getIntraQueuePreemptionDisabled();

  /**
   * 检查当前队列层级中任意一级是否禁用了队列内抢占功能
   * @return 当前队列层级是否禁用了队列内抢占
   */
  public boolean getIntraQueuePreemptionDisabledInHierarchy();

  /**
   * 获取当前队列的容量信息对象
   * @return 队列容量信息
   */
  public QueueCapacities getQueueCapacities();
  
  /**
   * 获取当前队列的资源使用信息对象
   * @return 资源使用信息
   */
  public ResourceUsage getQueueResourceUsage();

  /**
   * 当节点分区变更时，增加队列对应分区的已使用资源统计
   *
   * @param nodePartition 节点分区标签
   * @param resourceToInc 需要增加的资源量
   * @param application 所属应用
   */
  public void incUsedResource(String nodePartition, Resource resourceToInc,
      SchedulerApplicationAttempt application);

  /**
   * 当节点分区变更时，减少队列对应分区的已使用资源统计
   *
   * @param nodePartition 节点分区标签
   * @param resourceToDec 需要减少的资源量
   * @param application 所属应用
   */
  public void decUsedResource(String nodePartition, Resource resourceToDec,
      SchedulerApplicationAttempt application);

  /**
   * 当待分配资源被满足或取消时，减少队列的待分配资源统计
   *
   * @param nodeLabel
   *          应用请求的节点标签
   * @param resourceToDec
   *          需要减少的资源量
   */
  public void decPendingResource(String nodeLabel, Resource resourceToDec);

  /**
   * 获取当前队列允许使用的节点标签集合
   * @return 允许的节点标签集合
   */
  public Set<String> getNodeLabelsForQueue();

  @VisibleForTesting
  CSAssignment assignContainers(Resource clusterResource,
      FiCaSchedulerNode node, ResourceLimits resourceLimits,
      SchedulingMode schedulingMode);

  /**
   * 检查当前队列是否可以接受指定的资源提交请求
   * @param cluster 集群总资源
   * @param request 资源提交请求
   * @return true表示可以接受，false表示不能
   */
  boolean accept(Resource cluster,
      ResourceCommitRequest<FiCaSchedulerApp, FiCaSchedulerNode> request);

  /**
   * 应用资源提交请求，更新队列资源统计
   * @param cluster 集群总资源
   * @param request 资源提交请求
   */
  void apply(Resource cluster,
      ResourceCommitRequest<FiCaSchedulerApp, FiCaSchedulerNode> request);

  /**
   * 获取队列关联的读锁，用于并发访问控制
   * @return 队列的读锁
   */
  public ReentrantReadWriteLock.ReadLock getReadLock();

  /**
   * 获取队列关联的写锁，用于并发访问控制
   * @return 队列的写锁
   */
  ReentrantReadWriteLock.WriteLock getWriteLock();

  /**
   * 在应用跨队列迁移前，验证目标队列是否接受该应用提交
   * @param applicationId 应用ID
   * @param userName 用户名
   * @param queue 目标队列名称
   * @throws AccessControlException 权限检查失败时抛出
   */
  public void validateSubmitApplication(ApplicationId applicationId,
      String userName, String queue) throws AccessControlException;

  /**
   * 获取队列优先级
   * @return 队列优先级
   */
  Priority getPriority();

  /**
   * 获取队列的用户权重配置对象
   * @return 用户权重对象
   */
  UserWeights getUserWeights();

  /**
   * 获取当前队列关联的资源配额对象
   * @return 队列资源配额
   */
  public QueueResourceQuotas getQueueResourceQuotas();

  /**
   * 获取容量配置类型：百分比模式或绝对资源模式
   * @return 容量配置类型
   */
  public CapacityConfigType getCapacityConfigType();

  /**
   * 获取队列指定分区的有效容量，若配置了绝对最小/最大资源，优先使用绝对配置
   *
   * @param label
   *          节点分区标签
   * @return 队列有效容量
   */
  Resource getEffectiveCapacity(String label);

  /**
   * 获取从队列容量配置解析得到的容量资源向量
   * @param label 节点分区标签
   * @return 容量资源向量
   */
  QueueCapacityVector getConfiguredCapacityVector(String label);

  /**
   * 获取从队列最大容量配置解析得到的容量资源向量
   * @param label 节点分区标签
   * @return 最大容量资源向量
   */
  QueueCapacityVector getConfiguredMaxCapacityVector(String label);

  /**
   * 设置指定分区的最小容量向量
   * @param label 节点分区标签
   * @param minCapacityVector 最小容量向量
   */
  void setConfiguredMinCapacityVector(String label, QueueCapacityVector minCapacityVector);

  /**
   * 设置指定分区的最大容量向量
   * @param label 节点分区标签
   * @param maxCapacityVector 最大容量向量
   */
  void setConfiguredMaxCapacityVector(String label, QueueCapacityVector maxCapacityVector);

  /**
   * 获取