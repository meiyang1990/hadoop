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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivitiesManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceUsage;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerHealth;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.preemption.PreemptionManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;

/**
 * 容量调度器上下文的只读接口，提供对CapacityScheduler核心上下文信息的访问能力
 */
public interface CapacitySchedulerContext {
  /**
   * 获取容量调度器配置对象
   * @return 容量调度器配置
   */
  CapacitySchedulerConfiguration getConfiguration();

  /**
   * 获取容量调度器队列上下文对象
   * @return 队列上下文
   */
  CapacitySchedulerQueueContext getQueueContext();
  
  /**
   * 获取容器最小资源能力
   * @return 最小资源配置
   */
  Resource getMinimumResourceCapability();

  /**
   * 获取容器最大资源能力（集群全局配置）
   * @return 最大资源配置
   */
  Resource getMaximumResourceCapability();

  /**
   * 获取指定队列允许的容器最大资源能力
   * @param queueName 队列名称
   * @return 指定队列的最大资源配置
   */
  Resource getMaximumResourceCapability(String queueName);

  /**
   * 获取RM容器令牌密钥管理器
   * @return 容器令牌密钥管理器
   */
  RMContainerTokenSecretManager getContainerTokenSecretManager();
  
  /**
   * 获取集群节点数量
   * @return 集群节点总数
   */
  int getNumClusterNodes();

  /**
   * 获取YARN RM上下文对象
   * @return RM上下文
   */
  RMContext getRMContext();
  
  /**
   * 获取集群总资源量
   * @return 集群总资源
   */
  Resource getClusterResource();

  /**
   * 获取YARN全局配置对象
   * @return yarn configuration.
   */
  Configuration getConf();

  /**
   * 获取资源计算器
   * @return 资源计算器实例
   */
  ResourceCalculator getResourceCalculator();
  
  /**
   * 根据节点ID获取调度节点对象
   * @param nodeId 节点ID
   * @return 调度节点实例
   */
  FiCaSchedulerNode getNode(NodeId nodeId);

  /**
   * 根据应用尝试ID获取调度应用对象
   * @param attemptId 应用尝试ID
   * @return 调度应用实例
   */
  FiCaSchedulerApp getApplicationAttempt(ApplicationAttemptId attemptId);

  /**
   * 获取抢占管理器实例
   * @return 抢占管理器
   */
  PreemptionManager getPreemptionManager();

  /**
   * 获取调度器健康状态对象
   * @return 调度器健康状态
   */
  SchedulerHealth getSchedulerHealth();

  /**
   * 获取最后一次节点更新时间戳
   * @return 最后节点更新时间
   */
  long getLastNodeUpdateTime();

  /**
   * 获取集群根队列资源使用情况，根队列各标签的资源用量与集群整体保持一致
   * @return 集群资源使用情况
   */
  ResourceUsage getClusterResourceUsage();

  /**
   * 获取调度活动管理器
   * @return 活动管理器实例
   */
  ActivitiesManager getActivitiesManager();

  /**
   * 获取容量调度器队列管理器
   * @return 队列管理器实例
   */
  CapacitySchedulerQueueManager getCapacitySchedulerQueueManager();

  /**
   * 获取集群级别最大应用优先级
   * @return 集群最大应用优先级
   */
  Priority getMaxClusterLevelAppPriority();

  /**
   * 返回配置是否支持动态修改
   * @return true表示配置可动态修改，false表示不可修改
   */
  boolean isConfigurationMutable();

  /**
   * 从调度器获取时钟实例
   * @return 时钟实例
   */
  Clock getClock();

  /**
   * 获取待处理应用比较器，用于排序等待分配的应用
   * @return 待处理应用比较器
   */
  CapacityScheduler.PendingApplicationComparator getPendingApplicationComparator();
}