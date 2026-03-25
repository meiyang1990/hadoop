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

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceUsage;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerHealth;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivitiesManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.preemption.PreemptionManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;

/**
 * 容量调度器队列上下文，存储队列公共依赖信息，包含必要管理器实例和全局调度配置。
 * 为所有队列提供统一的上下文访问入口，避免队列重复持有公共依赖引用。
 */
public class CapacitySchedulerQueueContext {

  // 核心管理器实例引用
  private final CapacitySchedulerContext csContext;
  private final CapacitySchedulerQueueManager queueManager;
  private final RMNodeLabelsManager labelManager;
  private final PreemptionManager preemptionManager;
  private final ActivitiesManager activitiesManager;
  private final ResourceCalculator resourceCalculator;

  // 容量调度器配置实例
  private CapacitySchedulerConfiguration configuration;

  // 最小容器资源分配量
  private Resource minimumAllocation;

  /**
   * 构造队列上下文，从调度器上下文初始化所有公共依赖。
   * @param csContext 容量调度器上下文
   */
  public CapacitySchedulerQueueContext(CapacitySchedulerContext csContext) {
    this.csContext = csContext;
    this.queueManager = csContext.getCapacitySchedulerQueueManager();
    this.labelManager = csContext.getRMContext().getNodeLabelManager();
    this.preemptionManager = csContext.getPreemptionManager();
    this.activitiesManager = csContext.getActivitiesManager();
    this.resourceCalculator = csContext.getResourceCalculator();

    this.configuration = new CapacitySchedulerConfiguration(csContext.getConfiguration());
    this.minimumAllocation = csContext.getMinimumResourceCapability();
  }

  /**
   * 重新初始化队列上下文配置，在配置动态刷新时调用。
   */
  public void reinitialize() {
    // 当csConfProvider.loadConfiguration调用后，useLocalConfigurationProvider已正确设置
    // 无需从capacity-scheduler.xml重复加载，因此第二个参数传false跳过重新加载
    this.configuration = new CapacitySchedulerConfiguration(csContext.getConfiguration(), false);
    this.minimumAllocation = csContext.getMinimumResourceCapability();
  }

  /**
   * 获取队列管理器实例。
   * @return 队列管理器
   */
  public CapacitySchedulerQueueManager getQueueManager() {
    return queueManager;
  }

  /**
   * 获取节点标签管理器实例。
   * @return 节点标签管理器
   */
  public RMNodeLabelsManager getLabelManager() {
    return labelManager;
  }

  /**
   * 获取抢占管理器实例。
   * @return 抢占管理器
   */
  public PreemptionManager getPreemptionManager() {
    return preemptionManager;
  }

  /**
   * 获取调度活动管理器实例。
   * @return 活动管理器
   */
  public ActivitiesManager getActivitiesManager() {
    return activitiesManager;
  }

  /**
   * 获取资源计算器实例。
   * @return 资源计算器
   */
  public ResourceCalculator getResourceCalculator() {
    return resourceCalculator;
  }

  /**
   * 获取容量调度器配置实例。
   * @return 调度配置
   */
  public CapacitySchedulerConfiguration getConfiguration() {
    return configuration;
  }

  /**
   * 设置单条配置项，支持动态配置修改。
   * @param name 配置项名称
   * @param value 配置项值
   */
  public void setConfigurationEntry(String name, String value) {
    this.configuration.set(name, value);
  }

  /**
   * 获取最小容器资源分配量。
   * @return 最小分配资源
   */
  public Resource getMinimumAllocation() {
    return minimumAllocation;
  }

  /**
   * 获取集群总资源量。
   * @return 集群总资源
   */
  public Resource getClusterResource() {
    return csContext.getClusterResource();
  }

  /**
   * 获取整个集群的资源使用情况。
   * @return 根队列资源使用统计（即整个集群资源使用）
   */
  public ResourceUsage getClusterResourceUsage() {
    return queueManager.getRootQueue().getQueueResourceUsage();
  }

  /**
   * 获取调度器健康状态。
   * @return 调度健康信息
   */
  public SchedulerHealth getSchedulerHealth() {
    return csContext.getSchedulerHealth();
  }

  /**
   * 获取最近一次节点更新时间戳。
   * @return 节点更新时间戳
   */
  public long getLastNodeUpdateTime() {
    return csContext.getLastNodeUpdateTime();
  }

  /**
   * 根据节点ID获取调度节点实例。
   * @param nodeId 节点ID
   * @return 调度节点实例
   */
  public FiCaSchedulerNode getNode(NodeId nodeId) {
    return csContext.getNode(nodeId);
  }

  /**
   * 根据应用尝试ID获取调度应用实例。
   * @param applicationAttemptId 应用尝试ID
   * @return 调度应用实例
   */
  public FiCaSchedulerApp getApplicationAttempt(
      ApplicationAttemptId applicationAttemptId) {
    return csContext.getApplicationAttempt(applicationAttemptId);
  }

  /**
   * 获取待处理应用排序比较器。
   * @return 待处理应用比较器
   */
  public CapacityScheduler.PendingApplicationComparator getApplicationComparator() {
    return csContext.getPendingApplicationComparator();
  }
}