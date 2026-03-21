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

package org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity;

import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.ProportionalCapacityPreemptionPolicy.IntraQueuePreemptionOrderPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * 容量调度器抢占计算上下文接口，为理想资源分配计算和抢占决策提供上下文环境与核心数据
 * 为比例容量抢占策略提供统一的数据访问接口，解耦抢占计算与调度器内部实现
 */
public interface CapacitySchedulerPreemptionContext {
  /**
   * 获取容量调度器实例
   * @return 容量调度器实例
   */
  CapacityScheduler getScheduler();

  /**
   * 根据队列名和节点分区获取对应临时队列数据
   * @param queueName 队列名称
   * @param partition 节点分区
   * @return 分区对应的临时队列数据
   */
  TempQueuePerPartition getQueueByPartition(String queueName,
      String partition);

  /**
   * 获取指定队列的所有分区临时队列数据
   * @param queueName 队列名称
   * @return 所有分区的临时队列集合
   */
  Collection<TempQueuePerPartition> getQueuePartitions(String queueName);

  /**
   * 获取资源计算器实例
   * @return 资源计算器
   */
  ResourceCalculator getResourceCalculator();

  /**
   * 获取RM上下文实例
   * @return ResourceManager上下文
   */
  RMContext getRMContext();

  /**
   * 获取是否仅观察不执行抢占标志
   * @return true表示仅计算不实际抢占容器，false表示执行抢占
   */
  boolean isObserveOnly();

  /**
   * 获取待杀死容器集合
   * @return 符合抢占条件可杀死的容器ID集合
   */
  Set<ContainerId> getKillableContainers();

  /**
   * 获取最大允许超额占用容量阈值
   * @return 最大忽略超额容量比例
   */
  double getMaxIgnoreOverCapacity();

  /**
   * 获取自然终止因子，优先等待自然完成而不强制抢占的系数
   * @return 自然终止因子
   */
  double getNaturalTerminationFactor();

  /**
   * 获取所有叶子队列名称集合
   * @return 叶子队列名称集合
   */
  Set<String> getLeafQueueNames();

  /**
   * 获取集群所有节点分区集合
   * @return 所有分区名称集合
   */
  Set<String> getAllPartitions();

  /**
   * 获取集群支持的最大应用优先级
   * @return 最大应用优先级值
   */
  int getClusterMaxApplicationPriority();

  /**
   * 获取指定分区的总资源量
   * @param partition 分区名称
   * @return 分区总资源
   */
  Resource getPartitionResource(String partition);

  /**
   * 获取指定分区中资源未满足的队列列表，按优先级排序
   * @param partition 分区名称
   * @return 未满足需求队列的有序集合
   */
  LinkedHashSet<String> getUnderServedQueuesPerPartition(String partition);

  /**
   * 将队列添加到对应分区的未满足队列列表中
   * @param queueName 队列名称
   * @param partition 分区名称
   */
  void addPartitionToUnderServedQueues(String queueName, String partition);

  /**
   * 获取队列内抢占的最小阈值
   * @return 队列内抢占最小资源满足比例阈值
   */
  float getMinimumThresholdForIntraQueuePreemption();

  /**
   * 获取队列内抢占的最大允许限制
   * @return 队列内抢占最大允许超额比例
   */
  float getMaxAllowableLimitForIntraQueuePreemption();

  /**
   * 获取抢占杀死容器的默认最大等待超时时间
   * @return 最大等待超时时间（毫秒）
   */
  long getDefaultMaximumKillWaitTimeout();

  /**
   * 获取队列内抢占容器的排序策略
   * @return 队列内抢占排序策略实例
   */
  @Unstable
  IntraQueuePreemptionOrderPolicy getIntraQueuePreemptionOrderPolicy();

  /**
   * 获取跨队列抢占是否启用保守DRF策略标志
   * @return true表示启用保守DRF，false不启用
   */
  boolean getCrossQueuePreemptionConservativeDRF();

  /**
   * 获取队列内抢占是否启用保守DRF策略标志
   * @return true表示启用保守DRF，false不启用
   */
  boolean getInQueuePreemptionConservativeDRF();
}