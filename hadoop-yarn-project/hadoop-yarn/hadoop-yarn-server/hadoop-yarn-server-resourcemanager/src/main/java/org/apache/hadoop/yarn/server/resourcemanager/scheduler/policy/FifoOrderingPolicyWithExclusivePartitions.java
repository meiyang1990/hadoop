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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;

/**
 * FIFO应用排序策略，支持为每个独占节点分区维护独立的排序队列。
 * 类似{@link FifoOrderingPolicy}，但会为
 * {@code yarn.scheduler.capacity.<queue-path>.ordering-policy.exclusive-enforced-partitions}
 * 配置中指定的每个分区维护独立的排序策略。
 */
public class FifoOrderingPolicyWithExclusivePartitions<S extends SchedulableEntity>
    implements OrderingPolicy<S> {

  // 默认分区名称，用于未配置的分区回退
  private static final String DEFAULT_PARTITION = "DEFAULT_PARTITION";

  // 分区名 -> 对应分区的排序策略 映射表
  private Map<String, OrderingPolicy<S>> orderingPolicies;

  /**
   * 构造函数，初始化默认分区排序策略。
   */
  public FifoOrderingPolicyWithExclusivePartitions() {
    this.orderingPolicies = new HashMap<>();
    this.orderingPolicies.put(DEFAULT_PARTITION, new FifoOrderingPolicy());
  }

  /**
   * 获取所有分区可调度实体的合集。
   * @return 所有可调度实体集合
   */
  public Collection<S> getSchedulableEntities() {
    return unionOrderingPolicies().getSchedulableEntities();
  }

  /**
   * 获取指定分区可调度实体的分配迭代器。
   * @param sel 迭代器选择器，包含目标分区信息
   * @return 指定分区的迭代器
   */
  public Iterator<S> getAssignmentIterator(IteratorSelector sel) {
    // 仅返回过滤后分区中的可调度实体
    return getPartitionOrderingPolicy(sel.getPartition())
        .getAssignmentIterator(sel);
  }

  /**
   * 获取所有分区可调度实体的抢占迭代器。
   * @return 所有分区可抢占实体的迭代器
   */
  public Iterator<S> getPreemptionIterator() {
    // 所有分区的实体都可以被抢占
    return unionOrderingPolicies().getPreemptionIterator();
  }

  /**
   * 合并所有分区的可调度实体，生成一个包含所有实体的合并排序策略。
   * @return 包含所有可调度实体的合并FIFO排序策略
   */
  private OrderingPolicy<S> unionOrderingPolicies() {
    OrderingPolicy<S> ret = new FifoOrderingPolicy<>();
    for (Map.Entry<String, OrderingPolicy<S>> entry
        : orderingPolicies.entrySet()) {
      ret.addAllSchedulableEntities(entry.getValue().getSchedulableEntities());
    }
    return ret;
  }

  /**
   * 添加可调度实体到对应分区队列。
   * @param s 待添加的可调度实体
   */
  public void addSchedulableEntity(S s) {
    getPartitionOrderingPolicy(s.getPartition()).addSchedulableEntity(s);
  }

  /**
   * 从对应分区队列移除可调度实体。
   * @param s 待移除的可调度实体
   * @return 移除是否成功
   */
  public boolean removeSchedulableEntity(S s) {
    return getPartitionOrderingPolicy(s.getPartition())
        .removeSchedulableEntity(s);
  }

  /**
   * 批量添加可调度实体，按所属分区分发到对应队列。
   * @param sc 待添加的可调度实体集合
   */
  public void addAllSchedulableEntities(Collection<S> sc) {
    for (S entity : sc) {
      getPartitionOrderingPolicy(entity.getPartition())
          .addSchedulableEntity(entity);
    }
  }

  /**
   * 获取所有分区可调度实体总数。
   * @return 可调度实体总数，保持和原有FIFO策略接口行为一致，用于队列应用数量限制检查
   */
  public int getNumSchedulableEntities() {
    // 返回所有分区可调度实体总数，保持与原有FifoOrderingPolicy接口行为一致
    // 例如用于检查队列是否达到最大应用数量限制
    int ret = 0;
    for (Map.Entry<String, OrderingPolicy<S>> entry
        : orderingPolicies.entrySet()) {
      ret += entry.getValue().getNumSchedulableEntities();
    }
    return ret;
  }

  /**
   * 通知对应分区容器已分配，更新可调度实体状态。
   * @param schedulableEntity 对应的可调度实体
   * @param r 已分配的RM容器
   */
  public void containerAllocated(S schedulableEntity, RMContainer r) {
    getPartitionOrderingPolicy(schedulableEntity.getPartition())
        .containerAllocated(schedulableEntity, r);
  }

  /**
   * 通知对应分区容器已释放，更新可调度实体状态。
   * @param schedulableEntity 对应的可调度实体
   * @param r 已释放的RM容器
   */
  public void containerReleased(S schedulableEntity, RMContainer r) {
    getPartitionOrderingPolicy(schedulableEntity.getPartition())
        .containerReleased(schedulableEntity, r);
  }

  /**
   * 通知对应分区可调度实体需求已更新。
   * @param schedulableEntity 需求更新的可调度实体
   */
  public void demandUpdated(S schedulableEntity) {
    getPartitionOrderingPolicy(schedulableEntity.getPartition())
        .demandUpdated(schedulableEntity);
  }

  @Override
  public void configure(Map<String, String> conf) {
    if (conf == null) {
      return;
    }
    // 从配置中获取需要单独处理的独占分区列表
    String partitions =
        conf.get(YarnConfiguration.EXCLUSIVE_ENFORCED_PARTITIONS_SUFFIX);
    if (partitions != null) {
      // 为每个独占分区创建独立的FIFO排序策略
      for (String partition : partitions.split(",")) {
        partition = partition.trim();
        if (!partition.isEmpty()) {
          this.orderingPolicies.put(partition, new FifoOrderingPolicy());
        }
      }
    }
  }

  @Override
  public String getInfo() {
    return "FifoOrderingPolicyWithExclusivePartitions";
  }

  @Override
  public String getConfigName() {
    return CapacitySchedulerConfiguration
        .FIFO_WITH_PARTITIONS_APP_ORDERING_POLICY;
  }

  /**
   * 获取指定分区对应的排序策略，如果分区不存在则返回默认分区策略。
   * @param partition 分区名称
   * @return 对应分区的排序策略
   */
  private OrderingPolicy<S> getPartitionOrderingPolicy(String partition) {
    String keyPartition = orderingPolicies.containsKey(partition) ?
        partition : DEFAULT_PARTITION;
    return orderingPolicies.get(keyPartition);
  }
}