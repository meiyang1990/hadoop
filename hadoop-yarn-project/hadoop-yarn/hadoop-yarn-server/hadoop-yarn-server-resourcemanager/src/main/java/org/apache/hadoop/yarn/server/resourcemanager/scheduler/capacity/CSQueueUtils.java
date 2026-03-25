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

import java.util.Set;

import org.apache.hadoop.util.Sets;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceUsage;
import org.apache.hadoop.yarn.server.utils.Lock;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

/**
 * 容量调度器队列工具类，提供队列容量计算、配置加载、统计更新等通用工具能力
 */
public class CSQueueUtils {

  /** 浮点比较容差，处理精度误差 */
  public final static float EPSILON = 0.001f;

  /**
   * 校验队列最大容量合法性，仅用于测试
   * @param queuePath 队列路径
   * @param capacity 队列容量
   * @param maximumCapacity 队列最大容量
   */
  /*
   * Used only by tests
   */
  public static void checkMaxCapacity(QueuePath queuePath,
      float capacity, float maximumCapacity) {
    if (maximumCapacity < 0.0f || maximumCapacity > 1.0f) {
      throw new IllegalArgumentException(
          "Illegal value  of maximumCapacity " + maximumCapacity +
          " used in call to setMaxCapacity for queue " + queuePath);
    }
    }

  /**
   * 校验队列绝对容量合法性，仅用于测试
   * @param queuePath 队列路径
   * @param absCapacity 队列绝对容量
   * @param absMaxCapacity 队列绝对最大容量
   */
  /*
   * Used only by tests
   */
  public static void checkAbsoluteCapacity(QueuePath queuePath,
      float absCapacity, float absMaxCapacity) {
    if (absMaxCapacity < (absCapacity - EPSILON)) {
      throw new IllegalArgumentException("Illegal call to setMaxCapacity. "
          + "Queue '" + queuePath + "' has "
          + "an absolute capacity (" + absCapacity
          + ") greater than its absolute maximumCapacity (" + absMaxCapacity
          + ")");
  }
  }

  /**
   * 计算队列绝对最大容量，基于父队列绝对最大容量与自身最大容量比例
   * @param maximumCapacity 自身最大容量比例
   * @param parent 父队列
   * @return 计算得到的绝对最大容量
   */
  public static float computeAbsoluteMaximumCapacity(
      float maximumCapacity, CSQueue parent) {
    float parentAbsMaxCapacity =
        (parent == null) ? 1.0f : parent.getAbsoluteMaximumCapacity();
    return (parentAbsMaxCapacity * maximumCapacity);
  }

  /**
   * 从配置文件加载各节点标签对应的队列容量配置
   * @param queuePath 队列路径
   * @param queueCapacities 队列容量存储对象
   * @param csConf 容量调度器配置
   * @param nodeLabels 节点标签集合
   */
  public static void loadCapacitiesByLabelsFromConf(
      QueuePath queuePath, QueueCapacities queueCapacities,
      CapacitySchedulerConfiguration csConf, Set<String> nodeLabels) {
    // 清空原有可配置字段
    queueCapacities.clearConfigurableFields();

    // 遍历所有节点标签加载配置
    for (String label : nodeLabels) {
      // 处理无标签的默认分区
      if (label.equals(CommonNodeLabelsManager.NO_LABEL)) {
        queueCapacities.setCapacity(label,
            csConf.getNonLabeledQueueCapacity(queuePath) / 100);
        queueCapacities.setMaximumCapacity(label,
            csConf.getNonLabeledQueueMaximumCapacity(queuePath) / 100);
        queueCapacities.setMaxAMResourcePercentage(
            label,
            csConf.getMaximumAMResourcePercentPerPartition(queuePath, label));
        queueCapacities.setWeight(label,
            csConf.getNonLabeledQueueWeight(queuePath));
      } else{
        // 处理带标签的分区
        queueCapacities.setCapacity(label,
            csConf.getLabeledQueueCapacity(queuePath, label) / 100);
        queueCapacities.setMaximumCapacity(label,
            csConf.getLabeledQueueMaximumCapacity(queuePath, label) / 100);
        queueCapacities.setMaxAMResourcePercentage(label,
            csConf.getMaximumAMResourcePercentPerPartition(queuePath, label));
        queueCapacities.setWeight(label,
            csConf.getLabeledQueueWeight(queuePath, label));
      }
    }
  }

  /**
   * Update partitioned resource usage, if nodePartition == null, will update
   * used resource for all partitions of this queue.
   *
   * @param rc resource calculator.
   * @param totalPartitionResource total Partition Resource.
   * @param nodePartition node label.
   * @param childQueue child queue.
   */
  public static void updateUsedCapacity(final ResourceCalculator rc,
      final Resource totalPartitionResource, String nodePartition,
      AbstractCSQueue childQueue) {
    QueueCapacities queueCapacities = childQueue.getQueueCapacities();
    CSQueueMetrics queueMetrics = childQueue.getMetrics();
    ResourceUsage queueResourceUsage = childQueue.getQueueResourceUsage();
    Resource minimumAllocation = childQueue.getMinimumAllocation();
    float absoluteUsedCapacity = 0.0f;
    float usedCapacity = 0.0f;
    float reservedCapacity = 0.0f;
    float absoluteReservedCapacity = 0.0f;

    // 仅当分区总资源大于0时才计算
    if (Resources.greaterThan(rc, totalPartitionResource,
        totalPartitionResource, Resources.none())) {

      Resource queueGuaranteedResource = childQueue
          .getEffectiveCapacity(nodePartition);

      //TODO : Modify below code to support Absolute Resource configurations
      // (YARN-5881) for AutoCreatedLeafQueue
      // 处理自动创建叶子队列绝对容量为0的特殊情况，使用父队列模板容量计算保证资源
      if (Float.compare(queueCapacities.getAbsoluteCapacity
              (nodePartition), 0f) == 0
          && childQueue instanceof AutoCreatedLeafQueue) {

        //If absolute capacity is 0 for a leaf queue (could be a managed leaf
        // queue, then use the leaf queue's template capacity to compute
        // guaranteed resource for used capacity)

        // queueGuaranteed = totalPartitionedResource *
        // absolute_capacity(partition)
        ManagedParentQueue parentQueue = (ManagedParentQueue)
            childQueue.getParent();
        QueueCapacities leafQueueTemplateCapacities = parentQueue
            .getLeafQueueTemplate()
            .getQueueCapacities();
        queueGuaranteedResource = Resources.multiply(totalPartitionResource,
            leafQueueTemplateCapacities.getAbsoluteCapacity
                (nodePartition));
      }

      // 确保保证资源不小于最小分配，避免除零错误
      queueGuaranteedResource =
          Resources.max(rc, totalPartitionResource, queueGuaranteedResource,
              minimumAllocation);

      // 获取已使用资源计算绝对已用容量和相对已用容量
      Resource usedResource = queueResourceUsage.getUsed(nodePartition);
      absoluteUsedCapacity =
          Resources.divide(rc, totalPartitionResource, usedResource,
              totalPartitionResource);
      usedCapacity =
          Resources.divide(rc, totalPartitionResource, usedResource,
              queueGuaranteedResource);

      // 获取预留资源计算预留容量
      Resource resResource = queueResourceUsage.getReserved(nodePartition);
      reservedCapacity =
          Resources.divide(rc, totalPartitionResource, resResource,
              queueGuaranteedResource);
      absoluteReservedCapacity =
          Resources.divide(rc, totalPartitionResource, resResource,
              totalPartitionResource);
    }

    // 更新容量统计到队列容量对象
    queueCapacities
        .setAbsoluteUsedCapacity(nodePartition, absoluteUsedCapacity);
    queueCapacities.setUsedCapacity(nodePartition, usedCapacity);
    queueCapacities.setReservedCapacity(nodePartition, reservedCapacity);
    queueCapacities
        .setAbsoluteReservedCapacity(nodePartition, absoluteReservedCapacity);

    // QueueMetrics does not support per-label capacities,
    // so we report values only for the default partition.

    // 仅默认分区更新队列指标，队列指标暂不支持按标签区分
    queueMetrics.setUsedCapacity(nodePartition,
        queueCapacities.getUsedCapacity(RMNodeLabelsManager.NO_LABEL));
    queueMetrics.setAbsoluteUsedCapacity(nodePartition,
        queueCapacities.getAbsoluteUsedCapacity(
            RMNodeLabelsManager.NO_LABEL));

  }

  /**
   * 计算队列特定标签分区可用的最大资源
   * @param rc 资源计算器
   * @param queue 队列
   * @param cluster 集群总资源
   * @param partition 分区标签
   * @return 可用最大资源
   */
  private static Resource getMaxAvailableResourceToQueuePartition(
      final ResourceCalculator rc, CSQueue queue,
      Resource cluster, String partition) {
    // Calculate guaranteed resource for a label in a queue by below logic.
    // (total label resource) * (absolute capacity of label in that queue)
    Resource queueGuaranteedResource = queue.getEffectiveCapacity(partition);

    // Available resource in queue for a specific label will be calculated as
    // {(guaranteed resource for a label in a queue) -
    // (resource usage of that label in the queue)}
    Resource available = (Resources.greaterThan(rc, cluster,
        queueGuaranteedResource,
        queue.getQueueResourceUsage().getUsed(partition))) ? Resources
        .componentwiseMax(Resources.subtractFrom(queueGuaranteedResource,
            queue.getQueueResourceUsage().getUsed(partition)), Resources
            .none()) : Resources.none();

    return available;
  }

  /**
   * <p>
   * Update Queue Statistics:
   * </p>
   *
   * <ul>
   *   <li>used-capacity/absolute-used-capacity by partition</li>
   *   <li>non-partitioned max-avail-resource to queue</li>
   * </ul>
   *
   * <p>
   * When nodePartition is null, all partition of
   * used-capacity/absolute-used-capacity will be updated.
   * </p>
   *
   * @param rc resource calculator.
   * @param cluster cluster resource.
   * @param childQueue child queue.
   * @param nlm RMNodeLabelsManager.
   * @param nodePartition node label.
   */
  @Lock(CSQueue.class)
  public static void updateQueueStatistics(
      final ResourceCalculator rc, final Resource cluster,
      final AbstractCSQueue childQueue, final RMNodeLabelsManager nlm,
      final String nodePartition) {
    QueueCapacities queueCapacities = childQueue.getQueueCapacities();
    ResourceUsage queueResourceUsage = childQueue.getQueueResourceUsage();

    // 未指定分区，更新所有存在的分区统计信息
    if (nodePartition == null) {
      for (String partition : Sets.union(queueCapacities.getExistingNodeLabels(),
          queueResourceUsage.getExistingNodeLabels())) {
        // 更新已用容量
        updateUsedCapacity(rc, nlm.getResourceByLabel(partition, cluster),
            partition, childQueue);

        // Update queue metrics w.r.t node labels.
        // In QueueMetrics, null label is handled the same as NO_LABEL.
        // This is because queue metrics for partitions are not tracked.
        // In the future, will have to change this when/if queue metrics
        // for partitions also get tracked.
        // 更新队列可用资源指标
        childQueue.getMetrics().setAvailableResourcesToQueue(
            partition,
            getMaxAvailableResourceToQueuePartition(rc, childQueue,
                cluster, partition));
      }
    } else {
      // 指定分区，仅更新该分区统计信息
      updateUsedCapacity(rc, nlm.getResourceByLabel(nodePartition, cluster),
          nodePartition, childQueue);

      // Same as above.
      // 更新该分区可用资源指标
      childQueue.getMetrics().setAvailableResourcesToQueue(
          nodePartition,
          getMaxAvailableResourceToQueuePartition(rc, childQueue,
              cluster, nodePartition));
    }
   }

  /**
   * Updated configured capacity/max-capacity for queue.
   * @param rc resource calculator
   * @param partitionResource total cluster resources for this partition
   * @param partition partition being updated
   * @param queue queue
   */
   public static void updateConfiguredCapacityMetrics(ResourceCalculator rc,
       Resource partitionResource, String partition, AbstractCSQueue queue) {
     // 更新保证资源绝对值指标
     queue.getMetrics().setGuaranteedResources(partition, rc.multiplyAndNormalizeDown(
         partitionResource, queue.getQueueCapacities().getAbsoluteCapacity(partition),
         queue.getMinimumAllocation()));
     // 更新最大资源绝对值指标
     queue.getMetrics().setMaxCapacityResources(partition, rc.multiplyAndNormalizeDown(
         partitionResource, queue.getQueueCapacities().getAbsoluteMaximumCapacity(partition),
         queue.getMinimumAllocation()));
     // 更新容量比例指标
    queue.getMetrics().setGuaranteedCapacities(partition,
        queue.getQueueCapacities().getCapacity(partition),
        queue.getQueueCapacities().getAbsoluteCapacity(partition));
    queue.getMetrics().setMaxCapacities(partition,
        queue.getQueueCapacities().getMaximumCapacity(partition),
        queue.getQueueCapacities().getAbsoluteMaximumCapacity(partition));
   }

  /**
   * 批量更新所有节点标签分区的绝对容量，基于父队列绝对容量计算
   * @param queueCapacities 当前队列容量存储
   * @param parentQueueCapacities 父队列容量存储
   * @param nodeLabels 节点标签集合
   * @param isLegacyQueueMode 是否为传统队列模式
   */
  public static void updateAbsoluteCapacitiesByNodeLabels(QueueCapacities queueCapacities,
                                                          QueueCapacities parentQueueCapacities,
                                                          Set<String> nodeLabels,
                                                          boolean isLegacyQueueMode) {
    // 遍历所有标签分区更新绝对容量
    for (String label : nodeLabels) {
      if (isLegacyQueueMode) {
        // 传统模式下同时兼容容量和权重配置，取较大值计算绝对容量
        // Weight will be normalized to queue.weight =
        //      queue.weight(sum({sibling-queues.weight}))
        // When weight is set, capacity will be set to 0;
        // When capacity is set, weight will be normalized to 0,
        // So get larger from normalized_weight and capacity will make sure we do
        // calculation correct
        float capacity = Math.max(
            queueCapacities.getCapacity(label),
            queueCapacities
                .getNormalizedWeight(label));

        if (capacity > 0f) {
          // 绝对容量 = 自身比例 * 父队列绝对容量
          queueCapacities.setAbsoluteCapacity(label, capacity * (
              parentQueueCapacities == null ? 1 :
                  parentQueueCapacities.getAbsoluteCapacity(label)));
        }
      } else {
        // 新模式直接使用自身容量比例计算
        queueCapacities.setAbsoluteCapacity(label, queueCapacities.getCapacity(label) * (
            parentQueueCapacities == null ? 1 :
                parentQueueCapacities.getAbsoluteCapacity(label)));
      }

      // 计算绝对最大容量，逻辑同绝对容量
      float maxCapacity = queueCapacities
          .getMaximumCapacity(label);
      if (maxCapacity > 0f) {
        queueCapacities.setAbsoluteMaximumCapacity(label, maxCapacity * (
            parentQueueCapacities == null ? 1 :
                parentQueueCapacities.getAbsoluteMaximumCapacity(label)));
      }
    }
  }
}