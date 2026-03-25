// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.QueueCapacityVectorEntry;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.ResourceUnitCapacityType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueUpdateWarning.QueueUpdateWarningType;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.yarn.api.records.ResourceInformation.MEMORY_URI;

/**
 * 容量调度器队列资源计算驱动类，负责驱动父队列下所有子队列的资源分配计算，
 * 维护整个计算过程中的剩余资源等中间状态数据，供所有子队列计算使用。
 */
public class ResourceCalculationDriver {
  /** 资源计算优先级顺序：先计算绝对容量，再百分比，最后权重 */
  private static final ResourceUnitCapacityType[] CALCULATOR_PRECEDENCE =
      new ResourceUnitCapacityType[] {
          ResourceUnitCapacityType.ABSOLUTE,
          ResourceUnitCapacityType.PERCENTAGE,
          ResourceUnitCapacityType.WEIGHT};
  static final String MB_UNIT = "Mi";

  protected final QueueResourceRoundingStrategy roundingStrategy =
      new DefaultQueueResourceRoundingStrategy(CALCULATOR_PRECEDENCE);
  /** 当前计算驱动所属的父队列 */
  protected final CSQueue queue;
  /** 队列容量更新上下文，保存整个更新阶段的全局状态 */
  protected final QueueCapacityUpdateContext updateContext;
  /** 不同容量类型对应的计算器实例映射 */
  protected final Map<ResourceUnitCapacityType, AbstractQueueCapacityCalculator> calculators;
  /** 当前集群定义的所有资源类型列表 */
  protected final Collection<String> definedResources;

  /** 按节点标签划分的总剩余可分配资源，计算过程中逐步递减 */
  protected final Map<String, ResourceVector> overallRemainingResourcePerLabel = new HashMap<>();
  /** 按节点标签划分的当前批处理剩余可分配资源，每类容量计算完成后批量更新 */
  protected final Map<String, ResourceVector> batchRemainingResourcePerLabel = new HashMap<>();
  /** 归一化资源比例，按节点标签划分，用于绝对容量类型计算 */
  protected final Map<String, ResourceVector> normalizedResourceRatioPerLabel = new HashMap<>();
  /** 子队列权重总和，按节点标签、资源类型划分，用于权重容量类型计算 */
  protected final Map<String, Map<String, Double>> sumWeightsPerLabel = new HashMap<>();
  /** 当前计算器按标签统计的已使用资源量 */
  protected Map<String, Double> usedResourceByCurrentCalculatorPerLabel = new HashMap<>();

  /**
   * 构造资源计算驱动实例。
   *
   * @param queue 父队列，当前计算的根队列
   * @param updateContext 队列容量更新上下文
   * @param calculators 容量类型到计算器的映射
   * @param definedResources 集群已定义资源列表
   */
  public ResourceCalculationDriver(
      CSQueue queue, QueueCapacityUpdateContext updateContext,
      Map<ResourceUnitCapacityType, AbstractQueueCapacityCalculator> calculators,
      Collection<String> definedResources) {
    this.queue = queue;
    this.updateContext = updateContext;
    this.calculators = calculators;
    this.definedResources = definedResources;
  }


  /**
   * 获取当前计算驱动所属的父队列。
   *
   * @return 公共父队列
   */
  public CSQueue getQueue() {
    return queue;
  }

  /**
   * 获取父队列下所有子队列。
   *
   * @return 子队列集合
   */
  public Collection<CSQueue> getChildQueues() {
    return queue.getChildQueues();
  }

  /**
   * 获取整个更新阶段使用的上下文对象。
   *
   * @return 更新上下文
   */
  public QueueCapacityUpdateContext getUpdateContext() {
    return updateContext;
  }

  /**
   * 累加权重总和。
   *
   * @param label 节点标签
   * @param resourceName 资源名称
   * @param value 权重增量
   */
  public void incrementWeight(String label, String resourceName, double value) {
    sumWeightsPerLabel.putIfAbsent(label, new HashMap<>());
    sumWeightsPerLabel.get(label).put(resourceName,
        sumWeightsPerLabel.get(label).getOrDefault(resourceName, 0d) + value);
  }

  /**
   * 获取对应标签和资源的子队列权重总和。
   *
   * @param label 节点标签
   * @param resourceName 资源名称
   * @return 子队列权重总和
   */
  public double getSumWeightsByResource(String label, String resourceName) {
    return sumWeightsPerLabel.get(label).get(resourceName);
  }

  /**
   * 获取所有标签的归一化资源比例。
   * 归一化比例 = 子队列绝对容量总和 / 父队列有效最小资源
   *
   * @return 按标签分组的归一化资源比例
   */
  public Map<String, ResourceVector> getNormalizedResourceRatios() {
    return normalizedResourceRatioPerLabel;
  }

  /**
   * 获取父队列下对应资源的剩余比例。
   *
   * @param label 节点标签
   * @param resourceName 资源名称
   * @return 剩余资源占父队列总容量的比例
   */
  public double getRemainingRatioOfResource(String label, String resourceName) {
    return batchRemainingResourcePerLabel.get(label).getValue(resourceName)
        / queue.getEffectiveCapacity(label).getResourceValue(resourceName);
  }

  /**
   * 获取父队列最小绝对容量占集群总资源的比例。
   *
   * @param label 节点标签
   * @param resourceName 资源名称
   * @return 最小容量占比
   */
  public double getParentAbsoluteMinCapacity(String label, String resourceName) {
    return (double) queue.getEffectiveCapacity(label).getResourceValue(resourceName)
        / getUpdateContext().getUpdatedClusterResource(label).getResourceValue(resourceName);
  }

  /**
   * 获取父队列最大绝对容量占集群总资源的比例。
   *
   * @param label 节点标签
   * @param resourceName 资源名称
   * @return 最大容量占比
   */
  public double getParentAbsoluteMaxCapacity(String label, String resourceName) {
    return (double) queue.getEffectiveMaxCapacity(label).getResourceValue(resourceName)
        / getUpdateContext().getUpdatedClusterResource(label).getResourceValue(resourceName);
  }

  /**
   * 获取父队列对应标签的批处理剩余可分配资源，不存在则初始化空实例。
   * 仅当当前容量类型计算完成后才批量递减剩余资源。
   *
   * @param label 节点标签
   * @return 剩余资源向量
   */
  public ResourceVector getBatchRemainingResource(String label) {
    batchRemainingResourcePerLabel.putIfAbsent(label, ResourceVector.newInstance());
    return batchRemainingResourcePerLabel.get(label);
  }

  /**
   * 计算并设置当前父队列下所有子队列的最小和最大有效资源。
   * 按照优先级计算不同容量类型，完成后进行结果校验。
   */
  public void calculateResources() {
    // 初始化总剩余和批处理剩余资源，初始值为父队列有效总容量
    for (String label : queue.getConfiguredNodeLabels()) {
      overallRemainingResourcePerLabel.put(label,
          ResourceVector.of(queue.getEffectiveCapacity(label)));
      batchRemainingResourcePerLabel.put(label,
          ResourceVector.of(queue.getEffectiveCapacity(label)));
    }

    // 执行所有计算器的前置准备工作
    for (AbstractQueueCapacityCalculator capacityCalculator : calculators.values()) {
      capacityCalculator.calculateResourcePrerequisites(this);
    }

    // 遍历所有资源类型，按优先级计算不同容量类型的子队列资源
    for (String resourceName : definedResources) {
      for (ResourceUnitCapacityType capacityType : CALCULATOR_PRECEDENCE) {
        // 遍历所有子队列计算当前资源当前容量类型的资源分配
        for (CSQueue childQueue : getChildQueues()) {
          CalculationContext context = new CalculationContext(resourceName, capacityType,
              childQueue);
          calculateResourceOnChild(context);
        }

        // 当前容量类型计算完成，批量更新批处理剩余资源
        for (Map.Entry<String, Double> entry : usedResourceByCurrentCalculatorPerLabel.entrySet()) {
          batchRemainingResourcePerLabel.get(entry.getKey()).decrement(resourceName,
              entry.getValue());
        }

        // 重置当前计算器已使用资源统计，为下一个容量类型计算做准备
        usedResourceByCurrentCalculatorPerLabel = new HashMap<>();
      }
    }

    // 校验计算完成后的剩余资源，记录未完全分配警告
    validateRemainingResource();
  }

  /**
   * 对单个子队列计算指定资源和容量类型的资源分配。
   *
   * @param context 计算上下文
   */
  private void calculateResourceOnChild(CalculationContext context) {
    // 加写锁保证子队列资源修改安全
    context.getQueue().getWriteLock().lock();
    try {
      // 遍历子队列所有配置的节点标签，计算对应资源
      for (String label : context.getQueue().getConfiguredNodeLabels()) {
        // 跳过当前资源不是当前容量类型的情况
        if (!context.getQueue().getConfiguredCapacityVector(label).isResourceOfType(
            context.getResourceName(), context.getCapacityType())) {
          continue;
        }

        // 跳过总剩余资源中不存在该标签的情况
        if (!overallRemainingResourcePerLabel.containsKey(label)) {
          continue;
        }

        // 设置子队列资源，返回该子队列使用的资源量
        double usedResourceByChild = setChildResources(context, label);
        // 累加当前标签的已使用资源
        double aggregatedUsedResource = usedResourceByCurrentCalculatorPerLabel.getOrDefault(label,
            0d);
        double resourceUsedByLabel = aggregatedUsedResource + usedResourceByChild;

        // 递减总剩余资源
        overallRemainingResourcePerLabel.get(label).decrement(context.getResourceName(),
            usedResourceByChild);
        // 更新当前计算器已使用资源统计
        usedResourceByCurrentCalculatorPerLabel.put(label, resourceUsedByLabel);
      }
    } finally {
      // 释放写锁
      context.getQueue().getWriteLock().unlock();
    }
  }

  /**
   * 计算并设置子队列的最小和最大资源值。
   *
   * @param context 计算上下文
   * @param label 节点标签
   * @return 该子队列分配到的最小资源量
   */
  private double setChildResources(CalculationContext context, String label) {
    // 获取最小和最大容量的配置入口
    QueueCapacityVectorEntry capacityVectorEntry = context.getQueue().getConfiguredCapacityVector(
        label).getResource(context.getResourceName());
    QueueCapacityVectorEntry maximumCapacityVectorEntry = context.getQueue()
        .getConfiguredMaxCapacityVector(label).getResource(context.getResourceName());
    // 获取最大容量对应的计算器
    AbstractQueueCapacityCalculator maximumCapacityCalculator = calculators.get(
        maximumCapacityVectorEntry.getVectorResourceType());

    // 分别计算最小和最大资源值
    double minimumResource =
        calculators.get(context.getCapacityType()).calculateMinimumResource(this, context, label);
    double maximumResource = maximumCapacityCalculator.calculateMaximumResource(this, context,
        label);

    // 对计算结果进行舍入处理，符合资源单位要求
    minimumResource = roundingStrategy.getRoundedResource(minimumResource, capacityVectorEntry);
    maximumResource = roundingStrategy.getRoundedResource(maximumResource,
        maximumCapacityVectorEntry);
    // 校验并修正计算结果，处理资源越界等异常情况
    Pair<Double, Double> resources = validateCalculatedResources(context, label,
        new ImmutablePair<>(
        minimumResource, maximumResource));
    minimumResource = resources.getLeft();
    maximumResource = resources.getRight();

    // 将计算结果写入子队列的有效配额中
    context.getQueue().getQueueResourceQuotas().getEffectiveMinResource(label).setResourceValue(
        context.getResourceName(), (long) minimumResource);
    context.getQueue().getQueueResourceQuotas().getEffectiveMaxResource(label).setResourceValue(
        context.getResourceName(), (long) maximumResource);

    return minimumResource;
  }

  /**
   * 校验计算得到的最小和最大资源，处理越界等异常情况，记录警告信息。
   *
   * @param context 计算上下文
   * @param label 节点标签
   * @param calculatedResources 计算得到的最小、最大资源对
   * @return 修正后的资源对
   */
  private Pair<Double, Double> validateCalculatedResources(CalculationContext context,
      String label, Pair<Double, Double> calculatedResources) {
    double minimumResource = calculatedResources.getLeft();
    long minimumMemoryResource =
        context.getQueue().getQueueResourceQuotas().getEffectiveMinResource(label).getMemorySize();

    double remainingResourceUnderParent = overallRemainingResourcePerLabel.get(label).getValue(
        context.getResourceName());

    long parentMaximumResource = queue.getEffectiveMaxCapacity(label).getResourceValue(
        context.getResourceName());
    double maximumResource = calculatedResources.getRight();

    // 内存作为主资源，如果内存为0，其他资源也必须为0
    if (!context.getResourceName().equals(MEMORY_URI) && minimumMemoryResource == 0) {
      minimumResource = 0;
    }

    // 最大资源超过父队列最大资源，记录警告
    if (maximumResource != 0 && maximumResource > parentMaximumResource) {
      updateContext.addUpdateWarning(QueueUpdateWarningType.QUEUE_MAX_RESOURCE_EXCEEDS_PARENT
          .ofQueue(context.getQueue().getQueuePath()));
    }
    // 最大资源不超过父队列最大资源，为0则直接使用父队列最大资源
    maximumResource = maximumResource == 0 ? parentMaximumResource : Math.min(maximumResource,
        parentMaximumResource);

    // 最小资源超过最大资源，记录警告并将最小资源修正为最大资源
    if (maximumResource < minimumResource) {
      updateContext.addUpdateWarning(QueueUpdateWarningType.QUEUE_EXCEEDS_MAX_RESOURCE.ofQueue(
          context.getQueue().getQueuePath()));
      minimumResource = maximumResource;
    }

    // 最小资源超过父队列剩余资源
    if (minimumResource > remainingResourceUnderParent) {
      // 自动管理父队列的自动队列，剩余不足时分配0资源
      if (queue instanceof ManagedParentQueue) {
        minimumResource = 0;
      } else {
        // 其他队列记录警告，将最小资源修正为剩余资源
        updateContext.addUpdateWarning(
            QueueUpdateWarningType.QUEUE_OVERUTILIZED.ofQueue(
                context.getQueue().getQueuePath()).withInfo(
                    "Resource name: " + context.getResourceName() +
                        " resource value: " + minimumResource));
        minimumResource = remainingResourceUnderParent;
      }
    }

    // 最小资源为0，记录警告
    if (minimumResource == 0) {
      updateContext.addUpdateWarning(QueueUpdateWarningType.QUEUE_ZERO_RESOURCE.ofQueue(
          context.getQueue().getQueuePath())
          .withInfo("Resource name: " + context.getResourceName()));
    }

    return new ImmutablePair<>(minimumResource, maximumResource);
  }

  /**
   * 校验计算完成后的剩余资源，如果还有未分配完的资源，记录未充分利用警告。
   */
  private void validateRemainingResource() {
    for (String label : queue.getConfiguredNodeLabels()) {
      if (!batchRemainingResourcePerLabel.get(label).equals(ResourceVector.newInstance())) {
        updateContext.addUpdateWarning(QueueUpdateWarningType.BRANCH_UNDERUTILIZED.ofQueue(
            queue.getQueuePath()).withInfo("Label: " + label));
      }
    }
  }
}