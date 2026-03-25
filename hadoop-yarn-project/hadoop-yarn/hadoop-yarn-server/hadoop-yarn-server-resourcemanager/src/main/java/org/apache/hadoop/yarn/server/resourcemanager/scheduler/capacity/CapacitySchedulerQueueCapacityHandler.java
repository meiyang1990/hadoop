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

import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceLimits;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.ResourceUnitCapacityType;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static org.apache.hadoop.yarn.api.records.ResourceInformation.MEMORY_URI;
import static org.apache.hadoop.yarn.api.records.ResourceInformation.VCORES_URI;
import static org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager.NO_LABEL;

/**
 * 容量调度队列容量处理器，负责计算和设置队列的实际容量与资源值。
 * 按标签和资源类型分别计算队列有效最小/最大资源值。
 */
public class CapacitySchedulerQueueCapacityHandler {

  private static final Logger LOG =
      LoggerFactory.getLogger(CapacitySchedulerQueueCapacityHandler.class);

  // 不同容量类型对应的计算器映射
  private final Map<ResourceUnitCapacityType, AbstractQueueCapacityCalculator>
      calculators;
  // 根队列专用计算器
  private final AbstractQueueCapacityCalculator rootCalculator =
      new RootQueueCapacityCalculator();
  // 节点标签管理器
  private final RMNodeLabelsManager labelsManager;
  // 已定义资源名称列表，保证内存、vcore排在前面
  private final Collection<String> definedResources = new LinkedHashSet<>();
  // 是否为传统队列配置模式
  private final boolean isLegacyQueueMode;

  /**
   * 构造容量处理器，初始化不同容量类型的计算器，加载资源类型。
   * @param labelsManager 节点标签管理器
   * @param configuration 容量调度配置
   */
  public CapacitySchedulerQueueCapacityHandler(RMNodeLabelsManager labelsManager,
                                               CapacitySchedulerConfiguration configuration) {
    this.calculators = new HashMap<>();
    this.labelsManager = labelsManager;

    this.calculators.put(ResourceUnitCapacityType.ABSOLUTE,
        new AbsoluteResourceCapacityCalculator());
    this.calculators.put(ResourceUnitCapacityType.PERCENTAGE,
        new PercentageQueueCapacityCalculator());
    this.calculators.put(ResourceUnitCapacityType.WEIGHT,
        new WeightQueueCapacityCalculator());
    this.isLegacyQueueMode = configuration.isLegacyQueueMode();

    loadResourceNames();
  }

  /**
   * 更新指定队列下所有子队列的资源和容量指标值，运行时动态计算。
   * @param clusterResource 集群总资源
   * @param queue 父队列，需要更新该队列的所有子队列
   * @return 更新上下文，包含更新阶段相关信息
   */
  public QueueCapacityUpdateContext updateChildren(Resource clusterResource, CSQueue queue) {
    ResourceLimits resourceLimits = new ResourceLimits(clusterResource);
    QueueCapacityUpdateContext updateContext =
        new QueueCapacityUpdateContext(clusterResource, labelsManager);

    update(queue, updateContext, resourceLimits);
    return updateContext;
  }

  /**
   * 更新根队列资源容量，根队列始终使用百分比容量类型，
   * 将整个集群资源作为其有效最小和最大资源。
   * @param rootQueue 根队列
   * @param clusterResource 集群总资源
   */
  public void updateRoot(CSQueue rootQueue, Resource clusterResource) {
    ResourceLimits resourceLimits = new ResourceLimits(clusterResource);
    QueueCapacityUpdateContext updateContext =
        new QueueCapacityUpdateContext(clusterResource, labelsManager);

    RootCalculationDriver rootCalculationDriver = new RootCalculationDriver(rootQueue,
        updateContext,
        rootCalculator, definedResources);
    rootCalculationDriver.calculateResources();
    rootQueue.refreshAfterResourceCalculation(updateContext.getUpdatedClusterResource(),
        resourceLimits);
  }

  // 递归更新队列及其所有子队列资源容量
  private void update(
      CSQueue queue, QueueCapacityUpdateContext updateContext, ResourceLimits resourceLimits) {
    if (queue == null || CollectionUtils.isEmpty(queue.getChildQueues())) {
      return;
    }

    ResourceCalculationDriver resourceCalculationDriver = new ResourceCalculationDriver(
        queue, updateContext, calculators, definedResources);
    resourceCalculationDriver.calculateResources();

    updateChildrenAfterCalculation(resourceCalculationDriver, resourceLimits);
  }

  // 计算完成后遍历更新所有子队列容量和资源
  private void updateChildrenAfterCalculation(
      ResourceCalculationDriver resourceCalculationDriver, ResourceLimits resourceLimits) {
    AbstractParentQueue parentQueue = (AbstractParentQueue) resourceCalculationDriver.getQueue();
    for (CSQueue childQueue : parentQueue.getChildQueues()) {
      updateQueueCapacities(resourceCalculationDriver, childQueue);

      ResourceLimits childLimit = parentQueue.getResourceLimitsOfChild(childQueue,
          resourceCalculationDriver.getUpdateContext().getUpdatedClusterResource(),
          resourceLimits, NO_LABEL, false);
      childQueue.refreshAfterResourceCalculation(resourceCalculationDriver.getUpdateContext()
              .getUpdatedClusterResource(), childLimit);

      update(childQueue, resourceCalculationDriver.getUpdateContext(), childLimit);
    }
  }

  /**
   * 更新当前正在计算的子队列容量值，加写锁保证线程安全。
   * @param resourceCalculationDriver 资源计算驱动器
   * @param queue 需要更新容量的队列
   */
  private void updateQueueCapacities(
      ResourceCalculationDriver resourceCalculationDriver, CSQueue queue) {
    queue.getWriteLock().lock();
    try {
      for (String label : queue.getConfiguredNodeLabels()) {
        if (!isLegacyQueueMode) {
          // 根据计算出的有效资源值更新容量
          setQueueCapacities(resourceCalculationDriver.getUpdateContext().getUpdatedClusterResource(
              label), queue, label);
        } else {
          // 按传统逻辑更新容量
          for (ResourceUnitCapacityType capacityType :
              queue.getConfiguredCapacityVector(label).getDefinedCapacityTypes()) {
            AbstractQueueCapacityCalculator calculator = calculators.get(capacityType);
            calculator.updateCapacitiesAfterCalculation(resourceCalculationDriver, queue, label);
          }
        }
      }
    } finally {
      queue.getWriteLock().unlock();
    }
  }

  /**
   * 根据计算得到的有效最小/最大资源，设置队列容量和绝对容量值。
   * @param clusterResource 集群总资源
   * @param queue 需要设置容量的子队列
   * @param label 节点标签
   */
  public static void setQueueCapacities(Resource clusterResource, CSQueue queue, String label) {
    if (!(queue instanceof AbstractCSQueue)) {
      return;
    }

    AbstractCSQueue csQueue = (AbstractCSQueue) queue;
    // 集群资源还未初始化时不覆盖预留资源
    if ((csQueue instanceof ReservationQueue ||
        csQueue instanceof PlanQueue) &&
        Stream.of(clusterResource.getResources())
            .map(ResourceInformation::getValue)
            .noneMatch(num -> num > 0)) {
      return;
    }

    ResourceCalculator resourceCalculator = csQueue.resourceCalculator;

    CSQueue parent = queue.getParent();
    if (parent == null) {
      return;
    }
    // 根据父队列最小资源和当前队列最小资源计算容量占比
    // capacity = 当前队列有效最小资源 / 父队列有效最小资源
    float result = resourceCalculator.divide(clusterResource,
        queue.getQueueResourceQuotas().getEffectiveMinResource(label),
        parent.getQueueResourceQuotas().getEffectiveMinResource(label));
    queue.getQueueCapacities().setCapacity(label,
        Float.isInfinite(result) ? 0 : result);

    // 根据父队列最大资源和当前队列最大资源计算最大容量占比
    // maxCapacity = 当前队列有效最大资源 / 父队列有效最大资源
    result = resourceCalculator.divide(clusterResource,
        queue.getQueueResourceQuotas().getEffectiveMaxResource(label),
        parent.getQueueResourceQuotas().getEffectiveMaxResource(label));
    queue.getQueueCapacities().setMaximumCapacity(label,
        Float.isInfinite(result) ? 0 : result);

    csQueue.updateAbsoluteCapacities();
  }

  // 加载所有资源类型名称，将内存、vcore排在最前面保证顺序
  private void loadResourceNames() {
    Set<String> resources = new HashSet<>(ResourceUtils.getResourceTypes().keySet());
    if (resources.contains(MEMORY_URI)) {
      resources.remove(MEMORY_URI);
      definedResources.add(MEMORY_URI);
    }

    if (resources.contains(VCORES_URI)) {
      resources.remove(VCORES_URI);
      definedResources.add(VCORES_URI);
    }

    definedResources.addAll(resources);
  }
}