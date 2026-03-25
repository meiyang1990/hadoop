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

/**
 * 容量调度器基于百分比的队列容量计算器
 * 负责按百分比规则计算队列的最小/最大资源容量，是容量调度器分层资源分配的核心实现之一
 */
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.ResourceUnitCapacityType;

/**
 * 基于百分比配置的队列容量计算器，根据父队列容量和当前队列配置的百分比计算实际资源量
 */
public class PercentageQueueCapacityCalculator extends AbstractQueueCapacityCalculator {

  /**
   * 根据百分比配置计算队列最小资源量
   * @param resourceCalculationDriver 资源计算驱动，提供计算上下文和辅助方法
   * @param context 当前计算上下文，包含队列配置信息
   * @param label 节点标签，用于分区资源计算
   * @return 计算得到的队列最小资源绝对值
   */
  @Override
  public double calculateMinimumResource(
      ResourceCalculationDriver resourceCalculationDriver, CalculationContext context,
      String label) {
    String resourceName = context.getResourceName();

    // 获取父队列此标签下的绝对最小容量
    double parentAbsoluteCapacity = resourceCalculationDriver.getParentAbsoluteMinCapacity(label,
        resourceName);
    // 获取当前有效资源占总资源的剩余比例
    double remainingPerEffectiveResourceRatio =
        resourceCalculationDriver.getRemainingRatioOfResource(label, resourceName);
    // 根据父容量、剩余比例和当前队列配置百分比计算相对容量
    double absoluteCapacity = parentAbsoluteCapacity * remainingPerEffectiveResourceRatio
        * context.getCurrentMinimumCapacityEntry(label).getResourceValue() / 100;

    // 乘以集群总资源得到最终绝对资源值
    return resourceCalculationDriver.getUpdateContext().getUpdatedClusterResource(label)
        .getResourceValue(resourceName) * absoluteCapacity;
  }

  /**
   * 根据百分比配置计算队列最大资源量
   * @param resourceCalculationDriver 资源计算驱动，提供计算上下文和辅助方法
   * @param context 当前计算上下文，包含队列配置信息
   * @param label 节点标签，用于分区资源计算
   * @return 计算得到的队列最大资源绝对值
   */
  @Override
  public double calculateMaximumResource(
      ResourceCalculationDriver resourceCalculationDriver, CalculationContext context,
      String label) {
    String resourceName = context.getResourceName();

    // 获取父队列此标签下的绝对最大容量
    double parentAbsoluteMaxCapacity =
        resourceCalculationDriver.getParentAbsoluteMaxCapacity(label, resourceName);
    // 根据父容量和当前队列配置百分比计算绝对最大容量
    double absoluteMaxCapacity = parentAbsoluteMaxCapacity
        * context.getCurrentMaximumCapacityEntry(label).getResourceValue() / 100;

    // 乘以集群总资源得到最终绝对资源值
    return resourceCalculationDriver.getUpdateContext().getUpdatedClusterResource(label)
        .getResourceValue(resourceName) * absoluteMaxCapacity;
  }

  @Override
  public void calculateResourcePrerequisites(ResourceCalculationDriver resourceCalculationDriver) {

  }

  /**
   * 容量计算完成后更新队列的绝对容量
   * @param resourceCalculationDriver 资源计算驱动
   * @param queue 目标队列
   * @param label 节点标签
   */
  @Override
  public void updateCapacitiesAfterCalculation(ResourceCalculationDriver resourceCalculationDriver,
      CSQueue queue, String label) {
    ((AbstractCSQueue) queue).updateAbsoluteCapacities();
  }

  /**
   * 获取当前计算器对应的容量类型
   * @return 返回百分比类型容量
   */
  @Override
  public ResourceUnitCapacityType getCapacityType() {
    return ResourceUnitCapacityType.PERCENTAGE;
  }
}