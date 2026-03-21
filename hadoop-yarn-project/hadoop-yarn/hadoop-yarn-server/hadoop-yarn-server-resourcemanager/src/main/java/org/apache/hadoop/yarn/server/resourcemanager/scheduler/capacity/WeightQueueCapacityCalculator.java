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

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.ResourceUnitCapacityType;

import java.util.Collection;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.ResourceUnitCapacityType.WEIGHT;

/**
 * 基于权重的队列容量计算器，实现容量调度器中按权重分配队列资源的逻辑
 */
public class WeightQueueCapacityCalculator extends AbstractQueueCapacityCalculator {

  /**
   * 预计算子队列总权重，为后续容量计算做准备
   * @param resourceCalculationDriver 资源计算驱动上下文
   */
  @Override
  public void calculateResourcePrerequisites(ResourceCalculationDriver resourceCalculationDriver) {
    // 预计算所有子队列的权重总和
    for (CSQueue childQueue : resourceCalculationDriver.getChildQueues()) {
      for (String label : childQueue.getConfiguredNodeLabels()) {
        for (String resourceName : childQueue.getConfiguredCapacityVector(label)
            .getResourceNamesByCapacityType(getCapacityType())) {
          // 累加该标签+资源下的总权重
          resourceCalculationDriver.incrementWeight(label, resourceName, childQueue
              .getConfiguredCapacityVector(label).getResource(resourceName).getResourceValue());
        }
      }
    }
  }

  /**
   * 按权重计算队列最小资源容量
   * @param resourceCalculationDriver 资源计算驱动上下文
   * @param context 计算上下文
   * @param label 节点标签
   * @return 计算得到的队列绝对最小资源值
   */
  @Override
  public double calculateMinimumResource(ResourceCalculationDriver resourceCalculationDriver,
                                        CalculationContext context,
                                        String label) {
    String resourceName = context.getResourceName();
    // 计算当前队列的归一化权重（自身权重 / 同层级总权重）
    double normalizedWeight = context.getCurrentMinimumCapacityEntry(label).getResourceValue() /
        resourceCalculationDriver.getSumWeightsByResource(label, resourceName);

    // 获取当前批次该标签下剩余可分配资源
    double remainingResource = resourceCalculationDriver.getBatchRemainingResource(label)
        .getValue(resourceName);

    // 如果归一化权重为1（即只有当前队列使用权重分配），直接返回全部剩余资源避免舍入误差
    if (normalizedWeight == 1) {
      return remainingResource;
    }

    // 获取父队列剩余资源占比
    double remainingResourceRatio = resourceCalculationDriver.getRemainingRatioOfResource(
        label, resourceName);
    // 获取父队列绝对最小容量
    double parentAbsoluteCapacity = resourceCalculationDriver.getParentAbsoluteMinCapacity(
        label, resourceName);
    // 计算当前队列绝对容量 = 父容量 * 剩余资源占比 * 归一化权重
    double queueAbsoluteCapacity = parentAbsoluteCapacity * remainingResourceRatio
        * normalizedWeight;

    // 转换为基于集群总资源的绝对资源值返回
    return resourceCalculationDriver.getUpdateContext()
        .getUpdatedClusterResource(label).getResourceValue(resourceName) * queueAbsoluteCapacity;
  }

  /**
   * 按权重计算队列最大资源容量
   * @param resourceCalculationDriver 资源计算驱动上下文
   * @param context 计算上下文
   * @param label 节点标签
   * @return 计算得到的队列绝对最大资源值
   * @throws IllegalStateException 权重模式不支持最大容量计算，直接抛出异常
   */
  @Override
  public double calculateMaximumResource(ResourceCalculationDriver resourceCalculationDriver,
                                        CalculationContext context,
                                        String label) {
    throw new IllegalStateException("Resource " + context.getCurrentMinimumCapacityEntry(
        label).getResourceName() +
        " has " + "WEIGHT maximum capacity type, which is not supported");
  }

  /**
   * 获取当前计算器处理的容量类型
   * @return 容量类型，固定返回WEIGHT
   */
  @Override
  public ResourceUnitCapacityType getCapacityType() {
    return WEIGHT;
  }

  /**
   * 容量计算完成后更新队列的归一化权重和绝对容量
   * @param resourceCalculationDriver 资源计算驱动上下文
   * @param queue 当前队列
   * @param label 节点标签
   */
  @Override
  public void updateCapacitiesAfterCalculation(
      ResourceCalculationDriver resourceCalculationDriver, CSQueue queue, String label) {
    double sumCapacityPerResource = 0f;

    // 获取当前队列需要处理的所有资源类型
    Collection<String> resourceNames = getResourceNames(queue, label);
    for (String resourceName : resourceNames) {
      // 获取当前层级该资源的总权重
      double sumBranchWeight = resourceCalculationDriver.getSumWeightsByResource(label,
          resourceName);
      // 计算当前资源的归一化容量
      double capacity =  queue.getConfiguredCapacityVector(
          label).getResource(resourceName).getResourceValue() / sumBranchWeight;
      sumCapacityPerResource += capacity;
    }

    // 计算平均归一化权重并更新到队列
    queue.getQueueCapacities().setNormalizedWeight(label,
        (float) (sumCapacityPerResource / resourceNames.size()));
    // 更新队列的绝对容量值
    ((AbstractCSQueue) queue).updateAbsoluteCapacities();
  }
}