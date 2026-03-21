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

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.ResourceUnitCapacityType.PERCENTAGE;

/**
 * 根队列容量计算器，为YARN容量调度器的根队列提供容量计算实现。
 * 根队列占用集群全部资源，所有子队列的容量都是基于根队列总资源比例计算。
 */
public class RootQueueCapacityCalculator extends AbstractQueueCapacityCalculator {

  /**
   * 计算根队列资源前置条件，初始化归一化资源比例。
   * @param resourceCalculationDriver 资源计算驱动上下文
   */
  @Override
  public void calculateResourcePrerequisites(ResourceCalculationDriver resourceCalculationDriver) {
    AbsoluteResourceCapacityCalculator.setNormalizedResourceRatio(resourceCalculationDriver);
  }

  /**
   * 计算根队列最小资源量，根队列最小资源等于集群全部可用资源。
   * @param resourceCalculationDriver 资源计算驱动上下文
   * @param context 计算上下文
   * @param label 节点标签
   * @return 根队列最小资源值
   */
  @Override
  public double calculateMinimumResource(ResourceCalculationDriver resourceCalculationDriver,
                                         CalculationContext context, String label) {
    return resourceCalculationDriver.getUpdateContext().getUpdatedClusterResource(label)
        .getResourceValue(context.getResourceName());
  }

  /**
   * 计算根队列最大资源量，根队列最大资源等于集群全部可用资源。
   * @param resourceCalculationDriver 资源计算驱动上下文
   * @param context 计算上下文
   * @param label 节点标签
   * @return 根队列最大资源值
   */
  @Override
  public double calculateMaximumResource(ResourceCalculationDriver resourceCalculationDriver,
                                         CalculationContext context, String label) {
    return resourceCalculationDriver.getUpdateContext().getUpdatedClusterResource(label)
        .getResourceValue(context.getResourceName());
  }

  /**
   * 计算完成后更新根队列容量信息，设置根队列绝对容量为100%。
   * @param resourceCalculationDriver 资源计算驱动上下文
   * @param queue 目标根队列对象
   * @param label 节点标签
   */
  @Override
  public void updateCapacitiesAfterCalculation(
      ResourceCalculationDriver resourceCalculationDriver, CSQueue queue, String label) {
    queue.getQueueCapacities().setAbsoluteCapacity(label, 1);
    if (queue.getQueueCapacities().getWeight(label) == 1) {
      queue.getQueueCapacities().setNormalizedWeight(label, 1);
    }
  }

  /**
   * 获取容量计算类型，根队列使用百分比类型容量表示。
   * @return 百分比容量类型
   */
  @Override
  public ResourceUnitCapacityType getCapacityType() {
    return PERCENTAGE;
  }
}