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

import java.util.Collection;
import java.util.Collections;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.ResourceUnitCapacityType.PERCENTAGE;

/**
 * 根队列资源计算驱动器，专门处理根队列的资源容量计算逻辑。
 * 容量调度器中根队列作为顶级队列，资源计算逻辑有特殊性，单独封装此类。
 */
public final class RootCalculationDriver extends ResourceCalculationDriver {
  // 根队列容量计算器实例，负责实际执行根队列资源计算逻辑
  private final AbstractQueueCapacityCalculator rootCalculator;

  /**
   * 构造根队列资源计算驱动器实例。
   * @param rootQueue 根队列对象
   * @param updateContext 队列容量更新上下文
   * @param rootCalculator 根队列容量计算器
   * @param definedResources 已定义资源名称集合
   */
  public RootCalculationDriver(CSQueue rootQueue, QueueCapacityUpdateContext updateContext,
                               AbstractQueueCapacityCalculator rootCalculator,
                               Collection<String> definedResources) {
    super(rootQueue, updateContext, Collections.emptyMap(), definedResources);
    this.rootCalculator = rootCalculator;
  }

  @Override
  public void calculateResources() {
    // 遍历根队列配置的所有节点标签
    for (String label : queue.getConfiguredNodeLabels()) {
      // 遍历当前标签下所有配置的容量向量条目（每个资源对应一个条目）
      for (QueueCapacityVector.QueueCapacityVectorEntry capacityVectorEntry :
          queue.getConfiguredCapacityVector(label)) {
        // 获取当前计算的资源名称
        String resourceName = capacityVectorEntry.getResourceName();

        // 创建当前资源的计算上下文，根队列使用百分比类型容量
        CalculationContext context = new CalculationContext(resourceName, PERCENTAGE, queue);
        // 计算根队列最小资源量
        double minimumResource = rootCalculator.calculateMinimumResource(this, context, label);
        // 计算根队列最大资源量
        double maximumResource = rootCalculator.calculateMaximumResource(this, context, label);
        // 对最小资源量进行取整，保证资源单位对齐
        long roundedMinResource = (long) roundingStrategy
            .getRoundedResource(minimumResource, capacityVectorEntry);
        // 对最大资源量进行取整对齐
        long roundedMaxResource = (long) roundingStrategy
            .getRoundedResource(maximumResource,
                queue.getConfiguredMaxCapacityVector(label).getResource(resourceName));
        // 将计算完成的最小资源写入根队列生效配额
        queue.getQueueResourceQuotas().getEffectiveMinResource(label).setResourceValue(
            resourceName, roundedMinResource);
        // 将计算完成的最大资源写入根队列生效配额
        queue.getQueueResourceQuotas().getEffectiveMaxResource(label).setResourceValue(
            resourceName, roundedMaxResource);
      }
      // 所有资源计算完成后，更新当前标签下的根队列容量
      rootCalculator.updateCapacitiesAfterCalculation(this, queue, label);
    }

    // 计算所有资源依赖的先决条件配置
    rootCalculator.calculateResourcePrerequisites(this);
  }
}