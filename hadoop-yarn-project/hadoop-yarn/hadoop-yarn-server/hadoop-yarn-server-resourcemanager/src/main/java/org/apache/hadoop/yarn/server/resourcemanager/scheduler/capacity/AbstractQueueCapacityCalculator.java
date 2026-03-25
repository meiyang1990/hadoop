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
import java.util.Set;

/**
 * 队列容量计算器抽象基类，封装队列容量配置与资源计算逻辑的通用框架
 * 不同容量类型（绝对资源/百分比资源）的计算器实现该抽象类
 */
public abstract class AbstractQueueCapacityCalculator {

  /**
   * 计算完实际资源值后，更新队列容量指标与统计信息
   *
   * @param resourceCalculationDriver 包含队列分支中间计算结果的驱动对象
   * @param queue 进行容量计算的目标队列
   * @param label 节点标签
   */
  public abstract void updateCapacitiesAfterCalculation(
      ResourceCalculationDriver resourceCalculationDriver, CSQueue queue, String label);


  /**
   * 获取当前计算器支持处理的容量类型
   *
   * @return 容量类型（绝对资源/百分比资源）
   */
  public abstract ResourceUnitCapacityType getCapacityType();

  /**
   * 计算队列最小有效资源量
   *
   * @param resourceCalculationDriver 包含队列分支中间计算结果的驱动对象
   * @param context 当前迭代阶段计算上下文
   * @param label 节点标签
   * @return 最小有效资源量
   */
  public abstract double calculateMinimumResource(ResourceCalculationDriver resourceCalculationDriver,
                                                 CalculationContext context,
                                                 String label);

  /**
   * 计算队列最大有效资源量
   *
   * @param resourceCalculationDriver 包含队列分支中间计算结果的驱动对象
   * @param context 当前迭代阶段计算上下文
   * @param label 节点标签
   * @return 最大有效资源量
   */
  public abstract double calculateMaximumResource(ResourceCalculationDriver resourceCalculationDriver,
                                                 CalculationContext context,
                                                 String label);

  /**
   * 在开始计算实际资源值之前，执行必要的前置计算逻辑
   *
   * @param resourceCalculationDriver 包含父队列信息的计算驱动对象，用于前置计算
   */
  public abstract void calculateResourcePrerequisites(
      ResourceCalculationDriver resourceCalculationDriver);

  /**
   * 获取当前计算器处理容量类型下，已定义的所有资源名称
   *
   * @param queue 容量向量定义所属队列
   * @param label 节点标签
   * @return 资源名称集合
   */
  protected Set<String> getResourceNames(CSQueue queue, String label) {
    return getResourceNames(queue, label, getCapacityType());
  }

  /**
   * 获取指定容量类型下，已定义的所有资源名称
   *
   * @param queue        容量向量定义所属队列
   * @param label        节点标签
   * @param capacityType 目标容量类型
   * @return 资源名称集合
   */
  protected Set<String> getResourceNames(CSQueue queue, String label,
                                         ResourceUnitCapacityType capacityType) {
    // 从队列已配置容量向量中按容量类型提取资源名称
    return queue.getConfiguredCapacityVector(label)
        .getResourceNamesByCapacityType(capacityType);
  }
}