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

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.QueueCapacityVectorEntry;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.ResourceUnitCapacityType;

/**
 * 容量调度器资源计算迭代过程的上下文参数封装类，用于传递当前计算所需的资源、队列等信息
 */
public class CalculationContext {
  private final String resourceName;
  private final ResourceUnitCapacityType capacityType;
  private final CSQueue queue;

  /**
   * 构造计算上下文对象
   * @param resourceName 当前计算的资源名称
   * @param capacityType 容量计算类型
   * @param queue 当前计算对应的队列
   */
  public CalculationContext(String resourceName, ResourceUnitCapacityType capacityType,
                            CSQueue queue) {
    this.resourceName = resourceName;
    this.capacityType = capacityType;
    this.queue = queue;
  }

  public String getResourceName() {
    return resourceName;
  }

  public ResourceUnitCapacityType getCapacityType() {
    return capacityType;
  }

  public CSQueue getQueue() {
    return queue;
  }

  /**
   * 快捷获取当前队列指定标签下当前资源的最小容量配置项
   *
   * @param label 节点标签
   * @return 最小容量向量条目
   */
  public QueueCapacityVectorEntry getCurrentMinimumCapacityEntry(String label) {
    return queue.getConfiguredCapacityVector(label).getResource(resourceName);
  }

  /**
   * 快捷获取当前队列指定标签下当前资源的最大容量配置项
   *
   * @param label 节点标签
   * @return 最大容量向量条目
   */
  public QueueCapacityVectorEntry getCurrentMaximumCapacityEntry(String label) {
    return queue.getConfiguredMaxCapacityVector(label).getResource(resourceName);
  }
}