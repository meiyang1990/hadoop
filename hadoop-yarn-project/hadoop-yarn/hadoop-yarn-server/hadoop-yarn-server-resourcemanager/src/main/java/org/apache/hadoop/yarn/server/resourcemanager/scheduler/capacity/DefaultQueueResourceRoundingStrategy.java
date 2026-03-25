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
 * 默认队列资源舍入策略，用于容量调度器的资源计算。
 * 除了最后处理的权重类型资源使用四舍五入外，所有其他类型资源使用向下取整。
 */
public class DefaultQueueResourceRoundingStrategy implements QueueResourceRoundingStrategy {
  // 存储优先级列表中最后处理的资源容量类型
  private final ResourceUnitCapacityType lastCapacityType;

  /**
   * 构造默认舍入策略，根据容量类型优先级确定最后处理的资源类型
   * @param capacityTypePrecedence 容量类型处理优先级数组
   */
  public DefaultQueueResourceRoundingStrategy(
      ResourceUnitCapacityType[] capacityTypePrecedence) {
    if (capacityTypePrecedence.length == 0) {
      throw new IllegalArgumentException("Capacity type precedence collection is empty");
    }

    lastCapacityType = capacityTypePrecedence[capacityTypePrecedence.length - 1];
  }

  @Override
  public double getRoundedResource(double resourceValue, QueueCapacityVectorEntry capacityVectorEntry) {
    // 如果是最后处理的资源类型，使用四舍五入
    if (capacityVectorEntry.getVectorResourceType().equals(lastCapacityType)) {
      return Math.round(resourceValue);
    } else {
      // 其他资源类型使用向下取整
      return Math.floor(resourceValue);
    }
  }
}