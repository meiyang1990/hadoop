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

/**
 * YARN容量调度器队列资源舍入策略接口，定义将计算得到的浮点资源值转换为整数资源值的统一抽象。
 * 用于在按比例分配集群资源时，处理小数资源的舍入逻辑，保证总资源分配结果正确。
 */
public interface QueueResourceRoundingStrategy {

  /**
   * 对计算得到的浮点资源值进行舍入，返回最终可分配的资源值。
   * @param resourceValue 计算得到的原始浮点资源值
   * @param capacityVectorEntry 队列配置的容量条目，提供队列元信息
   * @return 舍入后的最终资源值
   */
  double getRoundedResource(double resourceValue, QueueCapacityVectorEntry capacityVectorEntry);
}