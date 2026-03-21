// 这个文件已经全部加上中文注释
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You can obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.converter.weightconversion;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSQueue;

/**
 * 公平调度器到容量调度器配置转换器接口，定义将公平调度队列权重转换为容量的方法。
 * 用于将公平调度器的权重配置转换为容量调度器兼容的容量配置。
 */
public interface CapacityConverter {
  /**
   * 为指定父队列的所有子队列完成权重到容量的转换。
   * @param queue 父队列，包含需要转换的所有子队列
   * @param csConfig 容量调度器配置对象，转换后的容量会写入此配置
   */
  void convertWeightsForChildQueues(FSQueue queue, CapacitySchedulerConfiguration csConfig);
}