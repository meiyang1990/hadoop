// 这个文件已经全部加上中文注释
/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationSchedulerConfiguration;

/**
 * YARN资源调度器队列管理器接口，定义了调度队列的统一管理接口
 * 负责调度队列的增删查改和重新初始化，为资源调度提供队列管理能力
 *
 * @param <T> 调度队列类型
 * @param <E> 预约调度配置类型
 */
@SuppressWarnings("rawtypes")
@Private
@Unstable
public interface SchedulerQueueManager<T extends SchedulerQueue,
    E extends ReservationSchedulerConfiguration> {

  /**
   * 获取根调度队列
   * @return 根调度队列实例
   */
  T getRootQueue();

  /**
   * 获取所有调度队列的映射表
   * @return 队列名到队列实例的映射，包含所有调度队列
   */
  Map<String, T> getQueues();

  /**
   * 从现有队列集合中移除指定队列
   * @param queueName 待删除的队列名称
   */
  void removeQueue(String queueName);

  /**
   * 新增一个调度队列到现有队列集合
   * @param queueName 新队列名称
   * @param queue 新队列实例
   */
  void addQueue(String queueName, T queue);

  /**
   * 根据队列名称获取对应调度队列
   * @param queueName 目标队列名称
   * @return 匹配的队列实例，不存在则返回null
   */
  T getQueue(String queueName);

  /**
   * 根据新配置重新初始化所有调度队列，支持动态队列配置更新
   * @param newConf 新的预约调度配置
   * @throws IOException 重新初始化队列失败时抛出异常
   */
  void reinitializeQueues(E newConf) throws IOException;
}