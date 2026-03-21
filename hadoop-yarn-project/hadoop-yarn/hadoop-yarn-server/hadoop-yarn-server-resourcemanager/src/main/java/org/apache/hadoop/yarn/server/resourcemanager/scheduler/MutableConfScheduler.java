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

import org.apache.hadoop.conf.Configuration;

/**
 * 支持运行时动态修改配置的YARN调度器接口，扩展了基础ResourceScheduler能力。
 * 允许在不重启ResourceManager的情况下更新调度配置，提供动态配置变更能力。
 */
public interface MutableConfScheduler extends ResourceScheduler {

  /**
   * 获取调度器当前生效的配置对象。
   * @return 调度器当前配置
   */
  Configuration getConfiguration();

  /**
   * 根据队列名称获取对应的队列对象。
   * @param queueName 队列名称
   * @return 对应队列对象，不存在则返回null
   */
  Queue getQueue(String queueName);

  /**
   * 返回当前调度器配置是否支持动态修改。
   * @return true表示支持动态修改配置，false表示不支持
   */
  boolean isConfigurationMutable();

  /**
   * 获取调度器的可变配置提供者，允许其他组件直接调用配置变更API修改调度配置。
   * @return 调度器的可变配置提供者实例
   */
  MutableConfigurationProvider getMutableConfProvider();
}