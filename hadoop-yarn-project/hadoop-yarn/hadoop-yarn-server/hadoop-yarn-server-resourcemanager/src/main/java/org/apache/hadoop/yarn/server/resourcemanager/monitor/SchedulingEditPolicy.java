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
package org.apache.hadoop.yarn.server.resourcemanager.monitor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;

/**
 * 调度编辑策略接口，定义YARN资源调度器动态调整的扩展策略规范
 * 供资源监控模块使用，允许自定义策略根据集群状态动态调整调度配置
 */
public interface SchedulingEditPolicy {

  /**
   * 初始化调度编辑策略，注入配置、RM上下文和调度器实例
   * @param config YARN配置对象
   * @param context RM上下文对象，包含集群全局状态
   * @param scheduler 资源调度器实例，可用于获取调度状态和修改调度配置
   */
  void init(Configuration config, RMContext context,
      ResourceScheduler scheduler);

  /**
   * This method is invoked at regular intervals. Internally the policy is
   * allowed to track containers and affect the scheduler. The "actions"
   * performed are passed back through an EventHandler.
   */
  void editSchedule();

  /**
   * 获取策略执行监控间隔，单位毫秒
   * @return 两次调度编辑执行的间隔时间
   */
  long getMonitoringInterval();

  /**
   * 获取策略名称，用于日志和监控标识
   * @return 策略名称字符串
   */
  String getPolicyName();

}