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

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerDynamicEditException;


import java.io.IOException;
import java.util.List;

/**
 * 自动创建队列管理策略接口，定义容量调度器下动态自动创建叶子队列的管理规范
 * 负责自动队列的初始化、容量调整和状态变更提交等核心生命周期操作
 */
public interface AutoCreatedQueueManagementPolicy {

  /**
   * 初始化策略，绑定父队列并完成初始配置加载
   * @param parentQueue 父队列，自动创建队列的所属父队列
   * @throws IOException 初始化过程中I/O异常
   */
  void init(AbstractParentQueue parentQueue) throws IOException;

  /**
   * 重新初始化策略状态，在配置变更时调用
   * @param parentQueue 父队列，重新绑定的父队列
   * @throws IOException 重新初始化过程中I/O异常
   */
  void reinitialize(AbstractParentQueue parentQueue) throws IOException;

  /**
   * 获取指定自动创建叶子队列的初始配置模板
   * @param leafQueue 目标叶子队列
   * @return 自动创建队列的初始配置和容量信息
   * @throws SchedulerDynamicEditException 获取初始配置失败时抛出
   */
  AutoCreatedLeafQueueConfig getInitialLeafQueueConfiguration(
      AbstractAutoCreatedLeafQueue leafQueue)
      throws SchedulerDynamicEditException;

  /**
   * 计算并调整自动创建叶子队列的容量分配
   * 仅计算队列配额变更，不更新实际队列状态，验证通过后由调度器提交变更
   *
   * @return 建议的队列配额变更列表，由调度器决定是否最终生效
   * @throws SchedulerDynamicEditException 计算队列管理变更失败时抛出
   */
  List<QueueManagementChange> computeQueueManagementChanges()
      throws SchedulerDynamicEditException;

  /**
   * 提交并更新队列状态，将计算好的队列变更应用到实际队列状态中
   *
   * @param queueManagementChanges 待提交的队列变更列表
   * @throws SchedulerDynamicEditException 提交队列管理变更失败时抛出
   */
  void commitQueueManagementChanges(
      List<QueueManagementChange> queueManagementChanges)
      throws SchedulerDynamicEditException;
}