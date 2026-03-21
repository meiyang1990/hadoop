// 这个文件已经全部加上中文注释
/*******************************************************************************
 *   Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *******************************************************************************/
package org.apache.hadoop.yarn.server.resourcemanager.reservation;

import java.util.Collection;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.util.Clock;

/**
 * YARN资源预留计划同步器接口，负责定时同步资源预留计划与底层资源调度器的状态。
 * 
 * 核心职责包含两个方向的同步：
 * 1. 将资源预留计划中已确认的预留分配映射到底层调度器，调整队列容量、优先级等参数，
 *    保证预留任务能够获得计划约定的资源量，实现资源预留承诺。
 * 2. 将集群资源变化（总可用资源变更）同步回资源预留计划，触发计划按需重新规划分配。
 * 
 * 实现要求：建议设计为无状态，即使在RM重启后长时间未运行，也能正确完成状态同步。
 * 需要足够频繁的运行，保证能够实时响应集群资源变化，维持预留承诺。
 */
public interface PlanFollower extends Runnable {

  /**
   * 初始化PlanFollower，注入所需依赖组件。
   * 
   * @param clock 系统时钟引用，用于时间相关计算
   * @param sched 底层资源调度器引用
   * @param plans 需要同步的所有资源预留计划集合
   */
  public void init(Clock clock, ResourceScheduler sched, Collection<Plan> plans);

  /**
   * 对指定资源预留计划执行同步操作。
   * 通常由run方法定时调用，也可在新预留请求即将开始时同步调用，避免竞态条件。
   * 
   * @param plan 需要同步的资源预留计划
   * @param shouldReplan 如果集群容量下降，true表示重新规划分配，false表示按比例缩小现有预留
   */
  public void synchronizePlan(Plan plan, boolean shouldReplan);

  /**
   * 更新需要同步的资源预留计划集合。
   * 
   * @param plans 每次定时同步需要处理的资源预留计划集合
   */
  public void setPlans(Collection<Plan> plans);

}