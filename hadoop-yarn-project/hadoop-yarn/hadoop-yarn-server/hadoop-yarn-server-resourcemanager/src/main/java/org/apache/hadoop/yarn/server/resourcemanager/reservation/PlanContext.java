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

package org.apache.hadoop.yarn.server.resourcemanager.reservation;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.planning.Planner;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.planning.ReservationAgent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;

/**
 * 预订计划上下文接口，提供对预订计划配置参数的只读访问接口。
 * 定义YARN容量调度中预订计划的核心配置查询能力。
 */
public interface PlanContext {

  /**
   * 获取计划的时间粒度，单位毫秒。
   * 
   * @return 计划的时间步长，单位毫秒
   */
  public long getStep();

  /**
   * 获取当前计划配置的预订代理，负责优化放置预订请求。
   * 
   * @return 当前计划配置的预订代理实例
   */
  public ReservationAgent getReservationAgent();

  /**
   * 获取重新规划器实例，当计划资源意外减少时会调用它重新规划。
   * 
   * @return 重新规划器实例，用于应对计划资源意外减少场景
   */
  public Planner getReplanner();

  /**
   * 获取当前计划配置的资源共享策略，管理不同用户间的资源共享规则。
   * 
   * @return 当前计划的资源共享策略实例
   */
  public SharingPolicy getSharingPolicy();

  /**
   * 获取系统资源计算器实例。
   * 
   * @return 系统资源计算器实例
   */
  public ResourceCalculator getResourceCalculator();

  /**
   * 获取该计划中可预订的最小资源分配量。
   * 
   * @return 该计划允许的最小可预订资源量
   */
  public Resource getMinimumAllocation();

  /**
   * 获取该计划中可预订的最大资源分配量。
   * 
   * @return 该计划允许的最大可预订资源量
   */
  public Resource getMaximumAllocation();

  /**
   * 获取该计划中周期性预订允许的最大周期。新提交预订的周期必须能被该值整除，否则提交失败。
   *
   * @return 当前计划允许的最大周期性预订周期
   */
  long getMaximumPeriodicity();

  /**
   * 获取该计划对应在资源调度器中对应的队列名称。
   * 
   * @return 当前计划对应的调度队列名称
   */
  public String getQueueName();

  /**
   * 获取该计划对应队列的指标统计对象。
   * 
   * @return 当前计划对应队列的指标统计对象
   */
  public QueueMetrics getQueueMetrics();

  /**
   * 获取预订过期后对仍在运行的应用程序的处理策略，是否杀死还是迁移到默认队列。
   * 
   * @return true表示需要杀死剩余应用，false表示需要迁移到默认队列
   */
  public boolean getMoveOnExpiry();

}