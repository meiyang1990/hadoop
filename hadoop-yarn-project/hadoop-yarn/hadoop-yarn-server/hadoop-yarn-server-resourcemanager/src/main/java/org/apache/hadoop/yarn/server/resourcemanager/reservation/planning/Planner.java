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

package org.apache.hadoop.yarn.server.resourcemanager.reservation.planning;

import java.util.List;

import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.Plan;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationSchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningException;

/**
 * YARN资源预留规划器接口，定义预留资源规划的核心方法
 * 负责根据预留请求在现有集群资源计划上完成资源分配规划
 */
public interface Planner {

  /**
   * 更新现有资源计划，处理新增/删除/修改已有预留，并批量处理新的预留请求
   *
   * @param plan 待重新规划的资源计划对象
   * @param contracts 需要处理的预留请求列表
   * @throws PlanningException 规划失败时抛出异常
   */
  public void plan(Plan plan, List<ReservationDefinition> contracts)
      throws PlanningException;

  /**
   * 初始化规划器实例，绑定队列和配置参数
   *
   * @param planQueueName 当前规划对应的队列名称
   * @param conf 预留调度器配置对象
   */
  void init(String planQueueName, ReservationSchedulerConfiguration conf);
}