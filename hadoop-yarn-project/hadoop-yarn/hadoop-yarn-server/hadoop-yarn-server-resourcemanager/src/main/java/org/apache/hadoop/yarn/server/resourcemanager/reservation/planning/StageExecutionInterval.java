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

import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.api.records.ReservationRequest;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.Plan;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.RLESparseResourceAllocation;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationInterval;

/**
 * 文件所属模块：YARN服务端 -> 资源调度器 -> 预约规划
 * 核心职责：定义计算预约阶段可分配资源时间区间的接口，为IterativePlanner迭代规划器提供阶段执行区间计算能力
 * 辅助工具类，用于计算{@link IterativePlanner}可以为阶段分配资源的时间区间。
 */
public interface StageExecutionInterval {
  /**
   * 计算给定预约阶段允许的最早启动时间，得到该阶段可分配资源的时间区间。
   *
   * @param plan 预约需要适配的资源计划
   * @param reservation 作业预约契约定义
   * @param currentReservationStage 当前处理的预约阶段
   * @param allocateLeft 标记是否采用从左到右的分配顺序
   * @param allocations 作业已分配的资源
   * @return 该阶段可获取资源的时间区间
   */
  ReservationInterval computeExecutionInterval(Plan plan,
      ReservationDefinition reservation,
      ReservationRequest currentReservationStage, boolean allocateLeft,
      RLESparseResourceAllocation allocations);

}