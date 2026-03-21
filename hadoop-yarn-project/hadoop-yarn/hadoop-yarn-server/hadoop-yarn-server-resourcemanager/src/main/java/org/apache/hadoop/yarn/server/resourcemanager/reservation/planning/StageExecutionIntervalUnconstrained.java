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
import org.apache.hadoop.yarn.api.records.ReservationRequestInterpreter;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.Plan;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.RLESparseResourceAllocation;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationInterval;

/**
 * 表示预约计划中不约束执行区间的阶段执行区间计算器，在满足作业约束的前提下为每个阶段分配最大可能的时间区间。
 * 对于ANY和ALL类型作业，区间为[作业到达时间, 作业截止时间)；
 * 对于ORDER类型作业，若从左向右分配则当前阶段不能早于所有前置阶段结束，若从右向左分配则当前阶段不能晚于所有后继阶段开始。
 */
public class StageExecutionIntervalUnconstrained implements
    StageExecutionInterval {

  /**
   * 计算当前预约阶段的允许执行时间区间。
   * @param plan 资源预约计划
   * @param reservation 预约定义
   * @param currentReservationStage 当前预约阶段请求
   * @param allocateLeft true为从左向右分配，false为从右向左分配
   * @param allocations 已分配的资源集合
   * @return 当前阶段允许执行的时间区间
   */
  @Override
  public ReservationInterval computeExecutionInterval(Plan plan,
      ReservationDefinition reservation,
      ReservationRequest currentReservationStage, boolean allocateLeft,
      RLESparseResourceAllocation allocations) {

    // 初始使用全局预约到达时间作为阶段最早开始时间
    Long stageArrival = reservation.getArrival();
    // 初始使用全局预约截止时间作为阶段最晚结束时间
    Long stageDeadline = reservation.getDeadline();

    // 获取作业的类型解释器（定义了多阶段执行顺序约束）
    ReservationRequestInterpreter jobType =
        reservation.getReservationRequests().getInterpreter();

    // 从左向右分配场景
    if (allocateLeft) {
      // 对于有序作业，根据已分配资源调整最早开始时间
      if ((jobType == ReservationRequestInterpreter.R_ORDER)
          || (jobType == ReservationRequestInterpreter.R_ORDER_NO_GAP)) {
        // 获取已分配资源的最晚结束时间，也就是所有前置阶段的结束时间
        Long allocationEndTime = allocations.getLatestNonNullTime();
        if (allocationEndTime != -1) {
          // 当前阶段最早只能在前置阶段全部完成后开始
          stageArrival = allocationEndTime;
        }
      }
    // 从右向左分配场景
    } else {
      // 对于有序作业，根据已分配资源调整最晚结束时间
      if ((jobType == ReservationRequestInterpreter.R_ORDER)
          || (jobType == ReservationRequestInterpreter.R_ORDER_NO_GAP)) {
        // 获取已分配资源的最早开始时间，也就是所有后继阶段的开始时间
        Long allocationStartTime = allocations.getEarliestStartTime();
        if (allocationStartTime != -1) {
          // 当前阶段最晚必须在后继阶段开始前结束
          stageDeadline = allocationStartTime;
        }
      }
    }
    // 返回计算得到的执行区间
    return new ReservationInterval(stageArrival, stageDeadline);
  }
}