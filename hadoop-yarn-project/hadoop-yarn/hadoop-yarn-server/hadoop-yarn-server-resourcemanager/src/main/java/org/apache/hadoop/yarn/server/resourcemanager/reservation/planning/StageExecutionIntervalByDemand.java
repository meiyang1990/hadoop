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
import org.apache.hadoop.yarn.server.resourcemanager.reservation.planning.IterativePlanner.StageProvider;

/**
 * 文件说明：YARN资源预留规划中按负载需求分配执行区间的实现类，继承StageExecutionInterval接口
 * 
 * 实现逻辑说明：
 * 1. 对于ANY和ALL类型的作业，执行区间直接使用[作业到达时间, 作业截止时间]
 * 2. 对于ORDER类型的作业，在保证每个阶段最小请求时长的前提下，将剩余时间按各阶段总资源权重比例分配
 * 权重为当前阶段的总资源需求量占所有未分配阶段总需求量的比例
 */

public class StageExecutionIntervalByDemand implements StageExecutionInterval {

  private long step;

  @Override
  public ReservationInterval computeExecutionInterval(Plan plan,
      ReservationDefinition reservation,
      ReservationRequest currentReservationStage, boolean allocateLeft,
      RLESparseResourceAllocation allocations) {

    // 先调用无约束实现获取当前阶段可使用的最大时间区间
    ReservationInterval maxInterval =
        (new StageExecutionIntervalUnconstrained()).computeExecutionInterval(
            plan, reservation, currentReservationStage, allocateLeft,
            allocations);

    ReservationRequestInterpreter jobType =
        reservation.getReservationRequests().getInterpreter();

    // 非ORDER类型作业直接返回最大区间，不做约束切割
    if ((jobType != ReservationRequestInterpreter.R_ORDER)
        && (jobType != ReservationRequestInterpreter.R_ORDER_NO_GAP)) {
      return maxInterval;
    }

    // 对ORDER和ORDER_NO_GAP类型，从最大区间中切割出当前阶段的子区间
    step = plan.getStep();

    double totalWeight = 0.0;
    long totalDuration = 0;

    // 根据分配方向创建阶段遍历器：allocateLeft为true时从后往前遍历，false时从前往后遍历
    StageProvider stageProvider = new StageProvider(!allocateLeft, reservation);

    // 遍历所有未分配阶段，累加总权重和总最小时长
    while (stageProvider.hasNext()) {
      ReservationRequest rr = stageProvider.next();
      totalWeight += calcWeight(rr);
      totalDuration += getRoundedDuration(rr, step);

      // 遍历到当前阶段停止
      if (rr == currentReservationStage) {
        break;
      }
    }

    // 计算当前阶段占所有未分配阶段总权重的比例
    double ratio = calcWeight(currentReservationStage) / totalWeight;

    // 计算当前区间参数：保证每个阶段至少获得请求时长，剩余窗口按权重比例分配给各阶段
    long maxIntervalArrival = maxInterval.getStartTime();
    long maxIntervalDeadline = maxInterval.getEndTime();
    long window = maxIntervalDeadline - maxIntervalArrival;
    long windowRemainder = window - totalDuration;

    if (allocateLeft) {
      // 从左向右分配：计算当前阶段最晚结束时间
      long latestEnd =
          (long) (maxIntervalArrival
              + getRoundedDuration(currentReservationStage, step)
              + (windowRemainder * ratio));

      // 按规划步长向下对齐时间
      latestEnd = stepRoundDown(latestEnd, step);

      // 返回当前阶段区间：从最大区间开始到计算出的最晚结束时间
      return new ReservationInterval(maxIntervalArrival, latestEnd);
    } else {
      // 从右向左分配：计算当前阶段最早开始时间
      long earlyStart =
          (long) (maxIntervalDeadline
              - getRoundedDuration(currentReservationStage, step)
              - (windowRemainder * ratio));

      // 按规划步长向上对齐时间
      earlyStart = stepRoundUp(earlyStart, step);

      // 返回当前阶段区间：从计算出的最早开始时间到最大区间结束
      return new ReservationInterval(earlyStart, maxIntervalDeadline);
    }
  }

  // 计算阶段权重：总资源需求量 = 时长 * 单容器内存 * 容器数
  protected double calcWeight(ReservationRequest stage) {
    return (stage.getDuration() * stage.getCapability().getMemorySize())
        * (stage.getNumContainers());
  }

  protected long getRoundedDuration(ReservationRequest stage, Long s) {
    return stepRoundUp(stage.getDuration(), s);
  }

  protected static long stepRoundDown(long t, long s) {
    return (t / s) * s;
  }

  protected static long stepRoundUp(long t, long s) {
    return ((t + s - 1) / s) * s;
  }
}