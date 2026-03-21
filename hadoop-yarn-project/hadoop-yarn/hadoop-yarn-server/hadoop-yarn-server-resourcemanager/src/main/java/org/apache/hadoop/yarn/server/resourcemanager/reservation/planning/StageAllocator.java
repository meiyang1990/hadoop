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

import java.util.Map;

import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.ReservationRequest;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.Plan;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.RLESparseResourceAllocation;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationInterval;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningException;

/**
 * YARN容量调度预约规划中，为迭代规划器分配单个预约阶段资源的接口。
 * 定义了在指定时间窗口内为单个预约阶段计算资源分配的契约，不同实现可提供不同的分配策略。
 */
public interface StageAllocator {

  /**
   * 在给定时间区间内计算单个预约阶段的资源分配。
   *
   * @param plan 预约规划对象，本次预约需要适配到该规划中
   * @param planLoads 对规划各时间点资源负载的"脏读"快照，用于快速计算可用资源
   * @param planModifications 规划算法已执行但尚未同步到原规划的分配修改记录
   * @param rr 当前待分配的预约阶段请求，包含资源需求
   * @param stageArrival 两阶段规划算法为该阶段设定的最早开始时间（到达时间）
   * @param stageDeadline 两阶段规划算法为该阶段设定的截止时间
   * @param period 当前阶段重复出现的周期（周期性预约场景使用，非周期为0）
   * @param user 提交本次预约的用户名
   * @param oldId 更新预约场景中原预约的ID，新增预约为null
   *
   * @return 计算得到的资源分配映射表，键为时间区间、值为对应资源；无法分配则返回null
   * @throws PlanningException 当分配过程发生错误时抛出
   */
  Map<ReservationInterval, Resource> computeStageAllocation(Plan plan,
      RLESparseResourceAllocation planLoads,
      RLESparseResourceAllocation planModifications, ReservationRequest rr,
      long stageArrival, long stageDeadline, long period, String user,
      ReservationId oldId) throws PlanningException;

}