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

import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningException;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.ResourceOverCommitException;

/**
 * 文件路径: hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/reservation/NoOverCommitPolicy.java
 * <p>
 * 不超容量配额的预约资源共享策略，强制遵守物理集群容量限制，确保新预约不会超出集群可用资源。
 * 支持对已有预约的更新操作，验证容量约束时会先扣除旧预约占用的资源，再验证新请求。
 */
@LimitedPrivate("yarn")
@Unstable
public class NoOverCommitPolicy implements SharingPolicy {

  /**
   * 验证新预约请求不会导致集群资源超额分配。
   * @param plan 资源预约计划，维护集群资源时间分片分配信息
   * @param reservation 待验证的新预约分配信息
   * @throws PlanningException 验证失败抛出规划异常
   */
  @Override
  public void validate(Plan plan, ReservationAllocation reservation)
      throws PlanningException {

    // 获取排除当前旧预约后，预约时间段内的可用资源
    RLESparseResourceAllocation available = plan.getAvailableResourceOverTime(
        reservation.getUser(), reservation.getReservationId(),
        reservation.getStartTime(), reservation.getEndTime(),
        reservation.getPeriodicity());

    // 验证预约请求不超出可用资源范围
    try {
      // 获取新预约在时间区间上的资源请求分布
      RLESparseResourceAllocation ask = reservation.getResourcesOverTime(
              reservation.getStartTime(), reservation.getEndTime());
      // 用可用资源减去请求资源，验证所有时间点结果均非负，确认资源足够
      RLESparseResourceAllocation
          .merge(plan.getResourceCalculator(), plan.getTotalCapacity(),
              available, ask,
              RLESparseResourceAllocation.RLEOperator.subtractTestNonNegative,
              reservation.getStartTime(), reservation.getEndTime());
    } catch (PlanningException p) {
      // 资源不足，抛出资源超额异常
      throw new ResourceOverCommitException(
          "Resources at time " + reservation.getStartTime()
          + " would be overcommitted by accepting reservation: "
              + reservation.getReservationId(), p);
    }
  }

  /**
   * 获取策略有效时间窗口，该策略无历史缓存，窗口为0。
   * @return 有效窗口大小，固定返回0
   */
  @Override
  public long getValidWindow() {
    // this policy has no "memory" so the valid window is set to zero
    return 0;
  }

  /**
   * 初始化策略，该策略无需初始化操作。
   * @param planQueuePath 预约计划对应队列路径
   * @param conf 预约调度器配置
   */
  @Override
  public void init(String planQueuePath,
      ReservationSchedulerConfiguration conf) {
    // nothing to do for this policy
  }

  /**
   * 计算可用资源，该策略直接返回传入的可用资源结果不做修改。
   * @param available 初始可用资源分布
   * @param plan 资源预约计划
   * @param user 用户名
   * @param oldId 旧预约ID
   * @param start 预约开始时间
   * @param end 预约结束时间
   * @return 计算后的可用资源分布
   * @throws PlanningException 规划异常
   */
  @Override
  public RLESparseResourceAllocation availableResources(
      RLESparseResourceAllocation available, Plan plan, String user,
      ReservationId oldId, long start, long end) throws PlanningException {
    return available;
  }

}