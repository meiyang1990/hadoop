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

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.ReservationRequest;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.Plan;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.RLESparseResourceAllocation;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.RLESparseResourceAllocation.RLEOperator;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationInterval;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningException;
import org.apache.hadoop.yarn.util.resource.Resources;

/**
 * 按照贪心分配规则计算阶段资源分配。贪心规则会重复在最靠右（最晚）的空闲区间分配请求的容器。
 */

public class StageAllocatorGreedy implements StageAllocator {

  @Override
  /**
   * 贪心计算预约请求的阶段资源分配
   * @param plan 资源分配计划
   * @param planLoads 计划已占用资源
   * @param planModifications 当前预约已分配的临时修改
   * @param rr 预约资源请求
   * @param stageEarliestStart 阶段最早开始时间
   * @param stageDeadline 阶段最晚截止时间
   * @param period 周期
   * @param user 用户名
   * @param oldId 旧预约ID（更新场景使用）
   * @return 分配结果，分配失败返回null
   * @throws PlanningException 规划异常
   */
  public Map<ReservationInterval, Resource> computeStageAllocation(Plan plan,
      RLESparseResourceAllocation planLoads,
      RLESparseResourceAllocation planModifications, ReservationRequest rr,
      long stageEarliestStart, long stageDeadline, long period, String user,
      ReservationId oldId) throws PlanningException {

    // 获取计划总容量
    Resource totalCapacity = plan.getTotalCapacity();

    // 保存最终分配结果
    Map<ReservationInterval, Resource> allocationRequests =
        new HashMap<ReservationInterval, Resource>();

    // 计算一批（gang）所需总资源，获取单批持续时间
    Resource gang = Resources.multiply(rr.getCapability(), rr.getConcurrency());
    long dur = rr.getDuration();
    long step = plan.getStep();

    // 将持续时间向上取整为计划步长的整数倍
    if (dur % step != 0) {
      dur += (step - (dur % step));
    }

    // 计算需要分配多少批（gang）容器，保证整除无余数
    int gangsToPlace = rr.getNumContainers() / rr.getConcurrency();

    int maxGang = 0;

    // 获取用户当前可用的净资源时间分布
    RLESparseResourceAllocation netAvailable =
        plan.getAvailableResourceOverTime(user, oldId, stageEarliestStart,
            stageDeadline, period);

    // 减去当前已临时分配给同预约其他请求的资源，得到当前真实可用资源
    netAvailable =
        RLESparseResourceAllocation.merge(plan.getResourceCalculator(),
            plan.getTotalCapacity(), netAvailable, planModifications,
            RLEOperator.subtract, stageEarliestStart, stageDeadline);

    // 循环分配直到分配完成，或没有足够空间容纳下一批
    while (gangsToPlace > 0 && stageDeadline - dur >= stageEarliestStart) {

      // 初始化当前轮次可容纳最大批数，记录最紧俏时间点
      maxGang = gangsToPlace;
      long minPoint = stageDeadline;
      int curMaxGang = maxGang;

      // 从截止时间向前遍历当前时间窗口内所有时间步
      for (long t = stageDeadline - plan.getStep(); t >= stageDeadline - dur
          && maxGang > 0; t = t - plan.getStep()) {

        // 获取当前时间点可用资源
        Resource netAvailableRes = netAvailable.getCapacityAtTime(t);

        // 计算当前时间点最多可容纳多少批
        curMaxGang =
            (int) Math.floor(Resources.divide(plan.getResourceCalculator(),
                totalCapacity, netAvailableRes, gang));

        // 取剩余待分配批数和当前可容纳批数的较小值
        curMaxGang = Math.min(gangsToPlace, curMaxGang);

        // 更新全局最小可容纳批数，记录对应时间点（后续分配从此向左查找）
        if (curMaxGang <= maxGang) {
          maxGang = curMaxGang;
          minPoint = t;
        }
      }

      // 如果能分配至少一批
      if (maxGang > 0) {
        // 减少待分配批数
        gangsToPlace -= maxGang;

        // 创建分配时间区间
        ReservationInterval reservationInt =
            new ReservationInterval(stageDeadline - dur, stageDeadline);
        // 计算分配总资源
        Resource reservationRes =
            Resources.multiply(rr.getCapability(), rr.getConcurrency()
                * maxGang);
        // 更新临时分配记录，避免同一预约内多次分配重复计算资源
        planModifications.addInterval(reservationInt, reservationRes);
        // 保存本次分配到结果
        allocationRequests.put(reservationInt, reservationRes);

      }

      // 将新截止时间设置到最紧俏时间点，下一轮从此向左继续分配
      stageDeadline = minPoint;
    }

    // 所有批分配完成，返回分配结果
    if (gangsToPlace == 0) {
      return allocationRequests;
    } else {
      // 分配失败，回滚所有已做的临时分配修改
      for (Map.Entry<ReservationInterval, Resource> tempAllocation
          : allocationRequests.entrySet()) {
        planModifications.removeInterval(tempAllocation.getKey(),
            tempAllocation.getValue());
      }
      // 返回null标识分配失败
      return null;
    }

  }

}