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

import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.Plan;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 混合 reservation 规划算法，先尝试使用低代价对齐规划算法分配资源，如果失败则回退到贪心算法。
 * 实现了YARN预留资源的规划代理接口，通过两级策略提升资源分配成功率。
 */
public class AlignedPlannerWithGreedy implements ReservationAgent {

  // 默认平滑因子
  public static final int DEFAULT_SMOOTHNESS_FACTOR = 10;
  public static final String SMOOTHNESS_FACTOR =
      "yarn.resourcemanager.reservation-system.smoothness-factor";
  private boolean allocateLeft = false;


  // 日志实例
  private static final Logger LOG = LoggerFactory
      .getLogger(AlignedPlannerWithGreedy.class);

  // 实际执行规划的代理实例
  private ReservationAgent planner;

  /**
   * 空构造函数。
   */
  public AlignedPlannerWithGreedy() {

  }

  @Override
  public void init(Configuration conf) {
    // 从配置读取平滑因子，使用默认值兜底
    int smoothnessFactor =
        conf.getInt(SMOOTHNESS_FACTOR, DEFAULT_SMOOTHNESS_FACTOR);
    // 从配置读取是否优先分配左侧（较早时间）资源，使用默认值兜底
    allocateLeft = conf.getBoolean(FAVOR_EARLY_ALLOCATION,
            DEFAULT_GREEDY_FAVOR_EARLY_ALLOCATION);

    // 保存规划算法列表，按尝试顺序排列
    List<ReservationAgent> listAlg = new LinkedList<ReservationAgent>();

    // 构造低代价对齐规划算法实例
    ReservationAgent algAligned =
        new IterativePlanner(new StageExecutionIntervalByDemand(),
            new StageAllocatorLowCostAligned(smoothnessFactor, allocateLeft),
            allocateLeft);

    listAlg.add(algAligned);

    // 构造贪心规划算法实例
    ReservationAgent algGreedy =
        new IterativePlanner(new StageExecutionIntervalUnconstrained(),
            new StageAllocatorGreedyRLE(allocateLeft), allocateLeft);
    listAlg.add(algGreedy);

    // 组合多个规划代理：先尝试对齐算法，失败则回退到贪心算法
    planner = new TryManyReservationAgents(listAlg);
  }

  @Override
  public boolean createReservation(ReservationId reservationId, String user,
      Plan plan, ReservationDefinition contract) throws PlanningException {

    LOG.info("placing the following ReservationRequest: " + contract);

    try {
      // 委托底层规划代理执行预留创建
      boolean res =
          planner.createReservation(reservationId, user, plan, contract);

      // 记录分配结果日志
      if (res) {
        LOG.info("OUTCOME: SUCCESS, Reservation ID: "
            + reservationId.toString() + ", Contract: " + contract.toString());
      } else {
        LOG.info("OUTCOME: FAILURE, Reservation ID: "
            + reservationId.toString() + ", Contract: " + contract.toString());
      }
      return res;
    } catch (PlanningException e) {
      // 规划异常记录日志后抛出
      LOG.info("OUTCOME: FAILURE, Reservation ID: " + reservationId.toString()
          + ", Contract: " + contract.toString());
      throw e;
    }

  }

  @Override
  public boolean updateReservation(ReservationId reservationId, String user,
      Plan plan, ReservationDefinition contract) throws PlanningException {

    LOG.info("updating the following ReservationRequest: " + contract);

    // 委托底层规划代理执行预留更新
    return planner.updateReservation(reservationId, user, plan, contract);

  }

  @Override
  public boolean deleteReservation(ReservationId reservationId, String user,
      Plan plan) throws PlanningException {

    LOG.info("removing the following ReservationId: " + reservationId);

    // 委托底层规划代理执行预留删除
    return planner.deleteReservation(reservationId, user, plan);

  }
}