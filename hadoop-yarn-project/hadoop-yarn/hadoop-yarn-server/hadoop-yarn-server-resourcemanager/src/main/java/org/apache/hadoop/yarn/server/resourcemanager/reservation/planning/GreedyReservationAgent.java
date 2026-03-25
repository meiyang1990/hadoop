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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.Plan;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This Agent employs a simple greedy placement strategy, placing the various
 * stages of a {@link ReservationDefinition} from the deadline moving backward
 * towards the arrival. This allows jobs with earlier deadline to be scheduled
 * greedily as well. Combined with an opportunistic anticipation of work if the
 * cluster is not fully utilized also seems to provide good latency for
 * best-effort jobs (i.e., jobs running without a reservation).
 *
 * This agent does not account for locality and only consider container
 * granularity for validation purposes (i.e., you can't exceed max-container
 * size).
 * YARN资源预留贪心调度代理，采用贪心策略在集群上为预留任务分配资源
 * 策略逻辑：从截止日期倒推开始时间分配资源，支持优先早时段或优先晚时段分配
 * 不考虑数据位置亲和性，仅容器粒度做容量校验
 */

public class GreedyReservationAgent implements ReservationAgent {

  // 日志实例
  private static final Logger LOG = LoggerFactory
      .getLogger(GreedyReservationAgent.class);

  // 实际执行规划的代理实例
  private ReservationAgent planner;
  // 是否优先分配早时段资源
  private boolean allocateLeft;

  /**
   * 空构造函数
   */
  public GreedyReservationAgent() {
  }

  @Override
  /**
   * 初始化贪心代理，读取配置并初始化底层规划器
   * @param conf 配置对象
   */
  public void init(Configuration conf) {
    // 读取配置获取优先分配方向
    allocateLeft = conf.getBoolean(FAVOR_EARLY_ALLOCATION,
        DEFAULT_GREEDY_FAVOR_EARLY_ALLOCATION);
    if (allocateLeft) {
      LOG.info("Initializing the GreedyReservationAgent to favor \"early\""
          + " (left) allocations (controlled by parameter: "
          + FAVOR_EARLY_ALLOCATION + ")");
    } else {
      LOG.info("Initializing the GreedyReservationAgent to favor \"late\""
          + " (right) allocations (controlled by parameter: "
          + FAVOR_EARLY_ALLOCATION + ")");
    }

    // 初始化迭代规划器，使用贪心RLE分配算法，不约束执行区间
    planner =
        new IterativePlanner(new StageExecutionIntervalUnconstrained(),
            new StageAllocatorGreedyRLE(allocateLeft), allocateLeft);
  }

  /**
   * 获取是否优先分配早时段资源
   * @return 优先早时段返回true，否则返回false
   */
  public boolean isAllocateLeft(){
    return allocateLeft;
  }

  @Override
  /**
   * 创建新的资源预留
   * @param reservationId 预留ID
   * @param user 提交用户
   * @param plan 资源规划对象
   * @param contract 预留定义
   * @return 创建成功返回true，失败返回false
   * @throws PlanningException 规划过程异常
   */
  public boolean createReservation(ReservationId reservationId, String user,
      Plan plan, ReservationDefinition contract) throws PlanningException {

    LOG.info("placing the following ReservationRequest: " + contract);

    try {
      // 委托底层规划器执行创建
      boolean res =
          planner.createReservation(reservationId, user, plan, contract);

      // 记录执行结果日志
      if (res) {
        LOG.info("OUTCOME: SUCCESS, Reservation ID: "
            + reservationId.toString() + ", Contract: " + contract.toString());
      } else {
        LOG.info("OUTCOME: FAILURE, Reservation ID: "
            + reservationId.toString() + ", Contract: " + contract.toString());
      }
      return res;
    } catch (PlanningException e) {
      LOG.info("OUTCOME: FAILURE, Reservation ID: " + reservationId.toString()
          + ", Contract: " + contract.toString());
      throw e;
    }

  }

  @Override
  /**
   * 更新已有资源预留
   * @param reservationId 预留ID
   * @param user 提交用户
   * @param plan 资源规划对象
   * @param contract 更新后的预留定义
   * @return 更新成功返回true，失败返回false
   * @throws PlanningException 规划过程异常
   */
  public boolean updateReservation(ReservationId reservationId, String user,
      Plan plan, ReservationDefinition contract) throws PlanningException {

    LOG.info("updating the following ReservationRequest: " + contract);

    return planner.updateReservation(reservationId, user, plan, contract);

  }

  @Override
  /**
   * 删除已有资源预留
   * @param reservationId 预留ID
   * @param user 提交用户
   * @param plan 资源规划对象
   * @return 删除成功返回true，失败返回false
   * @throws PlanningException 规划过程异常
   */
  public boolean deleteReservation(ReservationId reservationId, String user,
      Plan plan) throws PlanningException {

    LOG.info("removing the following ReservationId: " + reservationId);

    return planner.deleteReservation(reservationId, user, plan);

  }

}