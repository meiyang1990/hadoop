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

/**
 * YARN预留资源规划代理，按顺序尝试多个规划算法，只要有一个算法规划成功就返回结果。
 * 用于组合多种规划策略，提升预留资源分配成功率。
 */
public class TryManyReservationAgents implements ReservationAgent {

  // 按顺序保存待尝试的规划算法列表
  private final List<ReservationAgent> algs;

  /**
   * 构造方法，初始化待尝试的规划算法列表
   * @param algs 待尝试的规划算法列表
   */
  public TryManyReservationAgents(List<ReservationAgent> algs) {
    this.algs = new LinkedList<ReservationAgent>(algs);
  }

  @Override
  public boolean createReservation(ReservationId reservationId, String user,
      Plan plan, ReservationDefinition contract) throws PlanningException {

    // 保存最后一次规划异常
    PlanningException planningException = null;

    // 按顺序尝试所有规划算法
    for (ReservationAgent alg : algs) {

      try {
        // 当前算法规划成功，直接返回true
        if (alg.createReservation(reservationId, user, plan, contract)) {
          return true;
        }
      } catch (PlanningException e) {
        // 记录规划异常
        planningException = e;
      }

    }

    // 所有算法失败，如果存在异常则抛出最后一次异常
    if (planningException != null) {
      throw planningException;
    }

    // 所有算法都失败且无异常，返回false
    return false;

  }

  @Override
  public boolean updateReservation(ReservationId reservationId, String user,
      Plan plan, ReservationDefinition contract) throws PlanningException {

    // 保存最后一次规划异常
    PlanningException planningException = null;

    // 按顺序尝试所有规划算法
    for (ReservationAgent alg : algs) {

      try {
        // 当前算法更新成功，直接返回true
        if (alg.updateReservation(reservationId, user, plan, contract)) {
          return true;
        }
      } catch (PlanningException e) {
        // 记录规划异常
        planningException = e;
      }

    }

    // 所有算法失败，如果存在异常则抛出最后一次异常
    if (planningException != null) {
      throw planningException;
    }

    // 所有算法都失败且无异常，返回false
    return false;

  }

  @Override
  public boolean deleteReservation(ReservationId reservationId, String user,
      Plan plan) throws PlanningException {
    // 直接委托Plan执行删除操作
    return plan.deleteReservation(reservationId);
  }

  @Override
  public void init(Configuration conf) {
  }
}