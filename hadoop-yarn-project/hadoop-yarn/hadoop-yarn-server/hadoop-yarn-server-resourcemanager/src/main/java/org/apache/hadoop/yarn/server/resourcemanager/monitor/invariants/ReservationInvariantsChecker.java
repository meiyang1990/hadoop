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
package org.apache.hadoop.yarn.server.resourcemanager.monitor.invariants;

import org.apache.hadoop.yarn.server.resourcemanager.reservation.Plan;
import org.apache.hadoop.yarn.util.UTCClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;

/**
 * 文件说明: YARN资源预留系统不变性检查器，验证预留计划与队列配置的一致性不变量
 * 不变性检查器，验证资源预留系统满足特定一致性约束
 */
public class ReservationInvariantsChecker extends InvariantsChecker {

  private static final Logger LOG =
      LoggerFactory.getLogger(ReservationInvariantsChecker.class);

  // UTC时钟，用于获取当前检查时间
  private UTCClock clock = new UTCClock();

  /**
   * 执行预约系统一致性不变性检查，验证当前活跃预约数量与预约队列数量一致
   */
  @Override
  public void editSchedule() {
    // 获取预约系统中所有计划
    Collection<Plan> plans =
        getContext().getReservationSystem().getAllPlans().values();

    try {
      // 遍历所有预约计划逐一检查
      for (Plan plan : plans) {
        // 获取当前时间点该计划下的活跃预约数量
        long currReservations =
            plan.getReservationsAtTime(clock.getTime()).size();
        // 获取该计划对应父队列下的预约子队列数量
        long numberReservationQueues = getContext().getScheduler()
            .getQueueInfo(plan.getQueueName(), true, false).getChildQueues()
            .size();
        // 检查活跃预约数是否等于(子队列数 - 1)，不匹配则记录或抛出异常
        if (currReservations != numberReservationQueues - 1) {
          logOrThrow("Number of reservations (" + currReservations
              + ") does NOT match the number of reservationQueues ("
              + (numberReservationQueues - 1) + "), while it should.");
        }
      }
    } catch (IOException io) {
      // IO异常包装为不变性违反异常
      throw new InvariantViolationException("Issue during invariant check: ",
          io);
    }

  }

}