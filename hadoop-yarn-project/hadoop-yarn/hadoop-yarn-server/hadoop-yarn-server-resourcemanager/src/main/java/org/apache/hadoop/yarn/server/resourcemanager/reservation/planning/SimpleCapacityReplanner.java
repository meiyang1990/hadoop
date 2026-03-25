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

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.Plan;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationAllocation;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationSchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningException;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.UTCClock;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * 文件：YARN资源预留容量重规划器
 * 核心职责：在指定时间窗口内扫描现有资源预留，检测总容量是否超限，通过贪心删除后接收的预留来恢复容量合规
 * 重规划策略：按接受顺序逆序删除，最新接收的预留最先被删除
 */
public class SimpleCapacityReplanner implements Planner {

  private static final Logger LOG = LoggerFactory
      .getLogger(SimpleCapacityReplanner.class);

  private static final Resource ZERO_RESOURCE = Resource.newInstance(0, 0);

  // 系统时钟，用于获取当前时间
  private final Clock clock;

  // 控制重规划检查的时间窗口长度，仅检查未来该窗口内的容量
  private long lengthOfCheckZone;

  /**
   * 默认构造函数，使用UTC时钟
   */
  public SimpleCapacityReplanner() {
    this(new UTCClock());
  }

  /**
   * 测试用构造函数，允许注入自定义时钟
   * @param clock 时钟实例
   */
  @VisibleForTesting
  SimpleCapacityReplanner(Clock clock) {
    this.clock = clock;
  }

  @Override
  public void init(String planQueueName,
      ReservationSchedulerConfiguration conf) {
    // 从配置中加载当前队列的容量检查窗口长度
    this.lengthOfCheckZone = conf.getEnforcementWindow(new QueuePath(planQueueName));
  }

  @Override
  /**
   * 执行容量重规划，删除超限的预留以恢复容量合规
   * @param plan 资源预留规划实例，保存现有预留信息
   * @param contracts 新预留请求列表，本规划器不支持处理新请求
   * @throws PlanningException 规划异常
   */
  public void plan(Plan plan, List<ReservationDefinition> contracts)
      throws PlanningException {

    // 本规划器仅做容量修复，不处理新预留请求
    if (contracts != null) {
      throw new RuntimeException(
          "SimpleCapacityReplanner cannot handle new reservation contracts");
    }

    // 获取资源计算器、集群总容量和当前时间
    ResourceCalculator resCalc = plan.getResourceCalculator();
    Resource totCap = plan.getTotalCapacity();
    long now = clock.getTime();

    // 从当前时间开始，逐时间步检查容量，直到检查窗口结束或所有预留结束
    for (long t = now;
         (t < plan.getLastEndTime() && t < (now + lengthOfCheckZone));
         t += plan.getStep()) {
      // 计算当前时间点超出容量的部分
      Resource excessCap =
          Resources.subtract(plan.getTotalCommittedResources(t), totCap);
      // 如果容量超限，进入删除流程
      if (Resources.greaterThan(resCalc, totCap, excessCap, ZERO_RESOURCE)) {
        // 按接收时间逆序排序，最新的预留排在前面，优先删除新预留
        Set<ReservationAllocation> curReservations =
            new TreeSet<ReservationAllocation>(plan.getReservationsAtTime(t));
        // 迭代删除最新预留，直到容量恢复合规或无预留可删
        for (Iterator<ReservationAllocation> resIter =
            curReservations.iterator(); resIter.hasNext()
            && Resources.greaterThan(resCalc, totCap, excessCap,
                ZERO_RESOURCE);) {
          ReservationAllocation reservation = resIter.next();
          // 从规划中删除该预留
          plan.deleteReservation(reservation.getReservationId());
          // 更新剩余超容量值
          excessCap =
              Resources.subtract(excessCap, reservation.getResourcesAtTime(t));
          LOG.info("Removing reservation " + reservation.getReservationId()
              + " to repair physical-resource constraints in the plan: "
              + plan.getQueueName());
        }
      }
    }
  }
}