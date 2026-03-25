// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.reservation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.RLESparseResourceAllocation.RLEOperator;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningException;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.planning.Planner;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.planning.ReservationAgent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.UTCClock;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 该类实现了YARN预留系统状态的内存存储，提供对单个预留信息和资源时间维度聚合利用率的高效访问。
 * 是YARN容量调度器预留功能的核心内存数据结构。
 */
public class InMemoryPlan implements Plan {

  private static final Logger LOG = LoggerFactory.getLogger(InMemoryPlan.class);

  private static final Resource ZERO_RESOURCE = Resource.newInstance(0, 0);
  private final RMStateStore rmStateStore;

  // 按预留时间区间排序的所有预留集合
  private TreeMap<ReservationInterval, Set<InMemoryReservationAllocation>> currentReservations =
      new TreeMap<ReservationInterval, Set<InMemoryReservationAllocation>>();

  // 非周期性预留资源的运行长度编码稀疏存储
  private RLESparseResourceAllocation rleSparseVector;

  // 周期性预留资源的运行长度编码稀疏存储
  private PeriodicRLESparseResourceAllocation periodicRle;

  // 按用户分组存储非周期性预留资源分配
  private Map<String, RLESparseResourceAllocation> userResourceAlloc =
      new HashMap<String, RLESparseResourceAllocation>();

  // 按用户分组存储周期性预留资源分配
  private Map<String, RLESparseResourceAllocation> userPeriodicResourceAlloc =
      new HashMap<String, RLESparseResourceAllocation>();

  // 按用户分组存储活跃预留计数
  private Map<String, RLESparseResourceAllocation> userActiveReservationCount =
      new HashMap<String, RLESparseResourceAllocation>();

  // 按ReservationId索引所有预留分配信息
  private Map<ReservationId, InMemoryReservationAllocation> reservationTable =
      new HashMap<ReservationId, InMemoryReservationAllocation>();

  // 读写锁，保障并发读写线程安全
  private final ReentrantReadWriteLock readWriteLock =
      new ReentrantReadWriteLock();
  private final Lock readLock = readWriteLock.readLock();
  private final Lock writeLock = readWriteLock.writeLock();
  private final SharingPolicy policy;
  private final ReservationAgent agent;
  private final long step;
  private final ResourceCalculator resCalc;
  private final Resource minAlloc, maxAlloc;
  private final String queueName;
  private final QueueMetrics queueMetrics;
  private final Planner replanner;
  private final boolean getMoveOnExpiry;
  private final Clock clock;
  private final long maxPeriodicity;

  // 该计划对应队列的总资源容量
  private Resource totalCapacity;

  /**
   * 构造函数，使用默认最大周期配置
   * @param queueMetrics 队列指标统计
   * @param policy 资源共享策略
   * @param agent 预留代理
   * @param totalCapacity 队列总资源容量
   * @param step 时间步长
   * @param resCalc 资源计算器
   * @param minAlloc 最小分配资源
   * @param maxAlloc 最大分配资源
   * @param queueName 队列名称
   * @param replanner 重规划器
   * @param getMoveOnExpiry 过期是否自动移动
   * @param rmContext RM上下文
   */
  public InMemoryPlan(QueueMetrics queueMetrics, SharingPolicy policy,
      ReservationAgent agent, Resource totalCapacity, long step,
      ResourceCalculator resCalc, Resource minAlloc, Resource maxAlloc,
      String queueName, Planner replanner, boolean getMoveOnExpiry,
      RMContext rmContext) {
    this(queueMetrics, policy, agent, totalCapacity, step, resCalc, minAlloc,
        maxAlloc, queueName, replanner, getMoveOnExpiry,
        YarnConfiguration.DEFAULT_RM_RESERVATION_SYSTEM_MAX_PERIODICITY,
        rmContext);
  }

  /**
   * 构造函数，使用默认UTC时钟
   * @param queueMetrics 队列指标统计
   * @param policy 资源共享策略
   * @param agent 预留代理
   * @param totalCapacity 队列总资源容量
   * @param step 时间步长
   * @param resCalc 资源计算器
   * @param minAlloc 最小分配资源
   * @param maxAlloc 最大分配资源
   * @param queueName 队列名称
   * @param replanner 重规划器
   * @param getMoveOnExpiry 过期是否自动移动
   * @param maxPeriodicity 支持的最大周期性
   * @param rmContext RM上下文
   */
  public InMemoryPlan(QueueMetrics queueMetrics, SharingPolicy policy,
      ReservationAgent agent, Resource totalCapacity, long step,
      ResourceCalculator resCalc, Resource minAlloc, Resource maxAlloc,
      String queueName, Planner replanner, boolean getMoveOnExpiry,
      long maxPeriodicity, RMContext rmContext) {
    this(queueMetrics, policy, agent, totalCapacity, step, resCalc, minAlloc,
        maxAlloc, queueName, replanner, getMoveOnExpiry, maxPeriodicity,
        rmContext, new UTCClock());
  }

  /**
   * 全参数构造函数
   * @param queueMetrics 队列指标统计
   * @param policy 资源共享策略
   * @param agent 预留代理
   * @param totalCapacity 队列总资源容量
   * @param step 时间步长
   * @param resCalc 资源计算器
   * @param minAlloc 最小分配资源
   * @param maxAlloc 最大分配资源
   * @param queueName 队列名称
   * @param replanner 重规划器
   * @param getMoveOnExpiry 过期是否自动移动
   * @param maxPeriodicty 支持的最大周期性
   * @param rmContext RM上下文
   * @param clock 时钟实现
   */
  @SuppressWarnings("checkstyle:parameternumber")
  public InMemoryPlan(QueueMetrics queueMetrics, SharingPolicy policy,
      ReservationAgent agent, Resource totalCapacity, long step,
      ResourceCalculator resCalc, Resource minAlloc, Resource maxAlloc,
      String queueName, Planner replanner, boolean getMoveOnExpiry,
      long maxPeriodicty, RMContext rmContext, Clock clock) {
    this.queueMetrics = queueMetrics;
    this.policy = policy;
    this.agent = agent;
    this.step = step;
    this.totalCapacity = totalCapacity;
    this.resCalc = resCalc;
    this.minAlloc = minAlloc;
    this.maxAlloc = maxAlloc;
    this.rleSparseVector = new RLESparseResourceAllocation(resCalc);
    this.maxPeriodicity = maxPeriodicty;
    this.periodicRle =
        new PeriodicRLESparseResourceAllocation(resCalc, this.maxPeriodicity);
    this.queueName = queueName;
    this.replanner = replanner;
    this.getMoveOnExpiry = getMoveOnExpiry;
    this.clock = clock;
    this.rmStateStore = rmContext.getStateStore();
  }

  @Override
  public QueueMetrics getQueueMetrics() {
    return queueMetrics;
  }

  /**
   * 获取对应用户和周期类型的资源分配RLE存储
   * @param user 用户名
   * @param period 周期值，大于0表示周期性预留
   * @return 对应的RLE资源分配存储
   */
  private RLESparseResourceAllocation getUserRLEResourceAllocation(String user,
      long period) {
    RLESparseResourceAllocation resAlloc = null;
    if (period > 0) {
      if (userPeriodicResourceAlloc.containsKey(user)) {
        resAlloc = userPeriodicResourceAlloc.get(user);
      } else {
        resAlloc = new PeriodicRLESparseResourceAllocation(resCalc,
            periodicRle.getTimePeriod());
        userPeriodicResourceAlloc.put(user, resAlloc);
      }
    } else {
      if (userResourceAlloc.containsKey(user)) {
        resAlloc = userResourceAlloc.get(user);
      } else {
        resAlloc = new RLESparseResourceAllocation(resCalc);
        userResourceAlloc.put(user, resAlloc);
      }
    }
    return resAlloc;
  }

  /**
   * 清理用户空闲的RLE资源分配存储，避免内存泄漏
   * @param user 用户名
   * @param period 周期值
   */
  private void gcUserRLEResourceAllocation(String user, long period) {
    if (period > 0) {
      if (userPeriodicResourceAlloc.get(user).isEmpty()) {
        userPeriodicResourceAlloc.remove(user);
      }
    } else {
      if (userResourceAlloc.get(user).isEmpty()) {
        userResourceAlloc.remove(user);
      }
    }
  }

  /**
   * 增加新预留的资源分配，更新所有聚合统计数据
   * @param reservation 要添加的预留分配
   */
  private void incrementAllocation(ReservationAllocation reservation) {
    assert (readWriteLock.isWriteLockedByCurrentThread());
    Map<ReservationInterval, Resource> allocationRequests =
        reservation.getAllocationRequests();
    // 获取用户名和周期信息
    String user = reservation.getUser();
    long period = reservation.getPeriodicity();
    // 获取对应用户的RLE存储
    RLESparseResourceAllocation resAlloc =
        getUserRLEResourceAllocation(user, period);

    // 获取用户活跃预留计数存储
    RLESparseResourceAllocation resCount = userActiveReservationCount.get(user);
    if (resCount == null) {
      resCount = new RLESparseResourceAllocation(resCalc);
      userActiveReservationCount.put(user, resCount);
    }

    // 计算预留最早和最晚活跃时间
    long earliestActive = Long.MAX_VALUE;
    long latestActive = Long.MIN_VALUE;

    // 遍历所有时间区间分配
    for (Map.Entry<ReservationInterval, Resource> r : allocationRequests
        .entrySet()) {

      if (period > 0L) {
        // 周期性预留，按周期展开所有区间
        for (int i = 0; i < periodicRle.getTimePeriod() / period; i++) {

          long rStart = r.getKey().getStartTime() + i * period;
          long rEnd = r.getKey().getEndTime() + i * period;

          // 处理跨周期边界环绕情况
          if (rEnd > periodicRle.getTimePeriod()) {
            long diff = rEnd - periodicRle.getTimePeriod();
            rEnd = periodicRle.getTimePeriod();
            ReservationInterval newInterval = new ReservationInterval(0, diff);
            periodicRle.addInterval(newInterval, r.getValue());
            resAlloc.addInterval(newInterval, r.getValue());
          }

          ReservationInterval newInterval =
              new ReservationInterval(rStart, rEnd);
          periodicRle.addInterval(newInterval, r.getValue());
          resAlloc.addInterval(newInterval, r.getValue());
        }

      } else {
        // 非周期性预留，直接添加到全局和用户存储
        rleSparseVector.addInterval(r.getKey(), r.getValue());
        resAlloc.addInterval(r.getKey(), r.getValue());
        if (Resources.greaterThan(resCalc, totalCapacity, r.getValue(),
            ZERO_RESOURCE)) {
          earliestActive = Math.min(earliestActive, r.getKey().getStartTime());
          latestActive = Math.max(latestActive, r.getKey().getEndTime());
        }
      }
    }
    // 周期性预留自开始时间起一直活跃直到被取消
    if (period > 0L) {
      earliestActive = reservation.getStartTime();
      latestActive = Long.MAX_VALUE;
    }
    // 更新活跃预留计数
    resCount.addInterval(new ReservationInterval(earliestActive, latestActive),
        Resource.newInstance(1, 1));
  }

  /**
   * 减少已删除预留的资源分配，更新所有聚合统计数据
   * @param reservation 要删除的预留分配
   */
  private void decrementAllocation(ReservationAllocation reservation) {
    assert (readWriteLock.isWriteLockedByCurrentThread());
    Map<ReservationInterval, Resource> allocationRequests =
        reservation.getAllocationRequests();
    String user = reservation.getUser();
    long period = reservation.getPeriodicity();
    RLESparseResourceAllocation resAlloc =
        getUserRLEResourceAllocation(user, period);

    long earliestActive = Long.MAX_VALUE;
    long latestActive = Long.MIN_VALUE;
    // 遍历所有时间区间移除分配
    for (Map.Entry<ReservationInterval, Resource> r : allocationRequests
        .entrySet()) {
      if (period > 0L) {
        // 周期性预留，按周期展开移除
        for (int i = 0; i < periodicRle.getTimePeriod() / period; i++) {

          long rStart = r.getKey().getStartTime() + i * period;
          long rEnd = r.getKey().getEndTime() + i * period;

          // 处理跨周期边界环绕情况
          if (rEnd > periodicRle.getTimePeriod()) {
            long diff = rEnd - periodicRle.getTimePeriod();
            rEnd = periodicRle.getTimePeriod();
            ReservationInterval newInterval = new ReservationInterval(0, diff);
            periodicRle.removeInterval(newInterval, r.getValue());
            resAlloc.removeInterval(newInterval, r.getValue());
          }

          ReservationInterval newInterval =
              new ReservationInterval(rStart, rEnd);
          periodicRle.removeInterval(newInterval, r.getValue());
          resAlloc.removeInterval(newInterval, r.getValue());
        }
      } else {
        // 非周期性预留，直接从全局和用户存储移除
        rleSparseVector.removeInterval(r.getKey(), r.getValue());
        resAlloc.removeInterval(r.getKey(), r.getValue());
        if (Resources.greaterThan(resCalc, totalCapacity, r.getValue(),
            ZERO_RESOURCE)) {
          earliestActive = Math.min(earliestActive, r.getKey().getStartTime());
          latestActive = Math.max(latestActive, r.getKey().getEndTime());
        }
      }
    }
    // 清理空闲的用户存储
    gcUserRLEResourceAllocation(user, period);

    RLESparseResourceAllocation resCount = userActiveReservationCount.get(user);
    // 周期性预留自开始时间起一直活跃直到被取消
    if (period > 0L) {
      earliestActive = reservation.getStartTime();
      latestActive = Long.MAX_VALUE;
    }
    // 减少活跃预留计数
    resCount.removeInterval(
        new ReservationInterval(earliestActive, latestActive),
        Resource.newInstance(1, 1));
    // 清理空闲的计数存储
    if (resCount.isEmpty()) {
      userActiveReservationCount.remove(user);
    }
  }

  /**
   * 获取所有预留集合
   * @return 所有预留分配的不可变集合
   */
  public Set<ReservationAllocation> getAllReservations() {
    readLock.lock();
    try {
      if (currentReservations != null) {
        Set<ReservationAllocation> flattenedReservations =
            new TreeSet<ReservationAllocation>();
        for (Set<InMemoryReservationAllocation> res : currentReservations
            .values()) {
          flattenedReservations.addAll(res);
        }
        return flattenedReservations;
      } else {
        return null;
      }
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public boolean addReservation(ReservationAllocation reservation,
      boolean isRecovering) throws PlanningException {
    // 验证预留类型是否为内存实现
    InMemoryReservationAllocation inMemReservation =
        (InMemoryReservationAllocation) reservation;
    if (inMemReservation.getUser() == null) {
      String errMsg = "The specified Reservation with ID "
          + inMemReservation.getReservationId() + "