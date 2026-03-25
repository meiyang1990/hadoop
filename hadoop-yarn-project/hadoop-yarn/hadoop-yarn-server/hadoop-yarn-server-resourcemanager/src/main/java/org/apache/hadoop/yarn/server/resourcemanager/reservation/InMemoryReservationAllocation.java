// 这个文件已经全部加上中文注释
/*******************************************************************************
 *   Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *******************************************************************************/
package org.apache.hadoop.yarn.server.resourcemanager.reservation;

import java.util.Collections;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

/**
 * YARN资源预留分配的内存存储实现，基于RLESparseResourceAllocation实现时间维度资源分配存储
 * 负责保存单个资源预留的元数据和时间-资源映射关系
 */
public class InMemoryReservationAllocation implements ReservationAllocation {

  private final String planName;
  private final ReservationId reservationID;
  private final String user;
  private final ReservationDefinition contract;
  private final long startTime;
  private final long endTime;
  private final Map<ReservationInterval, Resource> allocationRequests;
  private boolean hasGang = false;
  private long acceptedAt = -1;
  private long periodicity = 0;

  private RLESparseResourceAllocation resourcesOverTime;

  /**
   * 构造一次性资源预留分配实例
   * @param reservationID 预留ID
   * @param contract 预留定义
   * @param user 提交用户
   * @param planName 所属计划名称
   * @param startTime 预留开始时间
   * @param endTime 预留结束时间
   * @param allocations 时间段-资源分配请求映射
   * @param calculator 资源计算器
   * @param minAlloc 最小分配单元
   */
  public InMemoryReservationAllocation(ReservationId reservationID,
      ReservationDefinition contract, String user, String planName,
      long startTime, long endTime,
      Map<ReservationInterval, Resource> allocations,
      ResourceCalculator calculator, Resource minAlloc) {
    this(reservationID, contract, user, planName, startTime, endTime,
        allocations, calculator, minAlloc, false);
  }

  /**
   * 构造资源预留分配实例，支持Gang调度和周期性预留
   * @param reservationID 预留ID
   * @param contract 预留定义
   * @param user 提交用户
   * @param planName 所属计划名称
   * @param startTime 预留开始时间
   * @param endTime 预留结束时间
   * @param allocations 时间段-资源分配请求映射
   * @param calculator 资源计算器
   * @param minAlloc 最小分配单元
   * @param hasGang 是否包含Gang调度任务
   */
  public InMemoryReservationAllocation(ReservationId reservationID,
      ReservationDefinition contract, String user, String planName,
      long startTime, long endTime,
      Map<ReservationInterval, Resource> allocations,
      ResourceCalculator calculator, Resource minAlloc, boolean hasGang) {
    this.contract = contract;
    this.startTime = startTime;
    this.endTime = endTime;
    this.reservationID = reservationID;
    this.user = user;
    this.allocationRequests = allocations;
    this.planName = planName;
    this.hasGang = hasGang;
    // 从预留定义解析周期性参数
    if (contract != null && contract.getRecurrenceExpression() != null) {
      this.periodicity = Long.parseLong(contract.getRecurrenceExpression());
    }
    // 根据是否周期性选择不同的存储实现
    if (periodicity > 0) {
      resourcesOverTime =
          new PeriodicRLESparseResourceAllocation(calculator, periodicity);
    } else {
      resourcesOverTime = new RLESparseResourceAllocation(calculator);
    }
    // 将所有分配时间段加入资源分配存储
    for (Map.Entry<ReservationInterval, Resource> r : allocations.entrySet()) {
      resourcesOverTime.addInterval(r.getKey(), r.getValue());
    }
  }

  @Override
  public ReservationId getReservationId() {
    return reservationID;
  }

  @Override
  public ReservationDefinition getReservationDefinition() {
    return contract;
  }

  @Override
  public long getStartTime() {
    return startTime;
  }

  @Override
  public long getEndTime() {
    return endTime;
  }

  @Override
  public Map<ReservationInterval, Resource> getAllocationRequests() {
    return Collections.unmodifiableMap(allocationRequests);
  }

  @Override
  public String getPlanName() {
    return planName;
  }

  @Override
  public String getUser() {
    return user;
  }

  @Override
  public boolean containsGangs() {
    return hasGang;
  }

  @Override
  public void setAcceptanceTimestamp(long acceptedAt) {
    this.acceptedAt = acceptedAt;
  }

  @Override
  public long getAcceptanceTime() {
    return acceptedAt;
  }

  @Override
  public Resource getResourcesAtTime(long tick) {
    // 超出预留时间范围返回0资源
    if (tick < startTime || tick >= endTime) {
      return Resource.newInstance(0, 0);
    }
    // 返回该时间点的资源量副本
    return Resources.clone(resourcesOverTime.getCapacityAtTime(tick));
  }

  @Override
  public RLESparseResourceAllocation getResourcesOverTime() {
    return resourcesOverTime;
  }

  @Override
  public RLESparseResourceAllocation getResourcesOverTime(long start,
      long end) {
    // 返回指定时间范围内的资源分配
    return resourcesOverTime.getRangeOverlapping(start, end);
  }

  @Override
  public long getPeriodicity() {
    return periodicity;
  }

  @Override
  public void setPeriodicity(long period) {
    periodicity = period;
  }

  @Override
  public String toString() {
    StringBuilder sBuf = new StringBuilder();
    sBuf.append(getReservationId()).append(" user:").append(getUser())
        .append(" startTime: ").append(getStartTime()).append(" endTime: ")
        .append(getEndTime()).append(" Periodiciy: ").append(periodicity)
        .append(" alloc:\n[").append(resourcesOverTime.toString()).append("] ");
    return sBuf.toString();
  }

  /**
   * 按接受时间降序比较，先接受的预留优先级更高
   * 接受时间相同则按ID降序排列
   */
  @Override
  public int compareTo(ReservationAllocation other) {
    // reverse order of acceptance
    if (this.getAcceptanceTime() > other.getAcceptanceTime()) {
      return -1;
    }
    if (this.getAcceptanceTime() < other.getAcceptanceTime()) {
      return 1;
    }
    if (this.getReservationId().getId() > other.getReservationId().getId()) {
      return -1;
    }
    if (this.getReservationId().getId() < other.getReservationId().getId()) {
      return 1;
    }
    return 0;
  }

  @Override
  public int hashCode() {
    return reservationID.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    InMemoryReservationAllocation other = (InMemoryReservationAllocation) obj;
    return this.reservationID.equals(other.getReservationId());
  }

}