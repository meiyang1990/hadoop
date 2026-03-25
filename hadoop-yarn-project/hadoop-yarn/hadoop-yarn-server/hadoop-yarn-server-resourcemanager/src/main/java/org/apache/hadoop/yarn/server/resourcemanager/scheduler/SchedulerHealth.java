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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * YARN资源调度器健康状态统计类，存储调度器各操作的运行统计详情。
 *
 * <p><code>SchedulerHealth</code> 为监控系统提供以下调度运行信息：
 * <ol>
 *   <li>
 *   最近一次调度运行时间戳
 *   </li>
 *   <li>
 *   最近一次调度运行中分配、预留、释放的资源总量
 *   </li>
 *   <li>
 *   最近一次分配、释放、预留、抢占操作的详细信息
 *   </li>
 *   <li>
 *   最近一次调度运行中各类操作的计数
 *   </li>
 *   <li>
 *   RM启动以来各类操作的累计计数（含分配、释放、预留、抢占、满足预留）
 *   </li>
 *</ol>
 *
 */

public class SchedulerHealth {

  /**
   * 存储单次调度操作的详细元信息。
   */
  static public class DetailedInformation {
    long timestamp;
    NodeId nodeId;
    ContainerId containerId;
    String queue;

    public DetailedInformation(long timestamp, NodeId nodeId,
        ContainerId containerId, String queue) {
      this.timestamp = timestamp;
      this.nodeId = nodeId;
      this.containerId = containerId;
      this.queue = queue;
    }

    public long getTimestamp() {
      return timestamp;
    }

    public NodeId getNodeId() {
      return nodeId;
    }

    public ContainerId getContainerId() {
      return containerId;
    }

    public String getQueue() {
      return queue;
    }
  }

  /**
   * 调度操作类型枚举。
   */
  enum Operation {
    ALLOCATION, RELEASE, PREEMPTION, RESERVATION, FULFILLED_RESERVATION
  }

  // 最近一次调度运行时间戳
  private long lastSchedulerRunTime;
  // 最近一次调度运行中各操作对应的资源总量
  private Map<Operation, Resource> lastSchedulerRunDetails;
  // 各操作类型最近一次操作的详细信息
  private Map<Operation, DetailedInformation> lastSchedulerHealthDetails;
  // 最近一次调度运行中各操作的计数
  private Map<Operation, Long> schedulerOperationCounts;
  // RM启动以来各操作的累计计数，从不重置
  private Map<Operation, Long> schedulerOperationAggregateCounts;

  SchedulerHealth() {
    lastSchedulerRunDetails = new ConcurrentHashMap<>();
    lastSchedulerHealthDetails = new ConcurrentHashMap<>();
    schedulerOperationCounts = new ConcurrentHashMap<>();
    schedulerOperationAggregateCounts = new ConcurrentHashMap<>();
    // 初始化所有操作类型的统计数据
    for (Operation op : Operation.values()) {
      lastSchedulerRunDetails.put(op, Resource.newInstance(0, 0));
      schedulerOperationCounts.put(op, 0L);
      lastSchedulerHealthDetails.put(op, new DetailedInformation(0, null, null,
        null));
      schedulerOperationAggregateCounts.put(op, 0L);
    }

  }

  /**
   * 更新最近一次容器分配操作的详细信息。
   * @param timestamp 操作时间戳
   * @param nodeId 分配节点ID
   * @param containerId 分配容器ID
   * @param queue 所属队列
   */
  public void updateAllocation(long timestamp, NodeId nodeId,
      ContainerId containerId, String queue) {
    DetailedInformation di =
        new DetailedInformation(timestamp, nodeId, containerId, queue);
    lastSchedulerHealthDetails.put(Operation.ALLOCATION, di);
  }

  /**
   * 更新最近一次容器释放操作的详细信息。
   * @param timestamp 操作时间戳
   * @param nodeId 释放节点ID
   * @param containerId 释放容器ID
   * @param queue 所属队列
   */
  public void updateRelease(long timestamp, NodeId nodeId,
      ContainerId containerId, String queue) {
    DetailedInformation di =
        new DetailedInformation(timestamp, nodeId, containerId, queue);
    lastSchedulerHealthDetails.put(Operation.RELEASE, di);
  }

  /**
   * 更新最近一次容器抢占操作的详细信息。
   * @param timestamp 操作时间戳
   * @param nodeId 抢占节点ID
   * @param containerId 被抢占容器ID
   * @param queue 所属队列
   */
  public void updatePreemption(long timestamp, NodeId nodeId,
      ContainerId containerId, String queue) {
    DetailedInformation di =
        new DetailedInformation(timestamp, nodeId, containerId, queue);
    lastSchedulerHealthDetails.put(Operation.PREEMPTION, di);
  }

  /**
   * 更新最近一次容器预留操作的详细信息。
   * @param timestamp 操作时间戳
   * @param nodeId 预留节点ID
   * @param containerId 预留容器ID
   * @param queue 所属队列
   */
  public void updateReservation(long timestamp, NodeId nodeId,
      ContainerId containerId, String queue) {
    DetailedInformation di =
        new DetailedInformation(timestamp, nodeId, containerId, queue);
    lastSchedulerHealthDetails.put(Operation.RESERVATION, di);
  }

  /**
   * 更新最近一次调度运行分配和预留的资源总量。
   * @param timestamp 调度运行时间戳
   * @param allocated 本次分配的总资源
   * @param reserved 本次预留的总资源
   */
  public void updateSchedulerRunDetails(long timestamp, Resource allocated,
      Resource reserved) {
    lastSchedulerRunTime = timestamp;
    lastSchedulerRunDetails.put(Operation.ALLOCATION, allocated);
    lastSchedulerRunDetails.put(Operation.RESERVATION, reserved);
  }

  /**
   * 更新最近一次调度运行释放的资源总量。
   * @param timestamp 调度运行时间戳
   * @param released 本次释放的总资源
   */
  public void updateSchedulerReleaseDetails(long timestamp, Resource released) {
    lastSchedulerRunTime = timestamp;
    lastSchedulerRunDetails.put(Operation.RELEASE, released);
  }

  /**
   * 更新最近一次调度运行释放操作计数。
   * @param count 本次释放操作次数
   */
  public void updateSchedulerReleaseCounts(long count) {
    updateCounts(Operation.RELEASE, count);
  }

  /**
   * 更新最近一次调度运行分配操作计数。
   * @param count 本次分配操作次数
   */
  public void updateSchedulerAllocationCounts(long count) {
    updateCounts(Operation.ALLOCATION, count);
  }

  /**
   * 更新最近一次调度运行预留操作计数。
   * @param count 本次预留操作次数
   */
  public void updateSchedulerReservationCounts(long count) {
    updateCounts(Operation.RESERVATION, count);
  }

  /**
   * 更新最近一次调度运行已满足预留操作计数。
   * @param count 本次已满足预留操作次数
   */
  public void updateSchedulerFulfilledReservationCounts(long count) {
    updateCounts(Operation.FULFILLED_RESERVATION, count);
  }

  /**
   * 更新最近一次调度运行抢占操作计数。
   * @param count 本次抢占操作次数
   */
  public void updateSchedulerPreemptionCounts(long count) {
    updateCounts(Operation.PREEMPTION, count);
  }

  /**
   * 更新指定操作的本次计数和累计计数。
   * @param op 操作类型
   * @param count 本次操作次数
   */
  private void updateCounts(Operation op, long count) {
    schedulerOperationCounts.put(op, count);
    Long tmp = schedulerOperationAggregateCounts.get(op);
    schedulerOperationAggregateCounts.put(op, tmp + count);
  }

  /**
   * 获取最近一次调度运行时间戳。
   *
   * @return 最近调度运行时间戳
   */
  public long getLastSchedulerRunTime() {
    return lastSchedulerRunTime;
  }

  private Resource getResourceDetails(Operation op) {
    return lastSchedulerRunDetails.get(op);
  }

  /**
   * 获取最近一次调度运行分配的总资源。
   *
   * @return 分配总资源
   */
  public Resource getResourcesAllocated() {
    return getResourceDetails(Operation.ALLOCATION);
  }

  /**
   * 获取最近一次调度运行预留的总资源。
   *
   * @return 预留总资源
   */
  public Resource getResourcesReserved() {
    return getResourceDetails(Operation.RESERVATION);
  }

  /**
   * 获取最近一次调度运行释放的总资源。
   *
   * @return 释放总资源
   */
  public Resource getResourcesReleased() {
    return getResourceDetails(Operation.RELEASE);
  }

  private DetailedInformation getDetailedInformation(Operation op) {
    return lastSchedulerHealthDetails.get(op);
  }

  /**
   * 获取最近一次分配操作的详细信息。
   *
   * @return 最近分配操作详情
   */
  public DetailedInformation getLastAllocationDetails() {
    return getDetailedInformation(Operation.ALLOCATION);
  }

  /**
   * 获取最近一次释放操作的详细信息。
   *
   * @return 最近释放操作详情
   */
  public DetailedInformation getLastReleaseDetails() {
    return getDetailedInformation(Operation.RELEASE);
  }

  /**
   * 获取最近一次预留操作的详细信息。
   *
   * @return 最近预留操作详情
   */
  public DetailedInformation getLastReservationDetails() {
    return getDetailedInformation(Operation.RESERVATION);
  }

  /**
   * 获取最近一次抢占操作的详细信息。
   *
   * @return 最近抢占操作详情
   */
  public DetailedInformation getLastPreemptionDetails() {
    return getDetailedInformation(Operation.PREEMPTION);
  }

  private Long getOperationCount(Operation op) {
    return schedulerOperationCounts.get(op);
  }

  /**
   * 获取最近一次调度运行分配操作次数。
   *
   * @return 分配操作次数
   */
  public Long getAllocationCount() {
    return getOperationCount(Operation.ALLOCATION);
  }

  /**
   * 获取最近一次调度运行释放操作次数。
   *
   * @return 释放操作次数
   */
  public Long getReleaseCount() {
    return getOperationCount(Operation.RELEASE);
  }

  /**
   * 获取最近一次调度运行预留操作次数。
   *
   * @return 预留操作次数
   */
  public Long getReservationCount() {
    return getOperationCount(Operation.RESERVATION);
  }

  /**
   * 获取最近一次调度运行抢占操作次数。
   *
   * @return 抢占操作次数
   */
  public Long getPreemptionCount() {
    return getOperationCount(Operation.PREEMPTION);
  }

  private Long getAggregateOperationCount(Operation op) {
    return schedulerOperationAggregateCounts.get(op);
  }

  /**
   * 获取RM启动以来累计分配操作次数。
   *
   * @return 累计分配次数
   */
  public Long getAggregateAllocationCount() {
    return getAggregateOperationCount(Operation.ALLOCATION);
  }

  /**
   * 获取RM启动以来累计释放操作次数。
   *
   * @return 累计释放次数
   */
  public Long getAggregateReleaseCount() {
    return getAggregateOperationCount(Operation.RELEASE);
  }

  /**
   * 获取RM启动以来累计预留操作次数。
   *
   * @return 累计预留次数
   */
  public Long getAggregateReservationCount() {
    return getAggregateOperationCount(Operation.RESERVATION);
  }

  /**
   * 获取RM启动以来累计抢占操作次数。
   *
   * @return 累计抢占次数
   */
  public Long getAggregatePreemptionCount() {
    return getAggregateOperationCount(Operation.PREEMPTION);
  }

  /**
   * 获取RM启动以来累计已满足预留操作次数。
   *
   * @return 累计已满足预留次数
   */
  public Long getAggregateFulFilledReservationsCount() {
    return getAggregateOperationCount(Operation.FULFILLED_RESERVATION);
  }
}