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

import java.util.Set;

import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningException;

/**
 * 文件说明：YARN资源预留计划只读视图接口，定义了查询已分配预留资源的方法集合
 * 核心职责：为预留代理查询空闲资源、为规划同步策略发布计划提供只读查询能力，隔离修改操作保证计划一致性
 */
interface PlanView extends PlanContext {

  /**
   * 根据指定条件查询特定用户的预留分配集合
   * @param reservationID 目标预留ID，若指定则过滤匹配该ID的预留
   * @param interval 查询的时间区间，仅保留与该区间重叠的预留
   * @param user 目标用户名，仅返回该用户的预留
   * @return 符合条件的预留分配集合
   */
  Set<ReservationAllocation> getReservations(ReservationId reservationID,
      ReservationInterval interval, String user);

  /**
   * 根据指定条件查询所有用户的预留分配集合
   * @param reservationID 目标预留ID，若指定则过滤匹配该ID的预留
   * @param interval 查询的时间区间，仅保留与该区间重叠的预留
   * @return 符合条件的预留分配集合
   */
  Set<ReservationAllocation> getReservations(ReservationId reservationID,
      ReservationInterval interval);

  /**
   * 根据预留ID查询唯一的预留分配
   * @param reservationID 目标预留唯一ID
   * @return 对应ID的预留分配，不存在则返回null
   */
  ReservationAllocation getReservationById(ReservationId reservationID);

  /**
   * 查询指定时间点指定用户的所有活跃预留分配
   * @param user 目标用户名
   * @param t 指定时间点（UTC毫秒）
   * @return 该用户在该时间点活跃的预留分配集合
   */
  Set<ReservationAllocation> getReservationByUserAtTime(String user, long t);

  /**
   * 查询指定时间点所有活跃预留分配
   * @param tick 指定时间点（UTC毫秒）
   * @return 该时间点所有活跃预留分配集合
   */
  Set<ReservationAllocation> getReservationsAtTime(long tick);

  /**
   * 查询当前计划中所有预留分配
   * @return 当前计划所有预留分配集合
   */
  Set<ReservationAllocation> getAllReservations();

  /**
   * 获取指定时间点所有预留已占用的总资源量
   * @param tick 指定时间点（UTC毫秒）
   * @return 指定时间点已提交预留占用的总资源
   */
  Resource getTotalCommittedResources(long tick);

  /**
   * 获取当前计划的总资源容量（通常对应对应队列的绝对容量）
   * @return 当前计划可分配的总资源容量
   */
  Resource getTotalCapacity();

  /**
   * 获取计划中最早的预留开始时间
   * @return 最早开始时间（UTC毫秒）
   */
  long getEarliestStartTime();

  /**
   * 获取计划中最晚的预留结束时间
   * @return 最晚结束时间（UTC毫秒）
   */
  long getLastEndTime();

  /**
   * 计算指定时间范围内指定用户的可用资源分布，支持周期性预留计算
   * @param user 目标用户名
   * @param oldId 需排除的已有预留ID（更新预留时使用，计算移除该预留后的可用资源）
   * @param start 时间区间起始（UTC毫秒）
   * @param end 时间区间结束（UTC毫秒）
   * @param period 周期性预留周期（毫秒），非零表示周期性查询，返回所有周期窗口中的最小可用资源
   * @return 运行长度编码的可用资源时间分布
   * @throws PlanningException 计算可用资源失败时抛出
   */
  RLESparseResourceAllocation getAvailableResourceOverTime(String user,
      ReservationId oldId, long start, long end, long period)
      throws PlanningException;

  /**
   * 获取指定时间范围内指定用户的预留数量时间分布
   * @param user 目标用户名
   * @param start 时间区间起始（UTC毫秒）
   * @param end 时间区间结束（UTC毫秒）
   * @return 运行长度编码的预留数量时间分布
   */
  RLESparseResourceAllocation getReservationCountForUserOverTime(String user,
      long start, long end);

  /**
   * 获取指定时间范围内指定用户的资源消耗时间分布
   * @param user 目标用户名
   * @param start 时间区间起始（UTC毫秒）
   * @param end 时间区间结束（UTC毫秒）
   * @return 运行长度编码的资源消耗时间分布
   */
  RLESparseResourceAllocation getConsumptionForUserOverTime(String user,
      long start, long end);

  /**
   * 获取指定时间区间内的累计负载分布
   * @param start 时间区间起始（UTC毫秒）
   * @param end 时间区间结束（UTC毫秒）
   * @return 运行长度编码的累计负载时间分布
   * @throws PlanningException 计算累计负载失败时抛出
   */
  RLESparseResourceAllocation getCumulativeLoadOverTime(long start, long end)
      throws PlanningException;

}