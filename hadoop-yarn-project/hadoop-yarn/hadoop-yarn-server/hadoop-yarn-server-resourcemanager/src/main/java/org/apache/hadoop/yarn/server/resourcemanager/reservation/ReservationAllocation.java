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

import java.util.Map;

import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.Resource;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * YARN资源预留分配接口，代表满足用户预留定义的具体资源时间分配结果。
 * 由Plan内部使用，用于存储所有已接受预留请求的资源分配信息。
 */
public interface ReservationAllocation
    extends Comparable<ReservationAllocation> {

  /**
   * 获取当前预留的唯一标识ID
   * 
   * @return 当前预留的ReservationId
   */
  ReservationId getReservationId();

  /**
   * 获取客户端提交的原始预留定义
   * 
   * @return 客户端提交的原始预留定义
   */
  ReservationDefinition getReservationDefinition();

  /**
   * 获取预留开始生效时间
   * 
   * @return 预留激活时间（毫秒时间戳）
   */
  long getStartTime();

  /**
   * 获取预留结束时间
   * 
   * @return 预留终止时间（毫秒时间戳）
   */
  long getEndTime();

  /**
   * 获取每个时间区间对应的资源分配请求映射
   * 
   * @return 时间区间到请求资源的映射表
   */
  Map<ReservationInterval, Resource> getAllocationRequests();

  /**
   * 获取当前预留所属计划的标识字符串
   * 
   * @return 当前预留所属计划名称
   */
  String getPlanName();

  /**
   * 获取提交预留请求的用户名
   * 
   * @return 请求预留的用户名
   */
  String getUser();

  /**
   * 判断当前预留是否包含同批任务（Gang）语义
   * 
   * @return true表示是同批请求，false否则
   */
  boolean containsGangs();

  /**
   * 设置预留被系统接受的时间戳
   * 
   * @param acceptedAt 预留被系统接受的时间戳
   */
  void setAcceptanceTimestamp(long acceptedAt);

  /**
   * 获取预留被系统接受的时间戳
   * 
   * @return 预留被系统接受的时间戳
   */
  long getAcceptanceTime();

  /**
   * 获取指定时间点上该预留占用的资源总量
   * 
   * @param tick 指定时间点（UTC毫秒时间戳）
   * @return 指定时间点上该预留占用的资源
   */
  Resource getResourcesAtTime(long tick);

  /**
   * 获取使用游程编码（RLE）表示的全时段资源分配
   *
   * @return 全时段资源分配的RLE稀疏表示
   */
  RLESparseResourceAllocation getResourcesOverTime();


  /**
   * 获取指定时间区间内使用游程编码（RLE）表示的资源分配
   *
   * @param start 时间区间起始时间
   * @param end 时间区间结束时间
   * @return 指定区间内资源分配的RLE稀疏表示
   */
  RLESparseResourceAllocation getResourcesOverTime(long start, long end);

  /**
   * 获取当前预留的周期，代表周期性作业的重复间隔。
   * 周期性作业单位为毫秒，非周期性作业周期为0。
   *
   * @return 当前预留的周期（毫秒）
   */
  long getPeriodicity();

  /**
   * 设置当前预留的周期，代表周期性作业的重复间隔。
   * 周期性作业单位为毫秒，非周期性作业周期为0。
   *
   * @param period 当前预留的周期（毫秒）
   */
  @VisibleForTesting
  void setPeriodicity(long period);

}