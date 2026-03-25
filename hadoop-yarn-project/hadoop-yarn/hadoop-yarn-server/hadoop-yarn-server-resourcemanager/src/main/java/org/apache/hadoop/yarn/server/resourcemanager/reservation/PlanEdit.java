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

import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningException;

/**
 * YARN 资源调度计划编辑接口，定义了修改资源计划状态的所有方法，属于资源 Reservation 调度模块核心接口。
 * 继承了 PlanView 和 PlanContext，同时提供计划读取和上下文信息能力。
 */
public interface PlanEdit extends PlanContext, PlanView {

  /**
   * 向资源计划中新增一个预约分配实例。
   * 
   * @param reservation 要新增到计划的预约分配实例
   * @param isRecovering 标识该预约是否是故障转移恢复过程中添加的
   * @return 新增成功返回true，否则返回false
   * @throws PlanningException 新增失败时抛出异常
   */
  boolean addReservation(ReservationAllocation reservation,
      boolean isRecovering) throws PlanningException;

  /**
   * 更新资源计划中已存在的预约分配，用于预约重新协商场景。
   * 
   * @param reservation 需要更新的预约分配实例
   * @return 更新成功返回true，否则返回false
   * @throws PlanningException 更新失败时抛出异常
   */
  boolean updateReservation(ReservationAllocation reservation)
      throws PlanningException;

  /**
   * 根据预约ID从资源计划中删除指定预约分配，主要用于垃圾回收场景。
   * 
   * @param reservationID 待删除预约的唯一标识ID
   * @return 删除成功返回true，否则返回false
   * @throws PlanningException 删除失败时抛出异常
   */
  boolean deleteReservation(ReservationId reservationID)
      throws PlanningException;

  /**
   * 归档清理已过期的预约，清理超出归档滑动窗口的已完成/已过期预约。
   * 
   * @param tick 当前时间戳，用于计算归档窗口边界
   * @throws PlanningException 归档清理失败时抛出异常
   */
  void archiveCompletedReservations(long tick) throws PlanningException;

  /**
   * 设置当前资源计划可用的总资源容量。
   * 
   * @param capacity 当前计划分配的总资源
   */
  void setTotalCapacity(Resource capacity);

}