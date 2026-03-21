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
package org.apache.hadoop.yarn.server.resourcemanager.reservation.planning;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.Plan;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningException;

/**
 * YARN资源预留代理接口，负责为用户预留请求分配集群资源，满足用户的资源使用约定
 */
public interface ReservationAgent {

  /**
   * 配置项：对于多个同等有效的分配方案，是否优先选择更早开始的分配
   */
  final static String FAVOR_EARLY_ALLOCATION =
      "yarn.resourcemanager.reservation-system.favor-early-allocation";
  /**
   * 默认配置：默认优先选择更早开始的分配
   */
  final static boolean DEFAULT_GREEDY_FAVOR_EARLY_ALLOCATION = true;

  /**
   * 根据用户约定创建新的资源预留
   *
   * @param reservationId 待创建的资源预留ID
   * @param user 发起创建请求的用户
   * @param plan 资源预留计划，本次预留需要嵌入该计划
   * @param contract 用户资源需求定义，包含会话所需资源规格
   *
   * @return 创建操作是否成功
   * @throws PlanningException 当无法将预留嵌入计划时抛出
   */
  public boolean createReservation(ReservationId reservationId, String user,
      Plan plan, ReservationDefinition contract) throws PlanningException;

  /**
   * 根据新约定更新已有资源预留
   *
   * @param reservationId 待更新的资源预留ID
   * @param user 发起更新请求的用户
   * @param plan 资源预留计划，本次更新需要嵌入该计划
   * @param contract 更新后的用户资源需求定义
   *
   * @return 更新操作是否成功
   * @throws PlanningException 当无法将更新后的预留嵌入计划时抛出
   */
  public boolean updateReservation(ReservationId reservationId, String user,
      Plan plan, ReservationDefinition contract) throws PlanningException;

  /**
   * 删除用户指定的资源预留
   *
   * @param reservationId 待删除的资源预留ID
   * @param user 发起删除请求的用户
   * @param plan 资源预留计划所属的计划
   *
   * @return 删除操作是否成功
   * @throws PlanningException 当删除操作失败时抛出
   */
  public boolean deleteReservation(ReservationId reservationId, String user,
      Plan plan) throws PlanningException;

  /**
   * 使用配置初始化代理
   *
   * @param conf Hadoop配置对象
   */
  void init(Configuration conf);

}