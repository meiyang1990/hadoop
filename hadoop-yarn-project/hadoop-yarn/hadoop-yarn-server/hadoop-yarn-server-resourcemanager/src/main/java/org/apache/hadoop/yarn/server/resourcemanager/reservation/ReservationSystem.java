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

import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.Recoverable;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.security.ReservationsACLsManager;

/**
 * 预留资源系统核心接口，负责支持未来资源预留功能。
 * 实现类需要完成以下核心职责：
 * 1. 完成所有已配置的{@link Plan}、对应{@code ReservationAgent}和{@link SharingPolicy}的初始化
 * 2. 管理{@link PlanFollower}，保证{@link Plan}和当前{@link ResourceScheduler}状态同步
 */
@LimitedPrivate("yarn")
@Unstable
public interface ReservationSystem extends Recoverable {

  /**
   * 设置预留系统的RM上下文对象。
   * 需要在实例化预留系统后立即调用一次。
   * 
   * @param rmContext ResourceManager创建的RM上下文对象
   */
  void setRMContext(RMContext rmContext);

  /**
   * 重新初始化预留系统。
   * 
   * @param conf 配置对象
   * @param rmContext ResourceManager当前上下文
   * @throws YarnException 当已配置计划初始化失败时抛出异常
   */
  void reinitialize(Configuration conf, RMContext rmContext)
      throws YarnException;

  /**
   * 获取已初始化的计划实例。
   * 
   * @param planName 计划名称
   * @return 对应名称的计划实例
   * 
   */
  Plan getPlan(String planName);

  /**
   * 获取当前预留系统已知的所有计划，主要供UI展示使用。
   * 
   * @return 计划名称-计划实例的映射表
   */
  Map<String, Plan> getAllPlans();

  /**
   * 调用PlanFollower同步指定计划和ResourceScheduler的资源状态。
   * 
   * @param planName 需要同步的计划名称
   * @param shouldReplan 如果为true，计划容量缩减时重新规划预留；如果为false，按比例缩减所有预留
   */
  void synchronizePlan(String planName, boolean shouldReplan);

  /**
   * 获取PlanFollower同步调度的时间步长（毫秒）。
   * 
   * @return PlanFollower调用的时间步长（毫秒）
   */
  long getPlanFollowerTimeStep();

  /**
   * 生成一个新的唯一预留ID。
   * 
   * @return 新的唯一预留ID
   * 
   */
  ReservationId getNewReservationId();

  /**
   * 获取指定预留ID关联的队列名称。
   * 
   * @param reservationId 预留唯一ID
   * @return 关联队列的名称
   * 
   */
  String getQueueForReservation(ReservationId reservationId);

  /**
   * 关联指定预留ID和队列。
   * 
   * @param reservationId 预留唯一ID
   * @param queueName 需要关联的队列名称
   * 
   */
  void setQueueForReservation(ReservationId reservationId, String queueName);

  /**
   * 获取预留ACL管理器，用于检查用户对预留的访问权限。
   *
   * @return 用于检查预留ACL的预留ACL管理器
   *
   */
  ReservationsACLsManager getReservationsACLsManager();
}