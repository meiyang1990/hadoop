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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationSchedulerConfiguration;

/**
 * 公平调度器预留队列的配置容器，存储预留调度相关的配置参数
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class ReservationQueueConfiguration {
  private long reservationWindow;
  private long enforcementWindow;
  private String reservationAdmissionPolicy;
  private String reservationAgent;
  private String planner;
  private boolean showReservationAsQueues;
  private boolean moveOnExpiry;
  private float avgOverTimeMultiplier;
  private float maxOverTimeMultiplier;

  /**
   * 构造函数，使用全局默认值初始化所有配置参数
   */
  public ReservationQueueConfiguration() {
    this.reservationWindow = ReservationSchedulerConfiguration
        .DEFAULT_RESERVATION_WINDOW;
    this.enforcementWindow = ReservationSchedulerConfiguration
        .DEFAULT_RESERVATION_ENFORCEMENT_WINDOW;
    this.reservationAdmissionPolicy = ReservationSchedulerConfiguration
        .DEFAULT_RESERVATION_ADMISSION_POLICY;
    this.reservationAgent = ReservationSchedulerConfiguration
        .DEFAULT_RESERVATION_AGENT_NAME;
    this.planner = ReservationSchedulerConfiguration
        .DEFAULT_RESERVATION_PLANNER_NAME;
    this.showReservationAsQueues = ReservationSchedulerConfiguration
        .DEFAULT_SHOW_RESERVATIONS_AS_QUEUES;
    this.moveOnExpiry = ReservationSchedulerConfiguration
        .DEFAULT_RESERVATION_MOVE_ON_EXPIRY;
    this.avgOverTimeMultiplier = ReservationSchedulerConfiguration
        .DEFAULT_CAPACITY_OVER_TIME_MULTIPLIER;
    this.maxOverTimeMultiplier = ReservationSchedulerConfiguration
        .DEFAULT_CAPACITY_OVER_TIME_MULTIPLIER;
  }

  /**
   * 获取预留窗口大小，单位毫秒
   * @return 预留窗口毫秒数
   */
  public long getReservationWindowMsec() {
    return reservationWindow;
  }

  /**
   * 获取预留强制 enforcement 窗口大小，单位毫秒
   * @return enforcement窗口毫秒数
   */
  public long getEnforcementWindowMsec() {
    return enforcementWindow;
  }

  /**
   * 是否在UI中把预留展示为队列
   * @return  true表示展示，false不展示
   */
  public boolean shouldShowReservationAsQueues() {
    return showReservationAsQueues;
  }

  /**
   * 预留过期后是否自动移动已分配容器
   * @return true表示移动，false不移动
   */
  public boolean shouldMoveOnExpiry() {
    return moveOnExpiry;
  }

  /**
   * 获取预留准入策略类名
   * @return 准入策略类全限定名
   */
  public String getReservationAdmissionPolicy() {
    return reservationAdmissionPolicy;
  }

  /**
   * 获取预留代理类名
   * @return 预留代理类全限定名
   */
  public String getReservationAgent() {
    return reservationAgent;
  }

  /**
   * 获取预留规划器类名
   * @return 规划器类全限定名
   */
  public String getPlanner() {
    return planner;
  }

  /**
   * 获取平均资源过载倍数
   * @return 平均容量过载倍数
   */
  public float getAvgOverTimeMultiplier() {
    return avgOverTimeMultiplier;
  }

  /**
   * 获取最大资源过载倍数
   * @return 最大容量过载倍数
   */
  public float getMaxOverTimeMultiplier() {
    return maxOverTimeMultiplier;
  }

  /**
   * 设置预留规划器类名
   * @param planner 规划器类全限定名
   */
  public void setPlanner(String planner) {
    this.planner = planner;
  }

  /**
   * 设置预留准入策略类名
   * @param reservationAdmissionPolicy 准入策略类全限定名
   */
  public void setReservationAdmissionPolicy(String reservationAdmissionPolicy) {
    this.reservationAdmissionPolicy = reservationAdmissionPolicy;
  }

  /**
   * 设置预留代理类名
   * @param reservationAgent 预留代理类全限定名
   */
  public void setReservationAgent(String reservationAgent) {
    this.reservationAgent = reservationAgent;
  }

  /**
   * 设置预留窗口大小，仅用于测试
   * @param reservationWindow 预留窗口毫秒数
   */
  @VisibleForTesting
  public void setReservationWindow(long reservationWindow) {
    this.reservationWindow = reservationWindow;
  }

  /**
   * 设置平均容量过载倍数，仅用于测试
   * @param averageCapacity 平均容量过载倍数
   */
  @VisibleForTesting
  public void setAverageCapacity(int averageCapacity) {
    this.avgOverTimeMultiplier = averageCapacity;
  }
}