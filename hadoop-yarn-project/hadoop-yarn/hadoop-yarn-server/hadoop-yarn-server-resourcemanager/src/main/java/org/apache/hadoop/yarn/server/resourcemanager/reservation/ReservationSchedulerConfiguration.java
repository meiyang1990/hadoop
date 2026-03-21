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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.api.records.ReservationACL;
import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueuePath;

import java.util.Map;

/**
 * 预约调度配置抽象基类，定义YARN预约调度的配置接口和默认值，
 * 为不同调度实现提供统一的预约配置访问抽象。
 */
public abstract class ReservationSchedulerConfiguration extends Configuration {

  /** 默认预约窗口大小，单位毫秒（1天） */
  @InterfaceAudience.Private
  public static final long DEFAULT_RESERVATION_WINDOW = 24*60*60*1000; // 1 day in msec

  /** 默认预约准入策略实现类全限定名 */
  @InterfaceAudience.Private
  public static final String DEFAULT_RESERVATION_ADMISSION_POLICY =
      "org.apache.hadoop.yarn.server.resourcemanager.reservation.CapacityOverTimePolicy";

  /** 默认预约代理实现类全限定名 */
  @InterfaceAudience.Private
  public static final String DEFAULT_RESERVATION_AGENT_NAME =
      "org.apache.hadoop.yarn.server.resourcemanager.reservation.planning.AlignedPlannerWithGreedy";

  /** 默认预约重规划器实现类全限定名 */
  @InterfaceAudience.Private
  public static final String DEFAULT_RESERVATION_PLANNER_NAME =
      "org.apache.hadoop.yarn.server.resourcemanager.reservation.planning.SimpleCapacityReplanner";

  /** 默认预约过期后是否将应用移到父队列 */
  @InterfaceAudience.Private
  public static final boolean DEFAULT_RESERVATION_MOVE_ON_EXPIRY = true;

  // default to 1h lookahead enforcement
  /** 默认预约约束强制检查窗口大小，单位毫秒（1小时） */
  @InterfaceAudience.Private
  public static final long DEFAULT_RESERVATION_ENFORCEMENT_WINDOW = 60*60*1000;
  // 1 hour

  /** 默认是否将预约显示为独立队列 */
  @InterfaceAudience.Private
  public static final boolean DEFAULT_SHOW_RESERVATIONS_AS_QUEUES = false;

  /** 默认容量超时间倍数阈值 */
  @InterfaceAudience.Private
  public static final float DEFAULT_CAPACITY_OVER_TIME_MULTIPLIER = 1;

  public ReservationSchedulerConfiguration() { super(); }

  public ReservationSchedulerConfiguration(
      Configuration configuration) {
    super(configuration);
  }

  /**
   * 检查指定队列是否启用预约调度
   * @param queue 队列路径
   * @return true表示该队列支持预约调度
   */
  public abstract boolean isReservable(QueuePath queue);

  /**
   * 获取指定队列的所有预约ACL权限配置
   * @param queue 队列路径
   * @return 预约ACL类型到访问控制列表的映射
   */
  public abstract Map<ReservationACL, AccessControlList> getReservationAcls(
          QueuePath queue);

  /**
   * 获取共享策略进行有效性检查的时间窗口大小
   * @param queue 队列路径
   * @return 检查窗口大小，单位毫秒
   */
  public long getReservationWindow(QueuePath queue) {
    return DEFAULT_RESERVATION_WINDOW;
  }

  /**
   * 获取预约窗口内允许的平均容量配额
   * @param queue 队列路径
   * @return 平均容量配额（相对于队列总容量的倍数）
   */
  public float getAverageCapacity(QueuePath queue) {
    return DEFAULT_CAPACITY_OVER_TIME_MULTIPLIER;
  }

  /**
   * 获取任意时刻允许的最大瞬时容量
   * @param queue 队列路径
   * @return 最大瞬时容量（相对于队列总容量的倍数）
   */
  public float getInstantaneousMaxCapacity(QueuePath queue) {
    return DEFAULT_CAPACITY_OVER_TIME_MULTIPLIER;
  }

  /**
   * 获取队列关联的预约准入策略实现类名
   * @param queue 队列路径
   * @return 准入策略实现类全限定名
   */
  public String getReservationAdmissionPolicy(QueuePath queue) {
    return DEFAULT_RESERVATION_ADMISSION_POLICY;
  }

  /**
   * 获取队列关联的预约代理实现类名
   * @param queue 队列路径
   * @return 预约代理实现类全限定名
   */
  public String getReservationAgent(QueuePath queue) {
    return DEFAULT_RESERVATION_AGENT_NAME;
  }

  /**
   * 获取是否在UI中显示预约为独立队列
   * @param queuePath 队列路径
   * @return true表示显示预约为独立队列
   */
  public boolean getShowReservationAsQueues(QueuePath queuePath) {
    return DEFAULT_SHOW_RESERVATIONS_AS_QUEUES;
  }

  /**
   * 获取队列关联的预约重规划器实现类名
   * @param queue 队列路径
   * @return 重规划器实现类全限定名
   */
  public String getReplanner(QueuePath queue) {
    return DEFAULT_RESERVATION_PLANNER_NAME;
  }

  /**
   * 获取预约过期后应用的处理策略
   * @param queue 队列路径
   * @return true表示将应用移到父队列，false表示杀死应用
   */
  public boolean getMoveOnExpiry(QueuePath queue) {
    return DEFAULT_RESERVATION_MOVE_ON_EXPIRY;
  }

  /**
   * 获取规划器验证计划满足约束的提前检查窗口大小
   * @param queue 队列路径
   * @return 检查窗口大小，单位毫秒
   */
  public long getEnforcementWindow(QueuePath queue) {
    return DEFAULT_RESERVATION_ENFORCEMENT_WINDOW;
  }
}