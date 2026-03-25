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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import java.io.IOException;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationSystem;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerDynamicEditException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.QueueEntitlement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 容量调度器中代表预约容量的动态叶子队列，由ReservationSystem预约系统管理
 * 继承AbstractAutoCreatedLeafQueue，用于支持资源预约调度功能
 *
 */
public class ReservationQueue extends AbstractAutoCreatedLeafQueue {
  private static final Logger LOG =
      LoggerFactory.getLogger(ReservationQueue.class);

  // 当前预约队列所属的父PlanQueue
  private PlanQueue parent;

  /**
   * 构造ReservationQueue实例，初始化队列配置
   * @param queueContext 容量调度器队列上下文
   * @param queueName 队列名称
   * @param parent 父PlanQueue队列
   * @throws IOException 初始化失败时抛出异常
   */
  public ReservationQueue(CapacitySchedulerQueueContext queueContext, String queueName,
      PlanQueue parent) throws IOException {
    super(queueContext, queueName, parent, null);
    super.setupQueueConfigs(queueContext.getClusterResource());

    // 从父PlanQueue继承所有预约队列通用的配额参数
    updateQuotas(parent.getUserLimitForReservation(),
        parent.getUserLimitFactor(),
        parent.getMaxApplicationsForReservations(),
        parent.getMaxApplicationsPerUserForReservation());
    this.parent = parent;
  }

  @Override
  public void reinitialize(CSQueue newlyParsedQueue,
      Resource clusterResource) throws IOException {
    // 获取写锁保证重初始化线程安全
    writeLock.lock();
    try {
      // 合法性校验，确保队列类型和路径匹配
      if (!(newlyParsedQueue instanceof ReservationQueue) || !newlyParsedQueue
          .getQueuePath().equals(getQueuePath())) {
        throw new IOException(
            "Trying to reinitialize " + getQueuePath() + " from "
                + newlyParsedQueue.getQueuePath());
      }
      // 调用父类完成基础重初始化
      super.reinitialize(newlyParsedQueue, clusterResource);
      // 更新队列统计信息
      CSQueueUtils.updateQueueStatistics(resourceCalculator, clusterResource,
          this, labelManager, null);

      // 重新从父队列更新配额参数
      updateQuotas(parent.getUserLimitForReservation(),
          parent.getUserLimitFactor(),
          parent.getMaxApplicationsForReservations(),
          parent.getMaxApplicationsPerUserForReservation());
    } finally {
      // 释放写锁
      writeLock.unlock();
    }
  }

  /**
   * 初始化预约队列的权限配额，预约队列占用全部父计划配额
   * @throws SchedulerDynamicEditException 编辑失败时抛出异常
   */
  public void initializeEntitlements() throws SchedulerDynamicEditException {
    setEntitlement(new QueueEntitlement(1.0f, 1.0f));
  }

  /**
   * 更新预约队列各项配额参数
   * @param userLimit 用户资源限制
   * @param userLimitFactor 用户限制系数
   * @param maxAppsForReservation 队列最大应用数
   * @param maxAppsPerUserForReservation 单用户最大应用数
   */
  private void updateQuotas(float userLimit, float userLimitFactor,
      int maxAppsForReservation, int maxAppsPerUserForReservation) {
    setUserLimit(userLimit);
    setUserLimitFactor(userLimitFactor);
    setMaxApplications(maxAppsForReservation);
    maxApplicationsPerUser = maxAppsPerUserForReservation;
  }

  @Override
  protected void setupConfigurableCapacities() {
    // 直接更新绝对容量，预约队列使用固定容量计算
    super.updateAbsoluteCapacities();
  }
}