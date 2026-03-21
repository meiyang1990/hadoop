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
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationConstants;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationSystem;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerDynamicEditException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 文件说明：容量调度器中由预留资源系统管理的动态计划队列，代表一个可动态创建预留子队列的父队列
 * 核心职责：为资源预留系统提供父队列容器，管理其子预留队列的资源与配额，对用户而言等价于支持预留的叶子队列
 * This represents a dynamic queue managed by the {@link ReservationSystem}.
 * From the user perspective this is equivalent to a LeafQueue that respect
 * reservations, but functionality wise is a sub-class of ParentQueue
 *
 */
public class PlanQueue extends AbstractManagedParentQueue {

  private static final Logger LOG = LoggerFactory.getLogger(PlanQueue.class);

  private int maxAppsForReservation;
  private int maxAppsPerUserForReservation;
  private float userLimit;
  private float userLimitFactor;
  private boolean showReservationsAsQueues;

  /**
   * 构造函数：初始化计划队列，加载配置并初始化配额
   * @param queueContext 容量调度器队列上下文
   * @param queueName 队列名称
   * @param parent 父队列
   * @param old 旧队列对象（用于重新初始化）
   * @throws IOException 初始化异常
   */
  public PlanQueue(CapacitySchedulerQueueContext queueContext, String queueName,
      CSQueue parent, CSQueue old) throws IOException {
    super(queueContext, queueName, parent, old);
    super.setupQueueConfigs(queueContext.getClusterResource());
    updateAbsoluteCapacities();

    // 加载计划队列的预留队列属性配置
    CapacitySchedulerConfiguration conf = queueContext.getConfiguration();
    QueuePath queuePath = super.getQueuePathObject();
    int maxAppsForReservation = conf.getMaximumApplicationsPerQueue(queuePath);
    showReservationsAsQueues = conf.getShowReservationAsQueues(queuePath);
    // 配置小于0时使用默认值，按集群总容量比例计算
    if (maxAppsForReservation < 0) {
      maxAppsForReservation =
          (int) (CapacitySchedulerConfiguration.
              DEFAULT_MAXIMUM_SYSTEM_APPLICATIIONS * super
              .getAbsoluteCapacity());
    }
    // 加载用户限制配置
    float configuredUserLimit = conf.getUserLimit(queuePath);
    float configuredUserLimitFactor = conf.getUserLimitFactor(queuePath);
    // 计算单用户最大预留应用数：总预留应用数 * 用户限制占比 * 用户限制因子
    int configuredMaxAppsPerUserForReservation =
        (int) (maxAppsForReservation * (configuredUserLimit / 100.0f) *
            configuredUserLimitFactor);
    // 用户限制因子为-1时，允许用户占满所有预留配额
    if (configuredUserLimitFactor == -1) {
      configuredMaxAppsPerUserForReservation = maxAppsForReservation;
    }
    // 更新当前队列的配额值
    updateQuotas(configuredUserLimit, configuredUserLimitFactor,
        maxAppsForReservation, configuredMaxAppsPerUserForReservation);

    // 记录队列创建日志
    StringBuilder queueInfo = new StringBuilder();
    queueInfo.append("Created Plan Queue: ").append(queueName)
        .append("\nwith capacity: [").append(super.getCapacity())
        .append("]\nwith max capacity: [").append(super.getMaximumCapacity())
        .append("\nwith max reservation apps: [").append(maxAppsForReservation)
        .append("]\nwith max reservation apps per user: [")
        .append(configuredMaxAppsPerUserForReservation)
        .append("]\nwith user limit: [")
        .append(configuredUserLimit).append("]\nwith user limit factor: [")
        .append(configuredUserLimitFactor).append("].");
    LOG.info(queueInfo.toString());
  }

  @Override
  public void reinitialize(CSQueue newlyParsedQueue,
      Resource clusterResource) throws IOException {
    // 获取写锁保证配置更新线程安全
    writeLock.lock();
    try {
      // 类型与路径合法性检查
      if (!(newlyParsedQueue instanceof PlanQueue) || !newlyParsedQueue
          .getQueuePath().equals(getQueuePath())) {
        throw new IOException(
            "Trying to reinitialize " + getQueuePath() + " from "
                + newlyParsedQueue.getQueuePath());
      }

      PlanQueue newlyParsedParentQueue = (PlanQueue) newlyParsedQueue;

      // 计划队列配置中只允许存在默认预留队列，不允许用户自定义子队列
      if (newlyParsedParentQueue.getChildQueues().size() != 1) {
        throw new IOException(
            "Reservable Queue should not have sub-queues in the"
                + "configuration expect the default reservation queue");
      }

      // 重新加载队列基础配置
      setupQueueConfigs(clusterResource);

      // 更新预留配额配置
      updateQuotas(newlyParsedParentQueue.userLimit,
          newlyParsedParentQueue.userLimitFactor,
          newlyParsedParentQueue.maxAppsForReservation,
          newlyParsedParentQueue.maxAppsPerUserForReservation);

      // 触发所有子预留队列重新初始化，重新计算绝对容量
      for (CSQueue res : this.getChildQueues()) {
        res.reinitialize(res, clusterResource);
      }
      // 更新预留队列显示配置
      showReservationsAsQueues =
          newlyParsedParentQueue.showReservationsAsQueues;
    } finally {
      // 释放写锁
      writeLock.unlock();
    }
  }

  /**
   * 初始化默认内部预留队列，保证SLS兼容性
   * @return 初始化完成的默认预留队列
   * @throws IOException 初始化异常
   */
  public ReservationQueue initializeDefaultInternalQueue() throws IOException {
    // 构造默认预留队列ID，命名规则为计划队列名+默认后缀
    String defReservationId =
        getQueueName() + ReservationConstants.DEFAULT_QUEUE_SUFFIX;

    // 创建默认预留队列实例
    ReservationQueue resQueue = new ReservationQueue(queueContext,
        defReservationId, this);
    try {
      // 初始化预留队列权限
      resQueue.initializeEntitlements();
    } catch (SchedulerDynamicEditException e) {
      throw new IllegalStateException(e);
    }
    // 将默认预留队列添加为当前计划队列的子队列
    childQueues.add(resQueue);

    return resQueue;
  }

  /**
   * 更新计划队列的配额配置
   * @param newUserLimit 新用户限制值
   * @param newUserLimitFactor 新用户限制因子
   * @param newMaxAppsForReservation 新预留总最大应用数
   * @param newMaxAppsPerUserForReservation 新单用户预留最大应用数
   */
  private void updateQuotas(float newUserLimit, float newUserLimitFactor,
      int newMaxAppsForReservation, int newMaxAppsPerUserForReservation) {
    this.userLimit = newUserLimit;
    this.userLimitFactor = newUserLimitFactor;
    this.maxAppsForReservation = newMaxAppsForReservation;
    this.maxAppsPerUserForReservation = newMaxAppsPerUserForReservation;
  }

  /**
   * Number of maximum applications for each of the reservations in this Plan.
   *
   * @return maxAppsForreservation
   */
  public int getMaxApplicationsForReservations() {
    return maxAppsForReservation;
  }

  /**
   * Number of maximum applications per user for each of the reservations in
   * this Plan.
   *
   * @return maxAppsPerUserForreservation
   */
  public int getMaxApplicationsPerUserForReservation() {
    return maxAppsPerUserForReservation;
  }

  /**
   * User limit value for each of the reservations in this Plan.
   *
   * @return userLimit
   */
  public float getUserLimitForReservation() {
    return userLimit;
  }

  /**
   * User limit factor value for each of the reservations in this Plan.
   *
   * @return userLimitFactor
   */
  public float getUserLimitFactor() {
    return userLimitFactor;
  }

  /**
   * Determine whether to hide/show the ReservationQueues.
   * @return true, show ReservationQueues; false, hide ReservationQueues.
   */
  public boolean showReservationsAsQueues() {
    return showReservationsAsQueues;
  }
}