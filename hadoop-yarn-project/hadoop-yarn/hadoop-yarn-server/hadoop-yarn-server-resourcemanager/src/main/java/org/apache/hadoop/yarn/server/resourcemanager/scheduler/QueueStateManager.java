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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationSchedulerConfiguration;

/**
 * 队列状态管理器，供资源调度器统一管理队列运行状态，支持队列的启停和删除检查
 * 
 * QueueStateManager which can be used by Scheduler to manage the queue state.
 *
 */
// TODO: The class will be used by YARN-5734-OrgQueue for
// easy CapacityScheduler queue configuration management.
@SuppressWarnings("rawtypes")
@Private
@Unstable
public class QueueStateManager<T extends SchedulerQueue,
    E extends ReservationSchedulerConfiguration> {

  private static final Logger LOG =
      LoggerFactory.getLogger(QueueStateManager.class);

  // 队列管理器引用，用于获取队列实例
  private SchedulerQueueManager<T, E> queueManager;

  /**
   * 初始化队列状态管理器，绑定队列管理器实例
   * @param newQueueManager 队列管理器实例
   */
  public synchronized void initialize(SchedulerQueueManager<T, E>
      newQueueManager) {
    this.queueManager = newQueueManager;
  }

  /**
   * 停止指定队列，将队列状态设置为停止
   * @param queueName 目标队列名称
   * @throws YarnException 如果指定队列不存在则抛出异常
   */
  @SuppressWarnings("unchecked")
  public synchronized void stopQueue(String queueName) throws YarnException {
    // 从队列管理器获取目标队列
    SchedulerQueue<T> queue = queueManager.getQueue(queueName);
    if (queue == null) {
      throw new YarnException("The specified queue:" + queueName
          + " does not exist!");
    }
    // 调用队列自身的停止方法更新状态
    queue.stopQueue();
  }

  /**
   * 激活指定队列，将队列状态恢复为运行中
   * @param queueName 目标队列名称
   * @throws YarnException 如果指定队列不存在或无法激活则抛出异常
   */
  @SuppressWarnings("unchecked")
  public synchronized void activateQueue(String queueName)
      throws YarnException {
    // 从队列管理器获取目标队列
    SchedulerQueue<T> queue = queueManager.getQueue(queueName);
    if (queue == null) {
      throw new YarnException("The specified queue:" + queueName
          + " does not exist!");
    }
    // 调用队列自身的激活方法更新状态
    queue.activateQueue();
  }

  /**
   * 检查指定队列是否满足删除条件
   * @param queueName 目标队列名称
   * @return true 如果队列可以删除，否则返回false
   */
  @SuppressWarnings("unchecked")
  public boolean canDelete(String queueName) {
    // 从队列管理器获取目标队列
    SchedulerQueue<T> queue = queueManager.getQueue(queueName);
    if (queue == null) {
      LOG.info("The specified queue:" + queueName + " does not exist!");
      return false;
    }
    // 只有已停止的队列才能删除
    if (queue.getState() == QueueState.STOPPED){
      return true;
    }
    LOG.info("Need to stop the specific queue:" + queueName + " first.");
    return false;
  }
}