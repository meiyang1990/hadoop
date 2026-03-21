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
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.event;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AbstractParentQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity
    .QueueManagementChange;

import java.util.List;

/**
 * 队列管理变更事件，用于通知调度器队列配置已发生变更
 */
public class QueueManagementChangeEvent extends SchedulerEvent {

  // 发生变更的父队列
  private AbstractParentQueue parentQueue;
  // 队列变更操作列表
  private List<QueueManagementChange> queueManagementChanges;

  /**
   * 构造队列管理变更事件
   * @param parentQueue 发生变更的父队列
   * @param queueManagementChanges 队列变更操作列表
   */
  public QueueManagementChangeEvent(AbstractParentQueue parentQueue,
      List<QueueManagementChange> queueManagementChanges) {
    super(SchedulerEventType.MANAGE_QUEUE);
    this.parentQueue = parentQueue;
    this.queueManagementChanges = queueManagementChanges;
  }

  /**
   * 获取发生变更的父队列
   * @return 变更所属父队列
   */
  public AbstractParentQueue getParentQueue() {
    return parentQueue;
  }

  /**
   * 获取所有队列变更操作列表
   * @return 队列变更操作集合
   */
  public List<QueueManagementChange> getQueueManagementChanges() {
    return queueManagementChanges;
  }
}