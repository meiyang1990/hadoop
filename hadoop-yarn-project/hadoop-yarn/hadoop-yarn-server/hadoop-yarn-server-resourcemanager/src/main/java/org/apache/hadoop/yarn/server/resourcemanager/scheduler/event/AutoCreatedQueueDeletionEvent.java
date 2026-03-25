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

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;

/**
 * 自动创建队列删除事件，用于触发自动创建空队列的清理流程。
 * 当动态自动创建的队列长时间没有应用运行时，调度器会收到该事件，
 * 检查并删除空闲队列以释放资源。
 */
public class AutoCreatedQueueDeletionEvent extends SchedulerEvent{
  // 待检查删除条件的队列
  private CSQueue checkQueue;

  /**
   * 构造自动创建队列删除事件
   * @param checkQueue 待检查是否满足删除条件的队列
   */
  public AutoCreatedQueueDeletionEvent(CSQueue checkQueue) {
    super(SchedulerEventType.AUTO_QUEUE_DELETION);
    this.checkQueue = checkQueue;
  }

  /**
   * 获取待检查删除条件的队列
   * @return 待检查的容量调度队列
   */
  public CSQueue getCheckQueue() {
    return checkQueue;
  }
}