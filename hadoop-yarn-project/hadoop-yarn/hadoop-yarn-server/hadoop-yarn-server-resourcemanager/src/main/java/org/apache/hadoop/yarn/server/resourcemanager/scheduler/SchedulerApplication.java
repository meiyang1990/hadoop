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

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;

/**
 * YARN资源调度器层面的应用抽象，封装应用的核心调度信息，持有应用的当前尝试实例
 * @param <T> 具体的应用尝试类型，必须继承SchedulerApplicationAttempt
 */
@Private
@Unstable
public class SchedulerApplication<T extends SchedulerApplicationAttempt> {

  private Queue queue;
  private final String user;
  private volatile T currentAttempt;
  private volatile Priority priority;
  private boolean unmanagedAM;

  /**
   * 构造调度应用实例，不指定初始优先级
   * @param queue 应用所属调度队列
   * @param user 提交应用的用户
   * @param unmanagedAM 是否为非托管ApplicationMaster
   */
  public SchedulerApplication(Queue queue, String user, boolean unmanagedAM) {
    this.queue = queue;
    this.user = user;
    this.unmanagedAM = unmanagedAM;
    this.priority = null;
  }

  /**
   * 构造调度应用实例，指定初始优先级
   * @param queue 应用所属调度队列
   * @param user 提交应用的用户
   * @param priority 应用优先级
   * @param unmanagedAM 是否为非托管ApplicationMaster
   */
  public SchedulerApplication(Queue queue, String user, Priority priority,
      boolean unmanagedAM) {
    this.queue = queue;
    this.user = user;
    this.unmanagedAM = unmanagedAM;
    this.priority = priority;
  }

  public Queue getQueue() {
    return queue;
  }
  
  public void setQueue(Queue queue) {
    this.queue = queue;
  }

  public String getUser() {
    return user;
  }

  public T getCurrentAppAttempt() {
    return currentAttempt;
  }

  public void setCurrentAppAttempt(T currentAttempt) {
    this.currentAttempt = currentAttempt;
  }

  /**
   * 停止应用，更新调度队列指标统计
   * @param rmAppFinalState 应用最终状态
   */
  public void stop(RMAppState rmAppFinalState) {
    queue.getMetrics().finishApp(user, rmAppFinalState, isUnmanagedAM());
  }

  public Priority getPriority() {
    return priority;
  }

  /**
   * 更新应用优先级，并同步更新当前运行的应用尝试的优先级
   * @param priority 新的应用优先级
   */
  public void setPriority(Priority priority) {
    this.priority = priority;

    // 如果当前有运行的应用尝试，同步更新其优先级
    if (null != currentAttempt) {
      currentAttempt.setPriority(priority);
    }
  }

  public boolean isUnmanagedAM() {
    return unmanagedAM;
  }
}