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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.event;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;

/**
 * YARN资源调度器事件：应用尝试移除事件，通知调度器移除已结束的应用尝试
 */
public class AppAttemptRemovedSchedulerEvent extends SchedulerEvent {

  private final ApplicationAttemptId applicationAttemptId;
  private final RMAppAttemptState finalAttemptState;
  private final boolean keepContainersAcrossAppAttempts;

  /**
   * 构造应用尝试移除调度事件
   * @param applicationAttemptId 被移除的应用尝试ID
   * @param finalAttemptState 应用尝试的最终状态
   * @param keepContainers 是否在应用尝试移除后保留容器
   */
  public AppAttemptRemovedSchedulerEvent(
      ApplicationAttemptId applicationAttemptId,
      RMAppAttemptState finalAttemptState, boolean keepContainers) {
    super(SchedulerEventType.APP_ATTEMPT_REMOVED);
    this.applicationAttemptId = applicationAttemptId;
    this.finalAttemptState = finalAttemptState;
    this.keepContainersAcrossAppAttempts = keepContainers;
  }

  /**
   * 获取被移除的应用尝试ID
   * @return 应用尝试ID
   */
  public ApplicationAttemptId getApplicationAttemptID() {
    return this.applicationAttemptId;
  }

  /**
   * 获取应用尝试的最终状态
   * @return 最终状态枚举值
   */
  public RMAppAttemptState getFinalAttemptState() {
    return this.finalAttemptState;
  }

  /**
   * 获取是否保留容器的标识，跨应用尝试复用场景使用
   * @return true表示保留容器，false表示清理所有容器
   */
  public boolean getKeepContainersAcrossAppAttempts() {
    return this.keepContainersAcrossAppAttempts;
  }
}