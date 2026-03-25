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

/**
 * 调度器事件：添加应用尝试事件
 * 通知YARN调度器有新的应用尝试(Application Attempt)被添加，需要调度器进行处理
 */
public class AppAttemptAddedSchedulerEvent extends SchedulerEvent {

  // 目标应用尝试ID
  private final ApplicationAttemptId applicationAttemptId;
  // 是否需要从之前的应用尝试转移状态
  private final boolean transferStateFromPreviousAttempt;
  // 当前是否处于应用尝试恢复阶段
  private final boolean isAttemptRecovering;

  /**
   * 构造添加应用尝试调度事件，默认不是恢复场景
   * @param applicationAttemptId 应用尝试ID
   * @param transferStateFromPreviousAttempt 是否从之前尝试转移状态
   */
  public AppAttemptAddedSchedulerEvent(
      ApplicationAttemptId applicationAttemptId,
      boolean transferStateFromPreviousAttempt) {
    this(applicationAttemptId, transferStateFromPreviousAttempt, false);
  }

  /**
   * 构造添加应用尝试调度事件，支持恢复场景
   * @param applicationAttemptId 应用尝试ID
   * @param transferStateFromPreviousAttempt 是否从之前尝试转移状态
   * @param isAttemptRecovering 是否是恢复过程中的应用尝试
   */
  public AppAttemptAddedSchedulerEvent(
      ApplicationAttemptId applicationAttemptId,
      boolean transferStateFromPreviousAttempt,
      boolean isAttemptRecovering) {
    super(SchedulerEventType.APP_ATTEMPT_ADDED);
    this.applicationAttemptId = applicationAttemptId;
    this.transferStateFromPreviousAttempt = transferStateFromPreviousAttempt;
    this.isAttemptRecovering = isAttemptRecovering;
  }

  /**
   * 获取目标应用尝试ID
   * @return 应用尝试ID
   */
  public ApplicationAttemptId getApplicationAttemptId() {
    return applicationAttemptId;
  }

  /**
   * 获取是否需要从之前应用尝试转移状态
   * @return 是否转移状态标识
   */
  public boolean getTransferStateFromPreviousAttempt() {
    return transferStateFromPreviousAttempt;
  }

  /**
   * 获取是否是恢复过程中的应用尝试
   * @return 是否恢复标识
   */
  public boolean getIsAttemptRecovering() {
    return isAttemptRecovering;
  }
}