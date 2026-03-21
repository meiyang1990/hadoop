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

package org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;

/**
 * 应用尝试启动事件，承载启动新应用尝试时传递的事件信息。
 * 用于通知ResourceManager中应用尝试状态机触发启动操作。
 */
public class RMAppStartAttemptEvent extends RMAppAttemptEvent {

  // 标记是否需要从前一个应用尝试转移状态
  private final boolean transferStateFromPreviousAttempt;

  /**
   * 构造应用尝试启动事件。
   * @param appAttemptId 目标应用尝试ID
   * @param transferStateFromPreviousAttempt 是否需要转移前一个应用尝试的状态
   */
  public RMAppStartAttemptEvent(ApplicationAttemptId appAttemptId,
      boolean transferStateFromPreviousAttempt) {
    super(appAttemptId, RMAppAttemptEventType.START);
    this.transferStateFromPreviousAttempt = transferStateFromPreviousAttempt;
  }

  /**
   * 获取是否需要从前一个应用尝试转移状态的标记。
   * @return true表示需要转移，false表示不需要
   */
  public boolean getTransferStateFromPreviousAttempt() {
    return transferStateFromPreviousAttempt;
  }
}