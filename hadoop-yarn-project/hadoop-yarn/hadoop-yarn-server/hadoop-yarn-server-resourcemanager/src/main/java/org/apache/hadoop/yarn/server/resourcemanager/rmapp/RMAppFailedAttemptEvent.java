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

package org.apache.hadoop.yarn.server.resourcemanager.rmapp;

import org.apache.hadoop.yarn.api.records.ApplicationId;

/**
 * YARN ResourceManager 应用尝试失败事件，承载应用尝试失败相关上下文信息。
 * 用于 RM 状态机处理应用运行失败场景，支持是否从上次尝试转移状态的配置。
 */
public class RMAppFailedAttemptEvent extends RMAppEvent {

  // 标记是否需要从之前的应用尝试转移状态信息
  private final boolean transferStateFromPreviousAttempt;

  /**
   * 构造应用尝试失败事件。
   * @param appId 应用ID
   * @param event 事件类型
   * @param diagnostics 失败诊断信息
   * @param transferStateFromPreviousAttempt 是否从之前尝试转移状态
   */
  public RMAppFailedAttemptEvent(ApplicationId appId, RMAppEventType event, 
      String diagnostics, boolean transferStateFromPreviousAttempt) {
    super(appId, event, diagnostics);
    this.transferStateFromPreviousAttempt = transferStateFromPreviousAttempt;
  }

  /**
   * 获取是否需要从之前的应用尝试转移状态的标记。
   * @return 是否转移状态
   */
  public boolean getTransferStateFromPreviousAttempt() {
    return transferStateFromPreviousAttempt;
  }
}