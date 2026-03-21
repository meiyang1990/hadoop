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
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.RMState;

/**
 * 应用恢复事件，用于ResourceManager重启后恢复已存在的应用程序状态
 * 携带应用恢复所需的持久化状态信息
 */
public class RMAppRecoverEvent extends RMAppEvent {

  // 从状态存储中恢复的应用程序持久化状态
  private final RMState state;

  /**
   * 构造应用恢复事件
   * @param appId 目标应用ID
   * @param state 恢复的应用持久化状态
   */
  public RMAppRecoverEvent(ApplicationId appId, RMState state) {
    super(appId, RMAppEventType.RECOVER);
    this.state = state;
  }

  /**
   * 获取恢复的应用持久化状态
   * @return 应用持久化状态
   */
  public RMState getRMState() {
    return state;
  }
}