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

package org.apache.hadoop.yarn.server.resourcemanager.recovery;

import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.ApplicationStateData;

/**
 * RM状态存储应用事件，用于存储应用状态信息到状态存储的事件
 * 是RM恢复流程中处理应用状态持久化的事件载体
 */
public class RMStateStoreAppEvent extends RMStateStoreEvent {

  // 待持久化的应用状态数据
  private final ApplicationStateData appState;

  /**
   * 构造存储应用状态的事件
   * @param appState 待存储的应用状态数据
   */
  public RMStateStoreAppEvent(ApplicationStateData appState) {
    super(RMStateStoreEventType.STORE_APP);
    this.appState = appState;
  }

  /**
   * 获取待存储的应用状态数据
   * @return 应用状态数据对象
   */
  public ApplicationStateData getAppState() {
    return appState;
  }
}