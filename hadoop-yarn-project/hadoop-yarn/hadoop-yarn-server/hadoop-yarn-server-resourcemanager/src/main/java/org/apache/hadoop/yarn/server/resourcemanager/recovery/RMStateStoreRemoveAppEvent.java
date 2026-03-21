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
 * RM状态存储删除应用事件，封装删除应用所需的状态信息。
 * 用于通知RM状态存储从恢复数据中移除已完成应用的状态信息。
 */
public class RMStateStoreRemoveAppEvent extends RMStateStoreEvent {
  // 待删除应用的状态数据
  ApplicationStateData appState;
  
  /**
   * 构造删除应用事件。
   * @param appState 待删除应用的状态数据
   */
  RMStateStoreRemoveAppEvent(ApplicationStateData appState) {
    super(RMStateStoreEventType.REMOVE_APP);
    this.appState = appState;
  }
  
  /**
   * 获取待删除应用的状态数据。
   * @return 应用状态数据
   */
  public ApplicationStateData getAppState() {
    return appState;
  }
}