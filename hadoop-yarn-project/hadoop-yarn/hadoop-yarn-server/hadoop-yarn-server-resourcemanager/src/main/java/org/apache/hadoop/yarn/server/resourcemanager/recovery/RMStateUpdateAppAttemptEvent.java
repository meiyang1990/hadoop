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

import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.ApplicationAttemptStateData;

/**
 * RM状态存储更新应用尝试状态事件，用于YARN ResourceManager恢复场景。
 * 承载需要持久化更新的应用尝试状态信息，触发RM状态存储的更新操作。
 */
public class RMStateUpdateAppAttemptEvent extends RMStateStoreEvent {

  // 需要更新的应用尝试状态数据
  ApplicationAttemptStateData attemptState;

  /**
   * 构造应用尝试状态更新事件。
   * @param attemptState 需要更新的应用尝试状态数据
   */
  public RMStateUpdateAppAttemptEvent(
      ApplicationAttemptStateData  attemptState) {
    super(RMStateStoreEventType.UPDATE_APP_ATTEMPT);
    this.attemptState = attemptState;
  }

  /**
   * 获取需要更新的应用尝试状态数据。
   * @return 应用尝试状态数据
   */
  public ApplicationAttemptStateData getAppAttemptState() {
    return attemptState;
  }
}