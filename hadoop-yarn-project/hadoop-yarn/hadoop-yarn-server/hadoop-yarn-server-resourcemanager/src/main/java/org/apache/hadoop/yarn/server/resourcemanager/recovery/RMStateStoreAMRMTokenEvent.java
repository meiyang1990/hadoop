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

import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.AMRMTokenSecretManagerState;

/**
 * RM状态存储中AMRM令牌密钥管理器状态变更事件，用于持久化恢复场景下保存令牌状态。
 */
public class RMStateStoreAMRMTokenEvent extends RMStateStoreEvent {
  // AMRM令牌密钥管理器当前状态
  private AMRMTokenSecretManagerState amrmTokenSecretManagerState;
  // 是否为更新操作标记
  private boolean isUpdate;

  /**
   * 构造RMStateStoreAMRMTokenEvent实例。
   * @param type 事件类型
   */
  public RMStateStoreAMRMTokenEvent(RMStateStoreEventType type) {
    super(type);
  }

  /**
   * 构造包含令牌状态信息的完整事件实例。
   * @param amrmTokenSecretManagerState AMRM令牌密钥管理器状态
   * @param isUpdate 是否为更新操作
   * @param type 事件类型
   */
  public RMStateStoreAMRMTokenEvent(
      AMRMTokenSecretManagerState amrmTokenSecretManagerState,
      boolean isUpdate, RMStateStoreEventType type) {
    this(type);
    this.amrmTokenSecretManagerState = amrmTokenSecretManagerState;
    this.isUpdate = isUpdate;
  }

  /**
   * 获取AMRM令牌密钥管理器状态信息。
   * @return AMRM令牌密钥管理器状态
   */
  public AMRMTokenSecretManagerState getAmrmTokenSecretManagerState() {
    return amrmTokenSecretManagerState;
  }

  /**
   * 获取操作类型标记，判断是否为更新操作。
   * @return true表示更新操作，false表示新增操作
   */
  public boolean isUpdate() {
    return isUpdate;
  }
}