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

import org.apache.hadoop.security.token.delegation.DelegationKey;

/**
 * RM状态存储中RMDT（RM Delegation Token，RM委派令牌）主密钥相关事件，
 * 用于持久化存储RM委派令牌主密钥变更，支持ResourceManager故障恢复。
 */
public class RMStateStoreRMDTMasterKeyEvent extends RMStateStoreEvent {
  // 需要持久化的委派令牌主密钥
  private DelegationKey delegationKey;

  /**
   * 构造RM委派令牌主密钥存储事件。
   * @param type 事件类型
   */
  public RMStateStoreRMDTMasterKeyEvent(RMStateStoreEventType type) {
    super(type);
  }

  /**
   * 构造带委派密钥信息的RM委派令牌主密钥存储事件。
   * @param delegationKey 需要存储的委派令牌主密钥
   * @param type 事件类型
   */
  public RMStateStoreRMDTMasterKeyEvent(DelegationKey delegationKey,
      RMStateStoreEventType type) {
    this(type);
    this.delegationKey = delegationKey;
  }

  /**
   * 获取需要存储的委派令牌主密钥。
   * @return 委派令牌主密钥
   */
  public DelegationKey getDelegationKey() {
    return delegationKey;
  }
}