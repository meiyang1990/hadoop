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
package org.apache.hadoop.yarn.server.federation.store.records;

import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * 联邦Router环境下ResourceManager委派令牌秘钥管理器状态存储类
 * 用于持久化存储联邦Router中委派令牌和主秘钥的状态信息
 */
public class RouterRMDTSecretManagerState {

  // 存储委派令牌标识到令牌信息的映射，key为令牌标识，value为Router存储的令牌信息
  private Map<RMDelegationTokenIdentifier, RouterStoreToken> delegationTokenState = new HashMap<>();

  // 存储所有有效的主秘钥集合
  private Set<DelegationKey> masterKeyState = new HashSet<>();

  // 委派令牌序列号，用于生成唯一令牌标识
  private int dtSequenceNumber = 0;

  /**
   * 获取所有委派令牌的状态映射
   * @return 委派令牌标识到令牌信息的映射
   */
  public Map<RMDelegationTokenIdentifier, RouterStoreToken> getTokenState() {
    return delegationTokenState;
  }

  /**
   * 获取所有主秘钥的状态集合
   * @return 有效的主秘钥集合
   */
  public Set<DelegationKey> getMasterKeyState() {
    return masterKeyState;
  }

  /**
   * 获取当前委派令牌序列号
   * @return 当前序列号值
   */
  public int getDTSequenceNumber() {
    return dtSequenceNumber;
  }

  /**
   * 设置委派令牌序列号
   * @param dtSequenceNumber 新的序列号值
   */
  public void setDtSequenceNumber(int dtSequenceNumber) {
    this.dtSequenceNumber = dtSequenceNumber;
  }
}