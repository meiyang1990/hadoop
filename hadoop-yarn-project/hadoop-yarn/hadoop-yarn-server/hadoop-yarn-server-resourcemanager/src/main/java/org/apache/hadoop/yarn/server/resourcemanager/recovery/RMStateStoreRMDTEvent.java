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

import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;

/**
 * RM状态存储中关于RM委托令牌的事件类，
 * 用于持久化存储委托令牌更新/删除等状态变更事件，支持ResourceManager故障恢复
 */
public class RMStateStoreRMDTEvent extends RMStateStoreEvent {
  // RM委托令牌标识符
  private RMDelegationTokenIdentifier rmDTIdentifier;
  // 令牌更新到期时间
  private Long renewDate;

  /**
   * 构造RM委托令牌状态存储事件
   * @param type 事件类型
   */
  public RMStateStoreRMDTEvent(RMStateStoreEventType type) {
    super(type);
  }

  /**
   * 构造包含完整委托令牌信息的状态存储事件
   * @param rmDTIdentifier RM委托令牌标识符
   * @param renewDate 令牌更新到期时间
   * @param type 事件类型
   */
  public RMStateStoreRMDTEvent(RMDelegationTokenIdentifier rmDTIdentifier,
      Long renewDate, RMStateStoreEventType type) {
    this(type);
    this.rmDTIdentifier = rmDTIdentifier;
    this.renewDate = renewDate;
  }

  /**
   * 获取RM委托令牌标识符
   * @return RM委托令牌标识符
   */
  public RMDelegationTokenIdentifier getRmDTIdentifier() {
    return rmDTIdentifier;
  }

  /**
   * 获取令牌更新到期时间
   * @return 令牌更新到期时间戳
   */
  public Long getRenewDate() {
    return renewDate;
  }
}