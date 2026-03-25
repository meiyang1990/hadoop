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

/**
 * RM状态存储事件类型枚举，定义了ResourceManager状态恢复持久化过程中所有可能发生的事件类型。
 * 用于标识需要持久化到状态存储的不同操作类型，支撑RM故障恢复后状态重建。
 */
public enum RMStateStoreEventType {
  /** 存储应用尝试信息 */
  STORE_APP_ATTEMPT,
  /** 存储应用基本信息 */
  STORE_APP,
  /** 更新应用基本信息 */
  UPDATE_APP,
  /** 更新应用尝试信息 */
  UPDATE_APP_ATTEMPT,
  /** 删除应用信息 */
  REMOVE_APP,
  /** 删除应用尝试信息 */
  REMOVE_APP_ATTEMPT,
  /** RM状态隔离事件（标记老RM已被隔离） */
  FENCED,

  // Below events should be called synchronously
  /** 存储主密钥信息 */
  STORE_MASTERKEY,
  /** 删除主密钥信息 */
  REMOVE_MASTERKEY,
  /** 存储授权令牌 */
  STORE_DELEGATION_TOKEN,
  /** 删除授权令牌 */
  REMOVE_DELEGATION_TOKEN,
  /** 更新授权令牌 */
  UPDATE_DELEGATION_TOKEN,
  /** 更新AMRM令牌 */
  UPDATE_AMRM_TOKEN,
  /** 存储资源预约信息 */
  STORE_RESERVATION,
  /** 删除资源预约信息 */
  REMOVE_RESERVATION,
  /** 存储代理CA证书 */
  STORE_PROXY_CA_CERT,
}