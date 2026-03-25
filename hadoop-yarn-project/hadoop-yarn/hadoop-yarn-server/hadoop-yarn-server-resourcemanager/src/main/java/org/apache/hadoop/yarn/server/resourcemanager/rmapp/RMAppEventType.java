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

/**
 * RM应用事件类型枚举，定义了ResourceManager中应用生命周期所有可能的事件类型。
 * 不同事件来自不同的RM内部组件，驱动RM应用状态机进行状态转换。
 */
public enum RMAppEventType {
  // Source: ClientRMService
  /** 启动新应用，来自客户端RM服务 */
  START,
  /** 恢复已保存应用，来自客户端RM服务 */
  RECOVER,
  /** 杀死应用，来自客户端RM服务 */
  KILL,

  // Source: Scheduler and RMAppManager
  /** 应用被拒绝，来自调度器或RM应用管理器 */
  APP_REJECTED,

  // Source: Scheduler
  /** 应用被接受，来自调度器 */
  APP_ACCEPTED,

  // Source: RMAppAttempt
  /** 应用尝试已注册，来自应用尝试 */
  ATTEMPT_REGISTERED,
  /** 应用尝试已注销，来自应用尝试 */
  ATTEMPT_UNREGISTERED,
  /** 应用尝试已完成，来自应用尝试 */
  ATTEMPT_FINISHED, // Will send the final state
  /** 应用尝试失败，来自应用尝试 */
  ATTEMPT_FAILED,
  /** 应用尝试被杀死，来自应用尝试 */
  ATTEMPT_KILLED,
  /** 节点更新，来自应用尝试 */
  NODE_UPDATE,
  /** 应用尝试已启动，来自应用尝试 */
  ATTEMPT_LAUNCHED,
  
  // Source: Container and ResourceTracker
  /** 应用已在节点上运行，来自容器或资源追踪器 */
  APP_RUNNING_ON_NODE,

  // Source: RMStateStore
  /** 新应用状态已保存，来自RM状态存储 */
  APP_NEW_SAVED,
  /** 应用状态已更新保存，来自RM状态存储 */
  APP_UPDATE_SAVED,
  /** 应用状态保存失败，来自RM状态存储 */
  APP_SAVE_FAILED,
}