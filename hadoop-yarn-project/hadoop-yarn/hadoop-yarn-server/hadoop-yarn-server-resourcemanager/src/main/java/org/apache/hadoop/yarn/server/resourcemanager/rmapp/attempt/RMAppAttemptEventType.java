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

package org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt;

/**
 * RM应用尝试事件类型枚举，定义了YARN ResourceManager中应用尝试生命周期中
 * 所有可能发生的事件类型，用于RM应用尝试状态机驱动状态转换。
 */
public enum RMAppAttemptEventType {
  // 事件来源：RM应用（RMApp）
  /** 启动应用尝试 */
  START,
  /** 杀死应用尝试 */
  KILL,
  /** 应用尝试失败 */
  FAIL,

  // 事件来源：应用Master启动器（AMLauncher）
  /** 应用Master已启动完成 */
  LAUNCHED,
  /** 应用Master启动失败 */
  LAUNCH_FAILED,

  // 事件来源：应用Master存活监控（AMLivelinessMonitor）
  /** 应用Master心跳超时过期 */
  EXPIRE,
  
  // 事件来源：ApplicationMaster服务（ApplicationMasterService）
  /** 应用Master已注册 */
  REGISTERED,
  /** 应用Master状态更新 */
  STATUS_UPDATE,
  /** 应用Master已反注册 */
  UNREGISTERED,

  // 事件来源：容器（Containers）
  /** 容器已分配给应用尝试 */
  CONTAINER_ALLOCATED,
  /** 容器运行完成 */
  CONTAINER_FINISHED,
  
  // 事件来源：RM状态存储（RMStateStore）
  /** 应用尝试新建信息已保存到状态存储 */
  ATTEMPT_NEW_SAVED,
  /** 应用尝试更新信息已保存到状态存储 */
  ATTEMPT_UPDATE_SAVED,

  // 事件来源：调度器（Scheduler）
  /** 应用尝试已添加到调度器 */
  ATTEMPT_ADDED,
  
  // 事件来源：RMAttemptImpl恢复流程
  /** 恢复应用尝试状态 */
  RECOVER

}