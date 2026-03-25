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

package org.apache.hadoop.yarn.server.resourcemanager.rmnode;

/**
 * YARN ResourceManager 节点管理事件类型枚举
 * 定义了RMNode状态机处理的所有事件类型，标识节点和容器相关的各类操作触发的事件
 */
public enum RMNodeEventType {
  
  /** 节点启动完成事件 */
  STARTED,
  
  // 事件来源：AdminService 管理服务
  /** 立即退役节点事件 */
  DECOMMISSION,
  /** 优雅退役节点事件（等待容器完成后退役） */
  GRACEFUL_DECOMMISSION,
  /** 重新启用已退役节点事件 */
  RECOMMISSION,
  
  // 事件来源：AdminService, ResourceTrackerService
  /** 更新节点资源容量事件 */
  RESOURCE_UPDATE,

  // 事件来源：ResourceTrackerService 节点资源跟踪服务
  /** 节点状态更新事件 */
  STATUS_UPDATE,
  /** 节点正在重启事件 */
  REBOOTING,
  /** 节点重新连接事件 */
  RECONNECTED,
  /** 节点关闭事件 */
  SHUTDOWN,

  // 事件来源：Application 应用
  /** 清理应用在该节点上的所有容器事件 */
  CLEANUP_APP,

  // 事件来源：Container 容器
  /** 容器已在节点分配事件 */
  CONTAINER_ALLOCATED,
  /** 清理指定容器事件 */
  CLEANUP_CONTAINER,
  /** 更新容器资源事件 */
  UPDATE_CONTAINER,

  // 事件来源：ClientRMService 客户端服务
  /** 向容器发送信号事件 */
  SIGNAL_CONTAINER,

  // 事件来源：RMAppAttempt 应用尝试
  /** ApplicationMaster拉取完成容器后清理事件 */
  FINISHED_CONTAINERS_PULLED_BY_AM,

  // 事件来源：NMLivelinessMonitor 节点活跃度监控
  /** 节点心跳超时过期事件 */
  EXPIRE
}