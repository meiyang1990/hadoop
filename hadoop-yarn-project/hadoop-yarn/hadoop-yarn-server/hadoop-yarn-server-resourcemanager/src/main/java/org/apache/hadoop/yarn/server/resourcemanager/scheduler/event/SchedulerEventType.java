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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.event;

/**
 * YARN ResourceManager 调度器事件类型枚举，定义了调度系统中所有可能发生的事件类型
 * 不同来源的事件会触发调度器不同的处理逻辑，实现调度流程的事件驱动
 */
public enum SchedulerEventType {

  // 来源: Node 节点相关事件
  /** 新增节点 */
  NODE_ADDED,
  /** 移除节点 */
  NODE_REMOVED,
  /** 更新节点信息 */
  NODE_UPDATE,
  /** 更新节点资源总量 */
  NODE_RESOURCE_UPDATE,
  /** 更新节点标签 */
  NODE_LABELS_UPDATE,
  /** 更新节点属性 */
  NODE_ATTRIBUTES_UPDATE,

  // 来源: RMApp 应用相关事件
  /** 新增应用 */
  APP_ADDED,
  /** 移除应用 */
  APP_REMOVED,

  // 来源: RMAppAttempt 应用尝试相关事件
  /** 新增应用尝试 */
  APP_ATTEMPT_ADDED,
  /** 移除应用尝试 */
  APP_ATTEMPT_REMOVED,

  // 来源: ContainerAllocationExpirer 容器分配过期事件
  /** 容器分配过期，需要回收 */
  CONTAINER_EXPIRED,

  // 来源: SchedulerAppAttempt::pullNewlyUpdatedContainer
  /** 释放容器资源 */
  RELEASE_CONTAINER,

  /* 来源: SchedulingEditPolicy 调度策略编辑事件 */
  /** 杀死已预留的容器 */
  KILL_RESERVED_CONTAINER,

  // 标记容器为抢占候选
  /** 将容器标记为待抢占 */
  MARK_CONTAINER_FOR_PREEMPTION,

  // 将待抢占容器标记为可杀死
  /** 将待抢占容器标记为可杀死 */
  MARK_CONTAINER_FOR_KILLABLE,

  // 取消容器可杀死标记
  /** 取消容器可杀死标记，恢复为不可杀死 */
  MARK_CONTAINER_FOR_NONKILLABLE,

  // 队列管理变更事件
  /** 队列管理操作（新增/修改/删除队列） */
  MANAGE_QUEUE,

  // 自动创建队列的自动删除检查事件
  /** 触发自动创建队列的删除检查 */
  AUTO_QUEUE_DELETION
}