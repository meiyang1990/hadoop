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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.scheduler;

/**
 * 定义NodeManager容器调度器支持的事件类型枚举，用于标识不同的容器调度事件。
 * 与{@link ContainerSchedulerEvent}关联，描述具体事件类型。
 */
public enum ContainerSchedulerEventType {
  /** 调度容器，请求分配资源启动容器 */
  SCHEDULE_CONTAINER,
  /** 容器执行完成，通知调度器释放资源 */
  CONTAINER_COMPLETED,
  /** 更新容器资源/状态信息 */
  UPDATE_CONTAINER,
  /** 生产源：节点心跳响应，RM请求清理队列中排队的容器 */
  SHED_QUEUED_CONTAINERS,
  /** 容器已暂停，通知调度器更新资源状态 */
  CONTAINER_PAUSED,
  /** 容器调度状态恢复完成，NM恢复流程结束 */
  RECOVERY_COMPLETED
}