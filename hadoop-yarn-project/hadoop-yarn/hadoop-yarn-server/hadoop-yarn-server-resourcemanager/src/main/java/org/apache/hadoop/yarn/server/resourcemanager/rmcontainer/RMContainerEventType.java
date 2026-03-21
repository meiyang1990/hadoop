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

package org.apache.hadoop.yarn.server.resourcemanager.rmcontainer;

/**
 * RMContainer状态机事件类型枚举定义，
 * 用于描述ResourceManager中容器生命周期的各类事件，驱动RMContainer状态机流转。
 */
public enum RMContainerEventType {

  // Source: SchedulerApp
  /** 启动容器事件 */
  START,
  /** 容器已被ApplicationMaster获取事件 */
  ACQUIRED,
  /** 杀死容器事件，节点下线时也会触发该事件 */
  KILL,
  /** 容器预留事件 */
  RESERVED,
  
  /** 容器资源增减后，被ApplicationMaster重新获取事件 */
  ACQUIRE_UPDATED_CONTAINER, 

  /** 容器已启动事件 */
  LAUNCHED,
  /** 容器已完成事件 */
  FINISHED,

  // Source: ApplicationMasterService->Scheduler
  /** 容器已释放事件 */
  RELEASED,

  // Source: ContainerAllocationExpirer  
  /** 容器分配超时过期事件 */
  EXPIRE,

  /** 容器恢复事件（恢复RM状态时触发） */
  RECOVER,
  
  // Source: Scheduler
  // Resource change approved by scheduler
  /** 调度器批准容器资源变更事件 */
  CHANGE_RESOURCE,
  
  // NM reported resource change is done
  /** NodeManager上报资源变更完成事件 */
  NM_DONE_CHANGE_RESOURCE 
}