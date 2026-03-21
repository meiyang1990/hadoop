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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.container;

/**
 * 容器状态机事件类型枚举，定义了NodeManager容器管理过程中所有可能的事件类型
 */
public enum ContainerEventType {

  // 事件生产者: ContainerManager
  /** 初始化容器 */
  INIT_CONTAINER,
  /** 杀死容器 */
  KILL_CONTAINER,
  /** 更新诊断信息 */
  UPDATE_DIAGNOSTICS_MSG,
  /** 容器执行完成 */
  CONTAINER_DONE,
  /** 重新初始化容器 */
  REINITIALIZE_CONTAINER,
  /** 回滚容器重新初始化 */
  ROLLBACK_REINIT,
  /** 暂停容器 */
  PAUSE_CONTAINER,
  /** 恢复容器 */
  RESUME_CONTAINER,
  /** 更新容器令牌 */
  UPDATE_CONTAINER_TOKEN,

  // 事件生产者: DownloadManager
  /** 容器初始化完成 */
  CONTAINER_INITED,
  /** 资源本地化完成 */
  RESOURCE_LOCALIZED,
  /** 资源本地化失败 */
  RESOURCE_FAILED,
  /** 容器资源清理完成 */
  CONTAINER_RESOURCES_CLEANEDUP,

  // 事件生产者: ContainersLauncher
  /** 容器启动完成 */
  CONTAINER_LAUNCHED,
  /** 容器执行成功退出 */
  CONTAINER_EXITED_WITH_SUCCESS,
  /** 容器执行失败退出 */
  CONTAINER_EXITED_WITH_FAILURE,
  /** 容器根据请求被杀死 */
  CONTAINER_KILLED_ON_REQUEST,
  /** 容器已暂停 */
  CONTAINER_PAUSED,
  /** 容器已恢复 */
  CONTAINER_RESUMED,

  // 事件生产者: ContainerScheduler
  /** 容器令牌更新完成 */
  CONTAINER_TOKEN_UPDATED,

  /** 恢复已暂停容器 */
  RECOVER_PAUSED_CONTAINER
}