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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.application;

/**
 * 节点管理器应用管理模块事件类型枚举
 * 定义了应用状态机处理的所有事件类型，按事件来源分类
 */
public enum ApplicationEventType {

  // Source: ContainerManager
  /** 初始化应用事件，来源：容器管理器 */
  INIT_APPLICATION,
  /** 初始化容器事件，来源：容器管理器 */
  INIT_CONTAINER,
  /** 结束应用事件，来源：初始化失败时由日志聚合服务触发 */
  FINISH_APPLICATION,

  // Source: ResourceLocalizationService
  /** 应用初始化完成事件，来源：资源本地化服务 */
  APPLICATION_INITED,
  /** 应用资源清理完成事件，来源：资源本地化服务 */
  APPLICATION_RESOURCES_CLEANEDUP,

  // Source: Container
  /** 应用内容器执行完成事件，来源：容器 */
  APPLICATION_CONTAINER_FINISHED,

  // Source: Log Handler
  /** 应用日志处理初始化完成事件，来源：日志处理器 */
  APPLICATION_LOG_HANDLING_INITED,
  /** 应用日志处理完成事件，来源：日志处理器 */
  APPLICATION_LOG_HANDLING_FINISHED,
  /** 应用日志处理失败事件，来源：日志处理器 */
  APPLICATION_LOG_HANDLING_FAILED
}