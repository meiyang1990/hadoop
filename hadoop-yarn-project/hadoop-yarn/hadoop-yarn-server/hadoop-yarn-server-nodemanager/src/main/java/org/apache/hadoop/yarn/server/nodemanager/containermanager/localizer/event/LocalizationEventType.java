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
package org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event;

/**
 * 容器资源本地化事件类型枚举
 * 定义了NM节点本地化过程中所有可能的事件类型，用于状态机流转
 */
public enum LocalizationEventType {
  /** 初始化应用程序资源本地化 */
  INIT_APPLICATION_RESOURCES,
  /** 本地化容器所需资源 */
  LOCALIZE_CONTAINER_RESOURCES,
  /** 缓存清理请求 */
  CACHE_CLEANUP,
  /** 清理容器资源 */
  CLEANUP_CONTAINER_RESOURCES,
  /** 销毁应用程序所有资源 */
  DESTROY_APPLICATION_RESOURCES,
  /** 容器资源本地化完成通知 */
  CONTAINER_RESOURCES_LOCALIZED,
}