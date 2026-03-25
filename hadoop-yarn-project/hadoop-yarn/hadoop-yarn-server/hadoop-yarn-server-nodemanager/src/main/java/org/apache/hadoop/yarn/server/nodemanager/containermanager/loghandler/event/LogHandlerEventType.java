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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.event;

/**
 * 日志处理器事件类型枚举
 * 定义了NodeManager日志处理模块支持的所有事件类型
 */
public enum LogHandlerEventType {
  /** 应用启动事件 */
  APPLICATION_STARTED,
  /** 容器完成事件 */
  CONTAINER_FINISHED,
  /** 应用完成事件 */
  APPLICATION_FINISHED,
  /** 日志聚合令牌更新事件 */
  LOG_AGG_TOKEN_UPDATE
}