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

package org.apache.hadoop.yarn.server.nodemanager;

/**
 * NodeManager容器管理器事件类型枚举，定义了容器管理器处理的各类事件类型。
 */
public enum ContainerManagerEventType {
  /** 终止所有应用 */
  FINISH_APPS,
  /** 终止指定容器 */
  FINISH_CONTAINERS,
  /** 更新容器资源/状态 */
  UPDATE_CONTAINERS,
  /** 向容器发送信号 */
  SIGNAL_CONTAINERS
}