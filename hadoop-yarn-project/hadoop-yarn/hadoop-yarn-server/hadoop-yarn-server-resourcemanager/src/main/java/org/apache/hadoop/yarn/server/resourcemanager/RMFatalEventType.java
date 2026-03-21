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

package org.apache.hadoop.yarn.server.resourcemanager;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * ResourceManager致命事件类型枚举
 * 定义了会导致RM终止服务的各类严重错误事件类型
 */
@InterfaceAudience.Private
public enum RMFatalEventType {
  // Source <- Store
  /** 状态存储被隔离（共享存储 fencing 触发，当前RM失去所有权） */
  STATE_STORE_FENCED,
  /** 状态存储操作失败 */
  STATE_STORE_OP_FAILED,

  // Source <- Embedded Elector
  /** 嵌入式选举器服务失败 */
  EMBEDDED_ELECTOR_FAILED,

  // Source <- Admin Service
  /** 切换到Active状态失败 */
  TRANSITION_TO_ACTIVE_FAILED,

  // Source <- Critical Thread Crash
  /** 关键工作线程崩溃 */
  CRITICAL_THREAD_CRASH
}