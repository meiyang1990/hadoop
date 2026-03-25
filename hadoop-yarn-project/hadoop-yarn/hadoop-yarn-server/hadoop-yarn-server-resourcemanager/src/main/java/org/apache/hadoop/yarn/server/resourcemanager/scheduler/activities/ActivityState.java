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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities;

/**
 * 调度活动状态枚举，定义了YARN资源分配过程中各类调度活动的状态集合。
 * 用于记录和追踪节点资源分配过程中每一步操作的结果状态，支持调度流程审计与调试。
 */
public enum ActivityState {
  // 添加新活动到节点分配时的默认初始状态
  DEFAULT,
  // 容器已分配给子队列/应用或当前队列/应用，分配请求被接受
  ACCEPTED,
  // 队列或应用主动放弃资源使用，或未分配任何资源
  SKIPPED,
  // 容器无法分配给子队列或当前应用，分配请求被拒绝
  REJECTED,
  ALLOCATED, // 成功分配一个新的非预留容器
  RESERVED,  // 成功预留一个新容器
  RE_RESERVED  // 成功重新预留一个新容器
}