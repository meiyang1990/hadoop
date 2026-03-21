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
 * 资源分配最终状态枚举集合，用于记录YARN资源分配活动的结果状态。
 */
public enum AllocationState {
  /** 默认状态，分配未完成处理 */
  DEFAULT,
  /** 
   * 队列或应用主动放弃使用资源，或者未分配到任何资源
   */
  SKIPPED,
  /** 成功分配一个新的非预留容器 */
  ALLOCATED,
  /** 成功从已有预留容器中分配出新容器 */
  ALLOCATED_FROM_RESERVED,
  /** 成功预留出新容器 */
  RESERVED
}