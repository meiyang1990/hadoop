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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.allocator;

/**
 * 容器分配结果状态枚举，定义容量调度器资源分配过程中的各种结果状态
 */
public enum AllocationState {
  /** 当前应用未分配到容器，跳过该应用 */
  APP_SKIPPED,
  /** 当前优先级无可用资源，跳过该优先级 */
  PRIORITY_SKIPPED,
  /** 未满足位置性要求，跳过本次分配 */
  LOCALITY_SKIPPED,
  /** 队列资源不足，跳过该队列 */
  QUEUE_SKIPPED,
  /** 成功分配容器 */
  ALLOCATED,
  /** 容器已预留，等待后续分配 */
  RESERVED
}