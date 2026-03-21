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

package org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt;

/**
 * YARN ResourceManager 应用尝试（RMAppAttempt）的状态枚举类
 * 定义了一个应用尝试从创建到结束的所有可能生命周期状态
 */
public enum RMAppAttemptState {
  /** 新建状态，应用尝试刚被创建 */
  NEW,
  /** 已提交状态，已经提交到调度器 */
  SUBMITTED,
  /** 已调度状态，已经被调度器安排执行 */
  SCHEDULED,
  /** 已分配状态，资源已经分配完成 */
  ALLOCATED,
  /** 已启动状态，应用尝试已经在NM节点启动 */
  LAUNCHED,
  /** 失败状态，应用尝试执行失败 */
  FAILED,
  /** 运行中状态，应用尝试正在正常执行 */
  RUNNING,
  /** 完成中状态，任务已经执行完成正在收尾 */
  FINISHING,
  /** 已完成状态，应用尝试正常执行结束 */
  FINISHED,
  /** 已杀死状态，应用尝试被手动杀死终止 */
  KILLED,
  /** 资源分配保存中状态，正在保存分配状态到状态存储 */
  ALLOCATED_SAVING,
  /** 非托管应用启动保存中状态，正在保存启动状态到状态存储 */
  LAUNCHED_UNMANAGED_SAVING,
  /** 最终状态保存中状态，正在保存最终状态到状态存储 */
  FINAL_SAVING
}