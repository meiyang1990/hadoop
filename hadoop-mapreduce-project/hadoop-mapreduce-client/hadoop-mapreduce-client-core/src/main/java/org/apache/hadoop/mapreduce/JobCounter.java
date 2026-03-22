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

package org.apache.hadoop.mapreduce;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * 作业级别计数器枚举，定义MapReduce作业运行过程中需要统计的各类作业级指标
 * 用于跟踪作业执行过程中任务执行状态、资源使用情况和数据本地性等核心指标
 */
// Per-job counters
@InterfaceAudience.Public
@InterfaceStability.Evolving
public enum JobCounter {
  /** 失败的Map任务数量 */
  NUM_FAILED_MAPS, 
  /** 失败的Reduce任务数量 */
  NUM_FAILED_REDUCES,
  /** 被杀死的Map任务数量 */
  NUM_KILLED_MAPS,
  /** 被杀死的Reduce任务数量 */
  NUM_KILLED_REDUCES,
  /** 总共启动的Map任务数量 */
  TOTAL_LAUNCHED_MAPS,
  /** 总共启动的Reduce任务数量 */
  TOTAL_LAUNCHED_REDUCES,
  /** 数据非本地本地性的Map任务数量 */
  OTHER_LOCAL_MAPS,
  /** 数据本地性的Map任务数量（数据与任务同节点） */
  DATA_LOCAL_MAPS,
  /** 机架本地性的Map任务数量（数据与任务同机架不同节点） */
  RACK_LOCAL_MAPS,
  /** @deprecated 已废弃 Map任务占用slot的总毫秒数 */
  @Deprecated
  SLOTS_MILLIS_MAPS,
  /** @deprecated 已废弃 Reduce任务占用slot的总毫秒数 */
  @Deprecated
  SLOTS_MILLIS_REDUCES,
  /** @deprecated 已废弃 空闲Map slot占用的总毫秒数 */
  @Deprecated
  FALLOW_SLOTS_MILLIS_MAPS,
  /** @deprecated 已废弃 空闲Reduce slot占用的总毫秒数 */
  @Deprecated
  FALLOW_SLOTS_MILLIS_REDUCES,
  /** 总共启动的uber任务数量（小作业优化模式） */
  TOTAL_LAUNCHED_UBERTASKS,
  /** uber任务中包含的Map子任务数量 */
  NUM_UBER_SUBMAPS,
  /** uber任务中包含的Reduce子任务数量 */
  NUM_UBER_SUBREDUCES,
  /** 失败的uber任务数量 */
  NUM_FAILED_UBERTASKS,
  /** 请求被抢占的任务数量 */
  TASKS_REQ_PREEMPT,
  /** Checkpoint总次数 */
  CHECKPOINTS,
  /** Checkpoint总字节数 */
  CHECKPOINT_BYTES,
  /** Checkpoint总耗时（毫秒） */
  CHECKPOINT_TIME,
  /** 所有Map任务总运行时间（毫秒） */
  MILLIS_MAPS,
  /** 所有Reduce任务总运行时间（毫秒） */
  MILLIS_REDUCES,
  /** Map任务vcore秒数总和（CPU资源使用量） */
  VCORES_MILLIS_MAPS,
  /** Reduce任务vcore秒数总和（CPU资源使用量） */
  VCORES_MILLIS_REDUCES,
  /** Map任务内存毫秒总和（MB，内存资源使用量） */
  MB_MILLIS_MAPS,
  /** Reduce任务内存毫秒总和（MB，内存资源使用量） */
  MB_MILLIS_REDUCES
}