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
 * MapReduce任务执行指标计数器枚举，定义了任务运行过程中需要统计的各类性能和数据指标
 * 这些指标用于监控任务执行状态、分析性能瓶颈和作业调优
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public enum TaskCounter {
  /** Map阶段输入记录数 */
  MAP_INPUT_RECORDS,
  /** Map阶段输出记录数 */
  MAP_OUTPUT_RECORDS,
  /** Map阶段跳过的坏记录数 */
  MAP_SKIPPED_RECORDS,
  /** Map阶段输出数据字节数 */
  MAP_OUTPUT_BYTES,
  /** Map阶段物化输出字节数（写入磁盘的大小） */
  MAP_OUTPUT_MATERIALIZED_BYTES,
  /** 输入分片原始数据字节数 */
  SPLIT_RAW_BYTES,
  /** Combine阶段输入记录数 */
  COMBINE_INPUT_RECORDS,
  /** Combine阶段输出记录数 */
  COMBINE_OUTPUT_RECORDS,
  /** Reduce阶段输入分组数 */
  REDUCE_INPUT_GROUPS,
  /** Reduce阶段Shuffle获取字节数 */
  REDUCE_SHUFFLE_BYTES,
  /** Reduce阶段输入记录数 */
  REDUCE_INPUT_RECORDS,
  /** Reduce阶段输出记录数 */
  REDUCE_OUTPUT_RECORDS,
  /** Reduce阶段跳过的坏分组数 */
  REDUCE_SKIPPED_GROUPS,
  /** Reduce阶段跳过的坏记录数 */
  REDUCE_SKIPPED_RECORDS,
  /** 溢出到磁盘的总记录数 */
  SPILLED_RECORDS,
  /** 成功完成Shuffle的Map任务数 */
  SHUFFLED_MAPS, 
  /** Shuffle过程中失败的拷贝次数 */
  FAILED_SHUFFLE,
  /** 合并的Map输出段数 */
  MERGED_MAP_OUTPUTS,
  /** GC垃圾回收耗时（毫秒） */
  GC_TIME_MILLIS,
  /** CPU总使用时间（毫秒） */
  CPU_MILLISECONDS,
  /** 物理内存使用量（字节） */
  PHYSICAL_MEMORY_BYTES,
  /** 虚拟内存使用量（字节） */
  VIRTUAL_MEMORY_BYTES,
  /** 已提交堆内存大小（字节） */
  COMMITTED_HEAP_BYTES,
  /** Map任务物理内存使用峰值（字节） */
  MAP_PHYSICAL_MEMORY_BYTES_MAX,
  /** Map任务虚拟内存使用峰值（字节） */
  MAP_VIRTUAL_MEMORY_BYTES_MAX,
  /** Reduce任务物理内存使用峰值（字节） */
  REDUCE_PHYSICAL_MEMORY_BYTES_MAX,
  /** Reduce任务虚拟内存使用峰值（字节） */
  REDUCE_VIRTUAL_MEMORY_BYTES_MAX;
}