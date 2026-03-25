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

package org.apache.hadoop.mapreduce.v2.api.records;

/**
 * TaskAttempt完成事件的状态枚举，定义MapReduce任务尝试执行完成后的所有可能结果状态
 * 用于在作业完成事件通知中标识任务尝试的最终执行结果
 */
public enum TaskAttemptCompletionEventStatus {
  /** 任务尝试执行失败 */
  FAILED,
  /** 任务尝试被杀死（主动终止，非执行失败） */
  KILLED,
  /** 任务尝试执行成功 */
  SUCCEEDED,
  /** 任务尝试已过期（被其他更晚启动的尝试取代） */
  OBSOLETE,
  /** 整个任务失败（所有尝试都失败，任务整体标记为失败） */
  TIPFAILED
}