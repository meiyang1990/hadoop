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
 * 文件: TaskType.java
 * 所属模块: hadoop-mapreduce-client-core
 * 核心职责: 定义MapReduce框架中所有任务类型的枚举
 * 
 * 枚举类型，用于标识MapReduce作业中不同类型的任务，包括核心计算任务和作业生命周期任务
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public enum TaskType {
  /** Map阶段计算任务 */
  MAP,
  /** Reduce阶段计算任务 */
  REDUCE,
  /** 作业初始化设置任务 */
  JOB_SETUP,
  /** 作业清理任务 */
  JOB_CLEANUP,
  /** 单个任务清理任务 */
  TASK_CLEANUP
}