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

/**
 * @file TaskCounters.cc
 * @brief  MapReduce本地任务任务计数器名称定义实现
 *
 * 定义所有MapReduce任务计数器的分组名称和具体计数器名称，
 * 供本地任务运行时统计任务执行指标使用
 */

#include "lib/TaskCounters.h"

namespace NativeTask {

/**
 * 宏定义：批量生成计数器名称的静态成员变量定义
 */
#define DEFINE_COUNTER(name) const char * TaskCounters::name = #name;

// 任务计数器分组名称
const char * TaskCounters::TASK_COUNTER_GROUP = "org.apache.hadoop.mapreduce.TaskCounter";

// 定义任务核心运行指标计数器名称
DEFINE_COUNTER(MAP_INPUT_RECORDS)
DEFINE_COUNTER(MAP_OUTPUT_RECORDS)
DEFINE_COUNTER(MAP_OUTPUT_BYTES)
DEFINE_COUNTER(MAP_OUTPUT_MATERIALIZED_BYTES)
DEFINE_COUNTER(COMBINE_INPUT_RECORDS)
DEFINE_COUNTER(COMBINE_OUTPUT_RECORDS)
DEFINE_COUNTER(SPILLED_RECORDS)

// 文件系统计数器分组名称
const char * TaskCounters::FILESYSTEM_COUNTER_GROUP = "FileSystemCounters";

// 定义文件系统IO指标计数器名称
DEFINE_COUNTER(FILE_BYTES_READ)
DEFINE_COUNTER(FILE_BYTES_WRITTEN)

} // namespace NativeTask