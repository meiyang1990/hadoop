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
 * @file TaskCounters.h
 * @brief 原生MapReduce任务计数器名称常量定义
 * 
 * 该文件定义了原生任务执行过程中需要上报给Hadoop的各类计数器名称，
 * 用于任务运行指标统计，包括Map阶段指标、Combine阶段指标和文件系统IO指标。
 */

#ifndef TASKCOUNTERS_H_
#define TASKCOUNTERS_H_

namespace NativeTask {

/**
 * @class TaskCounters
 * @brief 原生MapReduce任务计数器常量容器
 * 
 * 该类仅包含静态字符串常量，定义了任务运行过程中各类统计指标的名称，
 * 供原生任务在统计指标时统一使用，保证和Java端计数器名称一致。
 */
class TaskCounters {
public:
  // 任务计数器分组名称
  static const char * TASK_COUNTER_GROUP;

  // Map阶段输入记录数计数器名称
  static const char * MAP_INPUT_RECORDS;
  // Map阶段输出记录数计数器名称
  static const char * MAP_OUTPUT_RECORDS;
  // Map阶段输出字节数计数器名称
  static const char * MAP_OUTPUT_BYTES;
  // Map阶段物化输出字节数计数器名称（溢写磁盘的字节数）
  static const char * MAP_OUTPUT_MATERIALIZED_BYTES;
  // Combine阶段输入记录数计数器名称
  static const char * COMBINE_INPUT_RECORDS;
  // Combine阶段输出记录数计数器名称
  static const char * COMBINE_OUTPUT_RECORDS;
  // 溢写磁盘记录数计数器名称
  static const char * SPILLED_RECORDS;

  // 文件系统计数器分组名称
  static const char * FILESYSTEM_COUNTER_GROUP;

  // 文件读取字节数计数器名称
  static const char * FILE_BYTES_READ;
  // 文件写入字节数计数器名称
  static const char * FILE_BYTES_WRITTEN;
};

} // namespace NativeTask

#endif /* TASKCOUNTERS_H_ */