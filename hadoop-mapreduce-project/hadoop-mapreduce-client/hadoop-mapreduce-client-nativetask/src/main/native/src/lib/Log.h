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
 * @file Log.h
 * @brief  Hadoop MapReduce原生任务日志工具头文件
 * 提供宏定义日志输出能力，支持开关控制，用于NativeTask模块的调试与运行日志打印
 */

#ifndef LOG_H_
#define LOG_H_

#include <stdio.h>
#include <time.h>

/**
 * @brief 原生MapReduce任务命名空间，包含所有NativeTask模块的实现
 */
namespace NativeTask {

// 日志输出开关，定义后开启日志打印
#define PRINT_LOG

#ifdef PRINT_LOG

/**
 * 日志输出文件指针，外部全局定义，指定日志输出目标
 */
extern FILE * LOG_DEVICE;

/**
 * @brief  带时间戳的INFO级别日志打印宏
 * 自动添加当前时间前缀，输出到指定日志设备，仅当LOG_DEVICE非空时输出
 * @param  _fmt_  格式化字符串
 * @param  args   可变参数列表，对应格式化字符串的占位符
 */
#define LOG(_fmt_, args...)   if (LOG_DEVICE) { \
    time_t log_timer; struct tm log_tm; \
    time(&log_timer); localtime_r(&log_timer, &log_tm); \
    fprintf(LOG_DEVICE, "%02d/%02d/%02d %02d:%02d:%02d INFO " _fmt_ "\n", \
    log_tm.tm_year%100, log_tm.tm_mon+1, log_tm.tm_mday, \
    log_tm.tm_hour, log_tm.tm_min, log_tm.tm_sec, \
    ##args);}

#else

// 关闭日志时，日志宏展开为空
#define LOG(_fmt_, args...)

#endif

} // namespace NativeTask

#endif /* LOG_H_ */