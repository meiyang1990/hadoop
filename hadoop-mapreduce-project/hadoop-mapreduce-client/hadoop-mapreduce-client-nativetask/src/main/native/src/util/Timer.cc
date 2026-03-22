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
 * @file Timer.cc
 * @brief Hadoop MapReduce本地任务计时工具实现
 * @details 提供跨平台高精度计时能力，支持计算时间间隔、数据处理速度统计，
 * 用于本地Native任务的性能监控与调试
 */

#include <time.h>
#include "lib/commons.h"
#include "util/StringUtil.h"
#include "util/Timer.h"

namespace NativeTask {

#ifdef __MACH__
#include <mach/clock.h>
#include <mach/mach.h>

/**
 * @brief macOS平台获取当前高精度时间实现
 * @return 当前时间，单位纳秒
 */
static uint64_t clock_get() {
  clock_serv_t cclock;
  mach_timespec_t mts;
  host_get_clock_service(mach_host_self(), CALENDAR_CLOCK, &cclock);
  clock_get_time(cclock, &mts);
  mach_port_deallocate(mach_task_self(), cclock);
  return 1000000000ULL * mts.tv_sec + mts.tv_nsec;
}

#else

/**
 * @brief Linux平台获取当前高精度时间实现
 * @return 当前时间，单位纳秒
 */
static uint64_t clock_get() {
  timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  return 1000000000 * ts.tv_sec + ts.tv_nsec;
}

#endif

/**
 * @class Timer
 * @brief 高精度计时器，用于统计代码执行时间和处理速度
 * @details 支持记录上次计时时间点，计算间隔，格式化输出时间间隔和处理速度，
 * 适配macOS和Linux平台获取高精度系统时间
 */

/**
 * @brief 构造函数，初始化计时器并记录当前时间作为起始点
 */
Timer::Timer() {
  _last = clock_get();
}

/**
 * @brief 析构函数
 */
Timer::~Timer() {
}

/**
 * @brief 获取上次记录的时间点
 * @return 上次计时时间，单位纳秒
 */
uint64_t Timer::last() {
  return _last;
}

/**
 * @brief 获取当前时间
 * @return 当前系统时间，单位纳秒
 */
uint64_t Timer::now() {
  return clock_get();
}

/**
 * @brief 重置计时器，将当前时间设为新的起始点
 */
void Timer::reset() {
  _last = clock_get();
}

/**
 * @brief 获取从上一次计时到现在的时间间隔，格式化输出
 * @param msg 输出消息前缀，用于标识计时点
 * @return 格式化后的时间间隔字符串，单位秒
 */
string Timer::getInterval(const char * msg) {
  uint64_t now = clock_get();
  uint64_t interval = now - _last;
  _last = now;
  return StringUtil::Format("%s time: %.5lfs", msg, (double)interval / 1000000000.0);
}

/**
 * @brief 计算并格式化输出数据处理速度（按字节统计）
 * @param msg 输出消息前缀
 * @param size 处理的数据大小，单位字节
 * @return 格式化后的时间、数据大小、处理速度字符串
 */
string Timer::getSpeed(const char * msg, uint64_t size) {
  uint64_t now = clock_get();
  double interval = (now - _last) / 1000000000.0;
  _last = now;
  double speed = size / interval;
  return StringUtil::Format("%s time:\t %3.5lfs size: %10llu speed: %12.0lf/s", msg, interval, size,
      speed);
}

/**
 * @brief 计算并格式化输出数据处理速度（按MB统计）
 * @param msg 输出消息前缀
 * @param size 处理的数据大小，单位字节
 * @return 格式化后的时间、数据大小(MB)、处理速度(MB/s)字符串
 */
string Timer::getSpeedM(const char * msg, uint64_t size) {
  uint64_t now = clock_get();
  double interval = (now - _last) / 1000000000.0;
  _last = now;
  double msize = size / (1024.0 * 1024.0);
  double speed = msize / interval;
  return StringUtil::Format("%s time: %3.5lfs size: %.3lfM speed: %.2lfM/s", msg, interval, msize,
      speed);
}

/**
 * @brief 计算并格式化输出两组数据的处理速度（按字节统计）
 * @param msg 输出消息前缀
 * @param size1 第一组处理数据大小，单位字节
 * @param size2 第二组处理数据大小，单位字节
 * @return 格式化后的时间、两组数据大小、处理速度字符串
 */
string Timer::getSpeed2(const char * msg, uint64_t size1, uint64_t size2) {
  uint64_t now = clock_get();
  double interval = (now - _last) / 1000000000.0;
  _last = now;
  double speed1 = size1 / interval;
  double speed2 = size2 / interval;
  return StringUtil::Format("%s time: %3.5lfs size: %llu/%llu speed: %.0lf/%.0lf", msg, interval,
      size1, size2, speed1, speed2);
}

/**
 * @brief 计算并格式化输出两组数据的处理速度（按MB统计）
 * @param msg 输出消息前缀
 * @param size1 第一组处理数据大小，单位字节
 * @param size2 第二组处理数据大小，单位字节
 * @return 格式化后的时间、两组数据大小(MB)、处理速度(MB/s)字符串
 */
string Timer::getSpeedM2(const char * msg, uint64_t size1, uint64_t size2) {
  uint64_t now = clock_get();
  double interval = (now - _last) / 1000000000.0;
  _last = now;
  double msize1 = size1 / (1024.0 * 1024.0);
  double speed1 = msize1 / interval;
  double msize2 = size2 / (1024.0 * 1024.0);
  double speed2 = msize2 / interval;
  return StringUtil::Format("%s time: %3.5lfs size: %.3lfM/%.3lfM speed: %.2lfM/%.2lfM", msg,
      interval, msize1, msize2, speed1, speed2);
}

} // namespace NativeTask