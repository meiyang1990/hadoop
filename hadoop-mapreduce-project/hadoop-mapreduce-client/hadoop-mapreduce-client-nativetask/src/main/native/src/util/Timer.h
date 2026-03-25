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
 * @file Timer.h
 * @brief 原生任务模块高精度计时器头文件，用于性能监控和计算速度统计
 *
 * 为Hadoop MapReduce原生任务提供计时能力，支持统计时间间隔、处理速度等性能指标，
 * 主要用于任务执行过程中的性能调试与监控统计。
 */

#ifndef TIMER_H_
#define TIMER_H_

#include <stdint.h>
#include <stdio.h>
#include <string>

namespace NativeTask {

using std::string;

/**
 * @class Timer
 * @brief 高精度计时器类，用于统计时间间隔和计算数据处理速度
 *
 * 记录上一次计时点的时间戳，提供多种格式化输出方法，
 * 可生成包含时间间隔、处理速度的调试信息字符串，
 * 用于原生MapReduce任务的性能监控。
 */
class Timer {
protected:
  // 上一次计时点的时间戳（纳秒级）
  uint64_t _last;
public:
  Timer();
  ~Timer();

  /**
   * @brief 获取上一次计时点的时间戳
   * @return 上一次计时点的时间戳（纳秒）
   */
  uint64_t last();

  /**
   * @brief 获取当前时间戳
   * @return 当前时间戳（纳秒）
   */
  uint64_t now();

  /**
   * @brief 重置计时器，将当前时间设置为上一次计时点
   */
  void reset();

  /**
   * @brief 获取距离上一次计时的时间间隔，格式化输出
   * @param msg 消息前缀，用于标识统计场景
   * @return 格式化后的时间间隔信息字符串
   */
  string getInterval(const char * msg);

  /**
   * @brief 计算并格式化输出单批次数据处理速度
   * @param msg 消息前缀，用于标识统计场景
   * @param size 处理的数据总大小（字节）
   * @return 格式化后的处理速度信息字符串，单位为MB/s
   */
  string getSpeed(const char * msg, uint64_t size);

  /**
   * @brief 计算并格式化输出双批次数据处理速度
   * @param msg 消息前缀，用于标识统计场景
   * @param size1 第一部分数据大小（字节）
   * @param size2 第二部分数据大小（字节）
   * @return 格式化后的总处理速度信息字符串，单位为MB/s
   */
  string getSpeed2(const char * msg, uint64_t size1, uint64_t size2);

  /**
   * @brief 计算并格式化输出单批次数据处理速度（兆字节单位展示）
   * @param msg 消息前缀，用于标识统计场景
   * @param size 处理的数据总大小（兆字节）
   * @return 格式化后的处理速度信息字符串，单位为MB/s
   */
  string getSpeedM(const char * msg, uint64_t size);

  /**
   * @brief 计算并格式化输出双批次数据处理速度（兆字节单位输入）
   * @param msg 消息前缀，用于标识统计场景
   * @param size1 第一部分数据大小（兆字节）
   * @param size2 第二部分数据大小（兆字节）
   * @return 格式化后的总处理速度信息字符串，单位为MB/s
   */
  string getSpeedM2(const char * msg, uint64_t size1, uint64_t size2);
};

} // namespace NativeTask

#endif /* TIMER_H_ */