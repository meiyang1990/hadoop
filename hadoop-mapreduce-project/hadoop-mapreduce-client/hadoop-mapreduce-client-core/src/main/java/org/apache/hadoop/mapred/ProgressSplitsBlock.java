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

package org.apache.hadoop.mapred;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;

/**
 * 文件说明：MapReduce任务尝试进度分块统计容器，整合任务执行过程中不同维度的周期性进度统计数据
 * 将-wallclock时间、CPU时间、内存使用等多个统计数据聚合为单个对象，方便统一打包传输与处理
 */
@Private
@Unstable
public class ProgressSplitsBlock {
  // 挂钟时间（实际耗时）进度统计累加器
  final PeriodicStatsAccumulator progressWallclockTime;
  // CPU时间进度统计累加器
  final PeriodicStatsAccumulator progressCPUTime;
  // 虚拟内存使用量（单位：KB）进度统计累加器
  final PeriodicStatsAccumulator progressVirtualMemoryKbytes;
  // 物理内存使用量（单位：KB）进度统计累加器
  final PeriodicStatsAccumulator progressPhysicalMemoryKbytes;

  // 空数组常量，用于空值返回
  static final int[] NULL_ARRAY = new int[0];

  // 挂钟时间统计结果在结果数组中的索引
  static final int WALLCLOCK_TIME_INDEX = 0;
  // CPU时间统计结果在结果数组中的索引
  static final int CPU_TIME_INDEX = 1;
  // 虚拟内存统计结果在结果数组中的索引
  static final int VIRTUAL_MEMORY_KBYTES_INDEX = 2;
  // 物理内存统计结果在结果数组中的索引
  static final int PHYSICAL_MEMORY_KBYTES_INDEX = 3;

  // 默认进度分块数量
  static final int DEFAULT_NUMBER_PROGRESS_SPLITS = 12;

  /**
   * 构造函数，初始化不同维度的进度统计累加器
   * @param numberSplits 进度分块数量
   */
  ProgressSplitsBlock(int numberSplits) {
    progressWallclockTime
      = new CumulativePeriodicStats(numberSplits);
    progressCPUTime
      = new CumulativePeriodicStats(numberSplits);
    progressVirtualMemoryKbytes
      = new StatePeriodicStats(numberSplits);
    progressPhysicalMemoryKbytes
      = new StatePeriodicStats(numberSplits);
  }

  /**
   * 将所有维度的统计结果导出为二维数组，与LoggedTaskAttempt.SplitVectorKind格式对齐
   * @return 二维数组，第一维对应不同统计维度，第二维是该维度的统计结果数组
   */
  // this coordinates with LoggedTaskAttempt.SplitVectorKind
  int[][] burst() {
    int[][] result = new int[4][];

    result[WALLCLOCK_TIME_INDEX] = progressWallclockTime.getValues();
    result[CPU_TIME_INDEX] = progressCPUTime.getValues();
    result[VIRTUAL_MEMORY_KBYTES_INDEX] = progressVirtualMemoryKbytes.getValues();
    result[PHYSICAL_MEMORY_KBYTES_INDEX] = progressPhysicalMemoryKbytes.getValues();

    return result;
  }

  /**
   * 从导出的二维结果数组中获取指定索引的统计数组
   * @param burstedBlock 导出的二维统计结果数组
   * @param index 目标维度索引
   * @return 指定维度的统计结果数组，输入为null时返回空数组
   */
  static public int[] arrayGet(int[][] burstedBlock, int index) {
    return burstedBlock == null ? NULL_ARRAY : burstedBlock[index];
  }

  /**
   * 从导出结果中获取挂钟时间统计数组
   * @param burstedBlock 导出的二维统计结果数组
   * @return 挂钟时间统计数组
   */
  static public int[] arrayGetWallclockTime(int[][] burstedBlock) {
    return arrayGet(burstedBlock, WALLCLOCK_TIME_INDEX);
  }

  /**
   * 从导出结果中获取CPU时间统计数组
   * @param burstedBlock 导出的二维统计结果数组
   * @return CPU时间统计数组
   */
  static public int[] arrayGetCPUTime(int[][] burstedBlock) {
    return arrayGet(burstedBlock, CPU_TIME_INDEX);
  }

  /**
   * 从导出结果中获取虚拟内存使用量统计数组
   * @param burstedBlock 导出的二维统计结果数组
   * @return 虚拟内存使用量统计数组（单位：KB）
   */
  static public int[] arrayGetVMemKbytes(int[][] burstedBlock) {
    return arrayGet(burstedBlock, VIRTUAL_MEMORY_KBYTES_INDEX);
  }

  /**
   * 从导出结果中获取物理内存使用量统计数组
   * @param burstedBlock 导出的二维统计结果数组
   * @return 物理内存使用量统计数组（单位：KB）
   */
  static public int[] arrayGetPhysMemKbytes(int[][] burstedBlock) {
    return arrayGet(burstedBlock, PHYSICAL_MEMORY_KBYTES_INDEX);
  }
}