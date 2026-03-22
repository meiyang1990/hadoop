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

/**
 * 包装包私有访问权限的ProgressSplitsBlock，提供外部可访问的进度分片统计能力
 * 用于解决ProgressSplitsBlock为包访问权限无法被外部模块直接使用的问题
 * 为MapReduce任务进度和资源使用统计提供对外访问接口
 */
// Workaround for ProgressSplitBlock being package access
public class WrappedProgressSplitsBlock extends ProgressSplitsBlock {
  // 包装后的挂钟时间统计累加器
  private WrappedPeriodicStatsAccumulator wrappedProgressWallclockTime;
  // 包装后的CPU时间统计累加器
  private WrappedPeriodicStatsAccumulator wrappedProgressCPUTime;
  // 包装后的虚拟内存使用量统计累加器(单位KB)
  private WrappedPeriodicStatsAccumulator wrappedProgressVirtualMemoryKbytes;
  // 包装后的物理内存使用量统计累加器(单位KB)
  private WrappedPeriodicStatsAccumulator wrappedProgressPhysicalMemoryKbytes;

  /**
   * 构造函数，指定分片数量初始化进度分片块
   * @param numberSplits 分片数量
   */
  public WrappedProgressSplitsBlock(int numberSplits) {
    super(numberSplits);
  }

  /**
   * 获取所有分片的突发统计数据，用于分析任务进度波动
   * @return 分片突发统计结果二维数组
   */
  public int[][] burst() {
    return super.burst();
  }

  /**
   * 获取懒加载初始化的挂钟时间统计累加器包装对象
   * @return 包装后的挂钟时间统计累加器
   */
  public WrappedPeriodicStatsAccumulator getProgressWallclockTime() {
    if (wrappedProgressWallclockTime == null) {
      wrappedProgressWallclockTime = new WrappedPeriodicStatsAccumulator(
          progressWallclockTime);
    }
    return wrappedProgressWallclockTime;
  }

  /**
   * 获取懒加载初始化的CPU时间统计累加器包装对象
   * @return 包装后的CPU时间统计累加器
   */
  public WrappedPeriodicStatsAccumulator getProgressCPUTime() {
    if (wrappedProgressCPUTime == null) {
      wrappedProgressCPUTime = new WrappedPeriodicStatsAccumulator(
          progressCPUTime);
    }
    return wrappedProgressCPUTime;
  }

  /**
   * 获取懒加载初始化的虚拟内存使用量统计累加器包装对象
   * @return 包装后的虚拟内存使用量统计累加器(单位KB)
   */
  public WrappedPeriodicStatsAccumulator getProgressVirtualMemoryKbytes() {
    if (wrappedProgressVirtualMemoryKbytes == null) {
      wrappedProgressVirtualMemoryKbytes = new WrappedPeriodicStatsAccumulator(
          progressVirtualMemoryKbytes);
    }
    return wrappedProgressVirtualMemoryKbytes;
  }

  /**
   * 获取懒加载初始化的物理内存使用量统计累加器包装对象
   * @return 包装后的物理内存使用量统计累加器(单位KB)
   */
  public WrappedPeriodicStatsAccumulator getProgressPhysicalMemoryKbytes() {
    if (wrappedProgressPhysicalMemoryKbytes == null) {
      wrappedProgressPhysicalMemoryKbytes = new WrappedPeriodicStatsAccumulator(
          progressPhysicalMemoryKbytes);
    }
    return wrappedProgressPhysicalMemoryKbytes;
  }
}