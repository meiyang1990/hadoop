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
 * 周期性统计数据累加器抽象类，用于按任务进度分段累加任务运行过程中的指标观测值
 * 
 * 将0.0到1.0的任务进度范围平均划分为多个桶（进度段），每个桶保存对应进度区间内的观测值统计结果。
 * 子类通过实现抽象方法决定不同类型指标的累加计算方式，支持递增型指标（如CPU时间）和任意波动型指标（如内存占用）。
 * 该设计通过分段统计节省JobTracker内存，同时保留任务运行过程中指标随进度变化的分布信息。
 */
@Private
@Unstable
public abstract class PeriodicStatsAccumulator {
  // 将0.0到1.0的任务进度范围划分为count个进度段，每个进度段对应一个统计桶
  // 当前提供两种实现：
  // 1. 单调递增型指标：如CPU时间、墙钟时间（单位毫秒）
  // 2. 任意波动型指标：如物理内存、虚拟内存占用（单位KB）
  // 使用int存储结果，可节省JVM堆空间，限制：任务运行不超过57天，内存不超过2TB，满足绝大多数场景
  protected final int count;
  protected final int[] values;
    
  /**
   * 统计状态容器，保存上一次观测的状态数据，用于增量计算
   */
  static class StatsetState {
    int oldValue = 0;
    double oldProgress = 0.0D;

    double currentAccumulation = 0.0D;
  }

  // 引入间接引用减少已完成任务的内存占用，任务进度到达1.0后会释放该对象
  StatsetState state = new StatsetState();

  /**
   * 构造方法，初始化指定分段数量的统计累加器
   * @param count 进度分段数量，将0~1进度平均划分为count个区间
   */
  PeriodicStatsAccumulator(int count) {
    this.count = count;
    this.values = new int[count];
    for (int i = 0; i < count; ++i) {
      values[i] = -1;
    }
  }

  /**
   * 获取所有分段统计结果数组
   * @return 存放各分段统计值的数组
   */
  protected int[] getValues() {
    return values;
  }

  /**
   * 抽象方法，子类实现当前进度区间内的指标累加逻辑
   * 
   * 调用约定：
   * 1. 首次调用时，oldProgress和oldValue均为0
   * 2. 后续调用时，oldXXX为上一次调用的newXXX值，保证累加连续性
   * 3. currentAccumulation由当前方法和initializeInterval共同维护
   * 
   * @param newProgress 当前区间结束进度，本次观测覆盖区间为[oldProgress, newProgress]
   * @param newValue 当前结束进度对应的指标观测值
   */
  protected abstract void extendInternal(double newProgress, int newValue);

  /**
   * 初始化新进度区间的状态变量，为新分段累加做准备
   */
  protected void initializeInterval() {
    state.currentAccumulation = 0.0D;
  }

  /**
   * 处理新的指标观测值，按进度分段拆分区间并调用累加逻辑
   * 
   * 将从上一次观测到本次观测的进度区间，按分段边界拆分为多个子区间，
   * 对每个子区间通过插值计算边界值，依次调用extendInternal完成每个分段的累加，
   * 最后处理剩余未跨边界的区间部分。任务完成后释放状态对象节省内存。
   * 
   * @param newProgress 本次观测的进度终点
   * @param newValue 本次观测的指标值
   */    
  protected void extend(double newProgress, int newValue) {
    // 状态已释放或进度回退，直接返回
    if (state == null || newProgress < state.oldProgress) {
      return;
    }

    // 计算旧进度和新进度对应的分段索引
    int oldIndex = (int)(state.oldProgress * count);
    int newIndex = (int)(newProgress * count);
    int originalOldValue = state.oldValue;

    // 计算总指标变化量和总进度变化量，用于插值计算
    double fullValueDistance = (double)newValue - state.oldValue;
    double fullProgressDistance = newProgress - state.oldProgress;
    double originalOldProgress = state.oldProgress;

    // 遍历所有跨越的分段边界，逐个处理每个完整分段
    for (int closee = oldIndex; closee < newIndex; ++closee) {
      // 计算当前分段边界的进度值
      double interpolationProgress = (double)(closee + 1) / count;
      // 处理浮点精度问题，确保不超过本次观测进度
      interpolationProgress = Math.min(interpolationProgress, newProgress);

      // 计算当前分段的进度占比，插值计算边界位置的指标值
      double progressLength = (interpolationProgress - originalOldProgress);
      double interpolationProportion = progressLength / fullProgressDistance;
      double interpolationValueDistance
        = fullValueDistance * interpolationProportion;
      int interpolationValue
        = (int)interpolationValueDistance + originalOldValue;

      // 累加当前分段数据，更新状态，保存当前分段统计结果
      extendInternal(interpolationProgress, interpolationValue);
      advanceState(interpolationProgress, interpolationValue);
      values[closee] = (int)state.currentAccumulation;
      // 初始化下一个分段的状态
      initializeInterval();
    }

    // 处理最后一个不完整分段（未到达下一个边界）
    extendInternal(newProgress, newValue);
    advanceState(newProgress, newValue);

    // 进度到达终点，释放状态对象节省内存
    if (newIndex == count) {
      state = null;
    }
  }

  /**
   * 更新状态对象，保存本次观测结果作为下一次计算的基准
   * @param newProgress 本次观测进度
   * @param newValue 本次观测指标值
   */
  protected void advanceState(double newProgress, int newValue) {
    state.oldValue = newValue;
    state.oldProgress = newProgress;
  }    

  /**
   * 获取进度分段总数
   * @return 分段数量
   */
  int getCount() {
    return count;
  }

  /**
   * 获取指定索引分段的统计结果
   * @param index 分段索引
   * @return 对应分段的统计值
   */
  int get(int index) {
    return values[index];
  }
}