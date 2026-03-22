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
 * 文件说明：MapReduce任务状态周期统计实现类，属于MapReduce任务性能统计模块
 *
 * 该类是PeriodicStatsAccumulator的具体实现，用于处理随时间变化的状态量测量。
 * 每个统计区间的结果是该区间内指标按进度加权计算得到的平均值，适合对随任务进度变化的连续指标做统计。
 * 例如统计任务执行过程中不同进度阶段的平均CPU使用率、平均内存占用等指标。
 */
class StatePeriodicStats extends PeriodicStatsAccumulator {
  /**
   * 构造方法，初始化指定区间数量的周期统计器
   * @param count 统计区间的数量
   */
  StatePeriodicStats(int count) {
    super(count);
  }

  /**
   * 扩展累加计算，根据新的进度读数更新统计结果。
   * 通过计算分段线性曲线下的面积来累加加权值，最终得到每个区间的加权平均。
   * @param newProgress 新的任务进度值，范围0~1
   * @param newValue 当前进度对应指标的测量值
   */
  @Override
  protected void extendInternal(double newProgress, int newValue) {
    // 状态未初始化则直接返回
    if (state == null) {
      return;
    }

    // 计算梯形区间的平均高度，即当前段的平均值
    double mean = ((double)newValue + (double)state.oldValue)/2.0D;

    // 累加到当前区间的结果：平均值 * 进度增量 * 区间数量，完成加权累加
    state.currentAccumulation += mean * (newProgress - state.oldProgress) * count;
  }
}