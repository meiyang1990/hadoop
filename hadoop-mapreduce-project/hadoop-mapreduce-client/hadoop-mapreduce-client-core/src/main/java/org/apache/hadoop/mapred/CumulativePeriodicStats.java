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
 * 累加型周期性统计实现，继承自PeriodicStatsAccumulator，用于处理增量累加类指标的分区间统计计算
 * 
 * 该类针对原始数据为累计增量的场景进行统计，每个统计区间会计算出该区间对应进度范围内，指标按进度加权后的增量值
 * 典型示例：行程总距离，可计算每个进度区间内对应的行驶距离增量
 */
class CumulativePeriodicStats extends PeriodicStatsAccumulator {
  // int类型存储在此场景下可满足需求，任务运行最长约一周，不会出现int溢出问题（足够容纳24天内的增量差）
  // 存储上一次读取的累计指标值
  int previousValue = 0;

  /**
   * 构造方法，初始化指定个数统计区间的累加统计器
   * @param count 统计区间的数量
   */
  CumulativePeriodicStats(int count) {
    super(count);
  }

  /**
   * 扩展更新当前统计区间的累加值，计算并累加上次读取后新增的增量
   */
  @Override
  protected void extendInternal(double newProgress, int newValue) {
    // 若状态为空直接返回，不执行累加
    if (state == null) {
      return;
    }

    // 将本次增量累加到当前区间的累计值中
    state.currentAccumulation += (double)(newValue - previousValue);
    // 更新上一次值为当前值，供下一次计算使用
    previousValue = newValue;
  }
}