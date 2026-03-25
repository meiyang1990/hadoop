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

package org.apache.hadoop.mapreduce.v2.app.speculate;

/**
 * 数据统计工具类，用于MapReduce推测执行中计算任务运行时间的统计量（均值、方差、标准差等）
 * 为推测判断慢任务是否需要启动推测启动新任务提供统计依据
 */
public class DataStatistics {

  /**
   * 95%置信区间计算系数，对应正态分布95%置信度的z分数
   */
  private static final double DEFAULT_CI_FACTOR = 1.96;
  private int count = 0;
  private double sum = 0;
  private double sumSquares = 0;

  /**
   * 构造空的统计数据对象
   */
  public DataStatistics() {
  }

  /**
   * 用初始数值构造统计数据对象
   * @param initNum 初始数据值
   */
  public DataStatistics(final double initNum) {
    this.count = 1;
    this.sum = initNum;
    this.sumSquares = initNum * initNum;
  }

  /**
   * 添加新的观测值，更新统计量
   * @param newNum 新观测值
   */
  public synchronized void add(final double newNum) {
    this.count++;
    this.sum += newNum;
    this.sumSquares += newNum * newNum;
  }

  /**
   * 更新已有观测值，替换旧值为新值并更新统计量
   * @param old 旧的观测值
   * @param update 新的观测值
   */
  public synchronized void updateStatistics(final double old,
      final double update) {
    this.sum += update - old;
    this.sumSquares += (update * update) - (old * old);
  }

  /**
   * 计算当前所有观测值的均值
   * @return 均值，无数据时返回0.0
   */
  public synchronized double mean() {
    return count == 0 ? 0.0 : sum / count;
  }

  /**
   * 计算当前所有观测值的方差
   * @return 方差，数据量小于等于1时返回0.0
   */
  public synchronized double var() {
    // 方差公式：E(X²) - E(X)²
    if (count <= 1) {
      return 0.0;
    }
    double mean = mean();
    return Math.max((sumSquares / count) - mean * mean, 0.0d);
  }

  /**
   * 计算当前所有观测值的标准差
   * @return 标准差
   */
  public synchronized double std() {
    return Math.sqrt(this.var());
  }

  /**
   * 计算异常值阈值：均值加上指定倍数的标准差，超过该阈值可判定为异常慢任务
   * @param sigma 标准差倍数
   * @return 异常值阈值，无数据时返回0.0
   */
  public synchronized double outlier(final float sigma) {
    if (count != 0.0) {
      return mean() + std() * sigma;
    }

    return 0.0;
  }

  /**
   * 获取观测值数量
   * @return 观测值数量
   */
  public synchronized double count() {
    return count;
  }

  /**
   * 计算95%置信区间的均值上限，用于推测执行中估计任务完成时间的乐观上界
   * @return 95%置信区间均值上限，数据量小于等于1时返回0.0
   */
  public synchronized double meanCI() {
    if (count <= 1) {
      return 0.0;
    }
    double currMean = mean();
    double currStd = std();
    return currMean + (DEFAULT_CI_FACTOR * currStd / Math.sqrt(count));
  }

  @Override
  public String toString() {
    return "DataStatistics: count is " + count + ", sum is " + sum
        + ", sumSquares is " + sumSquares + " mean is " + mean()
        + " std() is " + std() + ", meanCI() is " + meanCI();
  }
}