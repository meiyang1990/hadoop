// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdfs.server.datanode.metrics;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.server.protocol.OutlierMetrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * 异常值检测工具类，用于在DataNode节点/磁盘集合中识别出聚合延迟明显高于其他成员的异常节点/磁盘
 * 
 * 采用Leys等人提出的基于中位数绝对偏差(MAD)的异常检测算法，并补充了启发式规则避免误判：
 * 1. 样本量过少时跳过异常检测
 * 2. 聚合延迟低于最低阈值的不会被标记为异常
 * 3. 聚合延迟低于中位数倍数的不会被标记为异常
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class OutlierDetector {
  public static final Logger LOG =
      LoggerFactory.getLogger(OutlierDetector.class);

  /**
   * 执行异常检测所需的最小资源样本数量
   */
  private volatile long minNumResources;

  /**
   * MAD算法的常量乘数
   */
  private static final double MAD_MULTIPLIER = (double) 1.4826;

  /**
   * 延迟最低阈值（毫秒），低于该值的节点/磁盘一定不会被判定为慢节点
   */
  private volatile long lowThresholdMs;

  /**
   * 偏差乘数：若样本超过中位数达到 (乘数 * 中位数绝对偏差)，则判定为异常值，3是保守选择
   */
  private static final int DEVIATION_MULTIPLIER = 3;

  /**
   * 中位数乘数：当多数样本聚集时MAD可能很小，该参数用于防止过度检测
   */
  @VisibleForTesting
  static final int MEDIAN_MULTIPLIER = 3;

  /**
   * 构造异常检测器
   * @param minNumResources 最小资源样本数量
   * @param lowThresholdMs 最低延迟阈值（毫秒）
   */
  public OutlierDetector(long minNumResources, long lowThresholdMs) {
    this.minNumResources = minNumResources;
    this.lowThresholdMs = lowThresholdMs;
  }

  /**
   * 识别出延迟远高于其他节点/磁盘的异常资源集合
   * @param stats 资源到聚合延迟的映射表，聚合延迟可以是平均值或百分位数（如90分位）
   * @return 异常资源名称到实际延迟的映射表
   */
  public Map<String, Double> getOutliers(Map<String, Double> stats) {
    final Map<String, Double> slowResources = new HashMap<>();
    Map<String, OutlierMetrics> slowResourceMetrics = getOutlierMetrics(stats);
    slowResourceMetrics.forEach(
        (node, outlierMetrics) -> slowResources.put(node, outlierMetrics.getActualLatency()));
    return slowResources;
  }

  /**
   * 识别出延迟远高于其他节点/磁盘的异常资源集合，返回包含完整检测指标的结果
   * @param stats 资源到聚合延迟的映射表，聚合延迟可以是平均值或百分位数（如90分位）
   * @return 异常资源名称到检测指标的映射表
   */
  public Map<String, OutlierMetrics> getOutlierMetrics(Map<String, Double> stats) {
    // 样本量不足，跳过异常检测
    if (stats.size() < minNumResources) {
      LOG.debug("Skipping statistical outlier detection as we don't have " +
              "latency data for enough resources. Have {}, need at least {}",
          stats.size(), minNumResources);
      return ImmutableMap.of();
    }
    // 提取所有延迟值并排序
    final List<Double> sorted = new ArrayList<>(stats.values());
    Collections.sort(sorted);
    // 计算中位数和中位数绝对偏差
    final Double median = computeMedian(sorted);
    final Double mad = computeMad(sorted);
    // 计算异常延迟上限，取最大阈值保证保守性
    Double upperLimitLatency = Math.max(
        lowThresholdMs, median * MEDIAN_MULTIPLIER);
    upperLimitLatency = Math.max(
        upperLimitLatency, median + (DEVIATION_MULTIPLIER * mad));

    final Map<String, OutlierMetrics> slowResources = new HashMap<>();

    LOG.trace("getOutliers: List={}, MedianLatency={}, "
            + "MedianAbsoluteDeviation={}, upperLimitLatency={}", sorted, median, mad,
        upperLimitLatency);

    // 遍历所有资源，收集超过延迟上限的异常资源
    for (Map.Entry<String, Double> entry : stats.entrySet()) {
      if (entry.getValue() > upperLimitLatency) {
        OutlierMetrics outlierMetrics =
            new OutlierMetrics(median, mad, upperLimitLatency, entry.getValue());
        slowResources.put(entry.getKey(), outlierMetrics);
      }
    }
    return slowResources;
  }

  /**
   * 计算已排序列表的中位数绝对偏差(MAD)
   * @param sortedValues 已排序的数值列表
   * @return 中位数绝对偏差结果
   */
  public static Double computeMad(List<Double> sortedValues) {
    if (sortedValues.size() == 0) {
      throw new IllegalArgumentException(
          "Cannot compute the Median Absolute Deviation " +
              "of an empty list.");
    }

    // 先计算原列表的中位数
    Double median = computeMedian(sortedValues);
    List<Double> deviations = new ArrayList<>(sortedValues);

    // 计算每个值与中位数的绝对偏差
    for (int i = 0; i < sortedValues.size(); ++i) {
      deviations.set(i, Math.abs(sortedValues.get(i) - median));
    }

    // 对偏差排序后计算中位数，再乘以常量乘数得到最终结果
    Collections.sort(deviations);
    return computeMedian(deviations) * MAD_MULTIPLIER;
  }

  /**
   * 计算已排序列表的中位数
   * @param sortedValues 已排序的数值列表
   * @return 中位数结果
   */
  public static Double computeMedian(List<Double> sortedValues) {
    if (sortedValues.size() == 0) {
      throw new IllegalArgumentException(
          "Cannot compute the median of an empty list.");
    }

    Double median = sortedValues.get(sortedValues.size() / 2);
    if (sortedValues.size() % 2 == 0) {
      median += sortedValues.get((sortedValues.size() / 2) - 1);
      median /= 2;
    }
    return median;
  }

  /**
   * 设置异常检测所需的最小资源样本数量
   * @param minNodes 最小资源样本数量
   */
  public void setMinNumResources(long minNodes) {
    minNumResources = minNodes;
  }

  /**
   * 获取异常检测所需的最小资源样本数量
   * @return 最小资源样本数量
   */
  public long getMinOutlierDetectionNodes() {
    return minNumResources;
  }

  /**
   * 设置最低延迟阈值
   * @param thresholdMs 最低延迟阈值（毫秒）
   */
  public void setLowThresholdMs(long thresholdMs) {
    lowThresholdMs = thresholdMs;
  }

  /**
   * 获取最低延迟阈值
   * @return 最低延迟阈值（毫秒）
   */
  public long getLowThresholdMs() {
    return lowThresholdMs;
  }
}