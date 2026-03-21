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

package org.apache.hadoop.yarn.server.timelineservice.metrics;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableQuantiles;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.hadoop.metrics2.lib.Interns.info;

/**
 * 时间线读取器的指标统计类，用于收集时间线服务读取操作的性能指标。
 */
@Metrics(about = "Metrics for timeline reader", context = "timelineservice")
final public class TimelineReaderMetrics {

  // 时间线读取器指标元信息定义
  private final static MetricsInfo METRICS_INFO = info("TimelineReaderMetrics",
      "Metrics for TimelineReader");
  // 标记单例是否已初始化，保证线程安全
  private static AtomicBoolean isInitialized = new AtomicBoolean(false);
  // 单例实例
  private static TimelineReaderMetrics instance = null;

  /** 查询实体失败延迟分位数统计 */
  @Metric(about = "GET entities failure latency", valueName = "latency")
  private MutableQuantiles getEntitiesFailureLatency;
  /** 查询实体成功延迟分位数统计 */
  @Metric(about = "GET entities success latency", valueName = "latency")
  private MutableQuantiles getEntitiesSuccessLatency;

  /** 查询实体类型失败延迟分位数统计 */
  @Metric(about = "GET entity types failure latency", valueName = "latency")
  private MutableQuantiles getEntityTypesFailureLatency;
  /** 查询实体类型成功延迟分位数统计 */
  @Metric(about = "GET entity types success latency", valueName = "latency")
  private MutableQuantiles getEntityTypesSuccessLatency;

  private TimelineReaderMetrics() {
  }

  /**
   * 获取TimelineReaderMetrics单例实例，懒加载初始化。
   * @return 单例实例
   */
  public static TimelineReaderMetrics getInstance() {
    if (!isInitialized.get()) {
      synchronized (TimelineReaderMetrics.class) {
        if (instance == null) {
          // 向默认指标系统注册当前指标实例
          instance =
              DefaultMetricsSystem.initialize("TimelineService").register(
                  METRICS_INFO.name(), METRICS_INFO.description(),
                  new TimelineReaderMetrics());
          isInitialized.set(true);
        }
      }
    }
    return instance;
  }

  /**
   * 销毁单例实例，重置初始化状态。
   */
  public synchronized static void destroy() {
    isInitialized.set(false);
    instance = null;
  }

  @VisibleForTesting
  public MutableQuantiles getGetEntitiesSuccessLatency() {
    return getEntitiesSuccessLatency;
  }

  @VisibleForTesting
  public MutableQuantiles getGetEntitiesFailureLatency() {
    return getEntitiesFailureLatency;
  }

  @VisibleForTesting
  public MutableQuantiles getGetEntityTypesSuccessLatency() {
    return getEntityTypesSuccessLatency;
  }

  @VisibleForTesting
  public MutableQuantiles getGetEntityTypesFailureLatency() {
    return getEntityTypesFailureLatency;
  }

  /**
   * 添加查询实体操作的延迟记录，按成功/失败分类统计。
   * @param durationMs 操作耗时，单位毫秒
   * @param succeeded 操作是否成功
   */
  public void addGetEntitiesLatency(
      long durationMs, boolean succeeded) {
    if (succeeded) {
      getEntitiesSuccessLatency.add(durationMs);
    } else {
      getEntitiesFailureLatency.add(durationMs);
    }
  }

  /**
   * 添加查询实体类型操作的延迟记录，按成功/失败分类统计。
   * @param durationMs 操作耗时，单位毫秒
   * @param succeeded 操作是否成功
   */
  public void addGetEntityTypesLatency(
      long durationMs, boolean succeeded) {
    if (succeeded) {
      getEntityTypesSuccessLatency.add(durationMs);
    } else {
      getEntityTypesFailureLatency.add(durationMs);
    }
  }
}