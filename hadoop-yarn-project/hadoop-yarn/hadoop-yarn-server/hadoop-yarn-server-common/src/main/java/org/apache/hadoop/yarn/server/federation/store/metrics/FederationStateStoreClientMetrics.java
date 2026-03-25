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

package org.apache.hadoop.yarn.server.federation.store.metrics;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableGaugeInt;
import org.apache.hadoop.metrics2.lib.MutableQuantiles;
import org.apache.hadoop.metrics2.lib.MutableRate;
import org.apache.hadoop.yarn.server.federation.store.FederationStateStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * YARN联邦状态存储客户端性能指标采集类，负责收集记录FederationStateStore各个接口的调用性能与运行状态。
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
@Metrics(about = "Performance and usage metrics for Federation StateStore",
         context = "fedr")
public final class FederationStateStoreClientMetrics implements MetricsSource {
  public static final Logger LOG =
      LoggerFactory.getLogger(FederationStateStoreClientMetrics.class);

  // 指标注册中心，统一管理当前类的所有指标
  private static final MetricsRegistry REGISTRY =
      new MetricsRegistry("FederationStateStoreClientMetrics");
  // 缓存FederationStateStore接口的所有方法，用于预初始化指标
  private final static Method[] STATESTORE_API_METHODS =
      FederationStateStore.class.getMethods();

  // 方法名 -> 失败调用计数器映射表
  private static final Map<String, MutableCounterLong> API_TO_FAILED_CALLS =
      new HashMap<String, MutableCounterLong>();
  // 方法名 -> 成功调用指标映射表，包含调用次数与延迟统计
  private static final Map<String, MutableRate> API_TO_SUCCESSFUL_CALLS =
      new HashMap<String, MutableRate>();

  // 方法名 -> 分位数延迟指标映射表，用于统计不同分位的延迟
  private static final Map<String, MutableQuantiles> API_TO_QUANTILE_METRICS =
      new HashMap<String, MutableQuantiles>();

  // 未知方法调用的错误日志模板
  private static final String UNKOWN_FAIL_ERROR_MSG =
      "Not recording failed call for unknown FederationStateStore method {}";
  private static final String UNKNOWN_SUCCESS_ERROR_MSG =
      "Not recording successful call for unknown "
          + "FederationStateStore method {}";

  // 聚合指标，全局统计，无需按方法查找
  @Metric("Total number of successful calls and latency(ms)")
  private static MutableRate totalSucceededCalls;

  @Metric("Total number of failed StateStore calls")
  private static MutableCounterLong totalFailedCalls;

  @Metric("Total number of Connections")
  private static MutableGaugeInt totalConnections;

  // 单例实例，必须在静态成员初始化后创建，避免构造器空指针异常
  private static final FederationStateStoreClientMetrics S_INSTANCE =
      DefaultMetricsSystem.instance()
          .register(new FederationStateStoreClientMetrics());

  /**
   * 获取单例实例。
   * @return 指标采集单例
   */
  synchronized public static FederationStateStoreClientMetrics getInstance() {
    return S_INSTANCE;
  }

  private FederationStateStoreClientMetrics() {
    // 为每个接口方法预创建指标，存入映射表
    for (Method m : STATESTORE_API_METHODS) {
      String methodName = m.getName();
      LOG.debug("Registering Federation StateStore Client metrics for {}",
          methodName);

      // 注册方法失败调用计数器，不统计延迟
      API_TO_FAILED_CALLS.put(methodName,
          REGISTRY.newCounter(methodName + "_numFailedCalls",
              "# failed calls to " + methodName, 0L));

      // 注册方法成功调用指标，同时统计调用次数和平均延迟
      API_TO_SUCCESSFUL_CALLS.put(methodName,
          REGISTRY.newRate(methodName + "_successfulCalls",
              "# successful calls and latency(ms) for" + methodName));

      // 注册分位数延迟指标，每10秒重新采样一次
      API_TO_QUANTILE_METRICS.put(methodName,
          REGISTRY.newQuantiles(methodName + "Latency",
              "Quantile latency (ms) for " + methodName, "ops", "latency", 10));
    }
  }

  /**
   * 记录一次联邦状态存储调用失败。通过调用栈自动获取当前方法名。
   */
  public static void failedStateStoreCall() {
    // 从调用栈获取触发方法名，[2]对应当前调用者
    String methodName =
        Thread.currentThread().getStackTrace()[2].getMethodName();
    MutableCounterLong methodMetric = API_TO_FAILED_CALLS.get(methodName);
    if (methodMetric == null) {
      LOG.error(UNKOWN_FAIL_ERROR_MSG, methodName);
      return;
    }

    // 全局和方法维度分别递增失败计数
    totalFailedCalls.incr();
    methodMetric.incr();
  }

  /**
   * 记录一次联邦状态存储调用成功，统计调用延迟。通过调用栈自动获取当前方法名。
   * @param duration 调用耗时(毫秒)
   */
  public static void succeededStateStoreCall(long duration) {
    String methodName =
        Thread.currentThread().getStackTrace()[2].getMethodName();
    MutableRate methodMetric = API_TO_SUCCESSFUL_CALLS.get(methodName);
    MutableQuantiles methodQuantileMetric =
        API_TO_QUANTILE_METRICS.get(methodName);
    if (methodMetric == null || methodQuantileMetric == null) {
      LOG.error(UNKNOWN_SUCCESS_ERROR_MSG, methodName);
      return;
    }

    // 全局和方法维度分别添加成功调用与延迟数据
    totalSucceededCalls.add(duration);
    methodMetric.add(duration);
    methodQuantileMetric.add(duration);
  }

  /**
   * 连接数递增。
   */
  public static void incrConnections() {
    totalConnections.incr();
  }

  /**
   * 连接数递减。
   */
  public static void decrConnections() {
    totalConnections.decr();
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    // 生成指标快照，供metrics系统采集
    REGISTRY.snapshot(collector.addRecord(REGISTRY.info()), all);
  }

  /**
   * 获取指定方法的失败调用次数，供单元测试使用。
   * @param methodName 方法名
   * @return 失败调用次数
   */
  @VisibleForTesting
  static long getNumFailedCallsForMethod(String methodName) {
    return API_TO_FAILED_CALLS.get(methodName).value();
  }

  /**
   * 获取指定方法的成功调用次数，供单元测试使用。
   * @param methodName 方法名
   * @return 成功调用次数
   */
  @VisibleForTesting
  static long getNumSucceessfulCallsForMethod(String methodName) {
    return API_TO_SUCCESSFUL_CALLS.get(methodName).lastStat().numSamples();
  }

  /**
   * 获取指定方法成功调用的平均延迟，供单元测试使用。
   * @param methodName 方法名
   * @return 平均延迟(毫秒)
   */
  @VisibleForTesting
  static double getLatencySucceessfulCallsForMethod(String methodName) {
    return API_TO_SUCCESSFUL_CALLS.get(methodName).lastStat().mean();
  }

  /**
   * 获取全局总失败调用次数，供单元测试使用。
   * @return 总失败调用次数
   */
  @VisibleForTesting
  static long getNumFailedCalls() {
    return totalFailedCalls.value();
  }

  /**
   * 获取全局总成功调用次数，供单元测试使用。
   * @return 总成功调用次数
   */
  @VisibleForTesting
  static long getNumSucceededCalls() {
    return totalSucceededCalls.lastStat().numSamples();
  }

  /**
   * 获取全局成功调用平均延迟，供单元测试使用。
   * @return 平均延迟(毫秒)
   */
  @VisibleForTesting
  static double getLatencySucceededCalls() {
    return totalSucceededCalls.lastStat().mean();
  }

  /**
   * 获取当前总连接数，供单元测试使用。
   * @return 总连接数
   */
  @VisibleForTesting
  public static int getNumConnections() {
    return totalConnections.value();
  }

}