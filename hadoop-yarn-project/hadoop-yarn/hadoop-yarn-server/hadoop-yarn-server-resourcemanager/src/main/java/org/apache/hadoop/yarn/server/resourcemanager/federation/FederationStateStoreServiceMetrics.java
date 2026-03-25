// 这个文件已经全部加上中文注释
/*
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
package org.apache.hadoop.yarn.server.resourcemanager.federation;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableQuantiles;
import org.apache.hadoop.metrics2.lib.MutableRate;
import org.apache.hadoop.yarn.server.federation.store.FederationStateStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.metrics2.lib.Interns.info;

/**
 * YARN联邦状态存储服务的指标收集类，负责统计记录FederationStateStore各个接口调用的成功率、延迟等指标。
 */
@Metrics(about = "Metrics for FederationStateStoreService", context = "fedr")
public final class FederationStateStoreServiceMetrics {

  public static final Logger LOG =
      LoggerFactory.getLogger(FederationStateStoreServiceMetrics.class);

  private static final MetricsInfo RECORD_INFO =
      info("FederationStateStoreServiceMetrics", "Metrics for the RM FederationStateStoreService");

  private static volatile FederationStateStoreServiceMetrics instance = null;
  private MetricsRegistry registry;

  // 缓存FederationStateStore接口的所有方法
  private final static Method[] STATESTORE_API_METHODS = FederationStateStore.class.getMethods();

  // 映射方法名到失败调用计数器
  private static final Map<String, MutableCounterLong> FAILED_CALLS = new HashMap<>();
  // 映射方法名到成功调用统计（包含次数和延迟）
  private static final Map<String, MutableRate> SUCCESSFUL_CALLS = new HashMap<>();
  // 映射方法名到分位数延迟统计
  private static final Map<String, MutableQuantiles> QUANTILE_METRICS = new HashMap<>();

  // 不存在于FederationStateStore接口的方法调用错误日志模板
  private static final String UNKOWN_FAIL_ERROR_MSG =
      "Not recording failed call for unknown FederationStateStore method {}";
  private static final String UNKNOWN_SUCCESS_ERROR_MSG =
      "Not recording successful call for unknown FederationStateStore method {}";

  /**
   * 获取FederationStateStoreServiceMetrics单例实例，惰性初始化。
   *
   * @return 单例实例
   */
  public static FederationStateStoreServiceMetrics getMetrics() {
    synchronized (FederationStateStoreServiceMetrics.class) {
      if (instance == null) {
        instance = DefaultMetricsSystem.instance()
            .register(new FederationStateStoreServiceMetrics());
      }
    }
    return instance;
  }

  /**
   * 私有构造函数，为每个FederationStateStore接口方法初始化对应的指标对象。
   */
  private FederationStateStoreServiceMetrics() {
    registry = new MetricsRegistry(RECORD_INFO);
    registry.tag(RECORD_INFO, "FederationStateStoreServiceMetrics");

    // 为每个接口方法创建指标并存入对应映射表
    for (Method m : STATESTORE_API_METHODS) {
      String methodName = m.getName();
      LOG.debug("Registering Federation StateStore Service metrics for {}", methodName);

      // 创建失败调用次数计数器，不记录延迟
      FAILED_CALLS.put(methodName, registry.newCounter(methodName + "NumFailedCalls",
          "# failed calls to " + methodName, 0L));

      // 创建成功调用统计，记录调用次数和平均延迟
      SUCCESSFUL_CALLS.put(methodName, registry.newRate(methodName + "SuccessfulCalls",
          "# successful calls and latency(ms) for" + methodName));

      // 创建分位数延迟统计，每10秒重新采样一次
      QUANTILE_METRICS.put(methodName, registry.newQuantiles(methodName + "Latency",
          "Quantile latency (ms) for " + methodName, "ops", "latency", 10));
    }
  }

  // 聚合指标，全局共享，无需每次调用查找
  @Metric("Total number of successful calls and latency(ms)")
  private static MutableRate totalSucceededCalls;

  @Metric("Total number of failed StateStore calls")
  private static MutableCounterLong totalFailedCalls;

  /**
   * 记录一次失败的状态存储服务调用，自动从调用栈获取方法名。
   */
  public static void failedStateStoreServiceCall() {
    String methodName = Thread.currentThread().getStackTrace()[2].getMethodName();
    MutableCounterLong methodMetric = FAILED_CALLS.get(methodName);

    if (methodMetric == null) {
      LOG.error(UNKOWN_FAIL_ERROR_MSG, methodName);
      return;
    }

    totalFailedCalls.incr();
    methodMetric.incr();
  }

  /**
   * 记录一次失败的状态存储服务调用，使用传入的方法名。
   * @param methodName 方法名
   */
  public static void failedStateStoreServiceCall(String methodName) {
    MutableCounterLong methodMetric = FAILED_CALLS.get(methodName);
    if (methodMetric == null) {
      LOG.error(UNKOWN_FAIL_ERROR_MSG, methodName);
      return;
    }
    totalFailedCalls.incr();
    methodMetric.incr();
  }

  /**
   * 记录一次成功的状态存储服务调用，自动从调用栈获取方法名。
   * @param duration 调用耗时（毫秒）
   */
  public static void succeededStateStoreServiceCall(long duration) {
    StackTraceElement[] stackTraceElements = Thread.currentThread().getStackTrace();
    if (ArrayUtils.isNotEmpty(stackTraceElements) && stackTraceElements.length > 2) {
      String methodName = Thread.currentThread().getStackTrace()[2].getMethodName();
      if(SUCCESSFUL_CALLS.containsKey(methodName)) {
        succeededStateStoreServiceCall(methodName, duration);
      } else {
        LOG.error(UNKNOWN_SUCCESS_ERROR_MSG, methodName);
      }
    } else {
      LOG.error("stackTraceElements is empty or length < 2.");
    }
  }

  /**
   * 记录一次成功的状态存储服务调用，使用传入的方法名和耗时。
   * @param methodName 方法名
   * @param duration 调用耗时（毫秒）
   */
  public static void succeededStateStoreServiceCall(String methodName, long duration) {
    if (SUCCESSFUL_CALLS.containsKey(methodName)) {
      MutableRate methodMetric = SUCCESSFUL_CALLS.get(methodName);
      MutableQuantiles methodQuantileMetric = QUANTILE_METRICS.get(methodName);
      if (methodMetric == null || methodQuantileMetric == null) {
        LOG.error(UNKNOWN_SUCCESS_ERROR_MSG, methodName);
        return;
      }
      totalSucceededCalls.add(duration);
      methodMetric.add(duration);
      methodQuantileMetric.add(duration);
    }
  }

  // 以下为单元测试使用的获取方法，仅用于测试验证指标数据

  @VisibleForTesting
  public static long getNumFailedCallsForMethod(String methodName) {
    return FAILED_CALLS.get(methodName).value();
  }

  @VisibleForTesting
  public static long getNumSucceessfulCallsForMethod(String methodName) {
    return SUCCESSFUL_CALLS.get(methodName).lastStat().numSamples();
  }

  @VisibleForTesting
  public static double getLatencySucceessfulCallsForMethod(String methodName) {
    return SUCCESSFUL_CALLS.get(methodName).lastStat().mean();
  }

  @VisibleForTesting
  public static long getNumFailedCalls() {
    return totalFailedCalls.value();
  }

  @VisibleForTesting
  public static long getNumSucceededCalls() {
    return totalSucceededCalls.lastStat().numSamples();
  }

  @VisibleForTesting
  public static double getLatencySucceededCalls() {
    return totalSucceededCalls.lastStat().mean();
  }
}