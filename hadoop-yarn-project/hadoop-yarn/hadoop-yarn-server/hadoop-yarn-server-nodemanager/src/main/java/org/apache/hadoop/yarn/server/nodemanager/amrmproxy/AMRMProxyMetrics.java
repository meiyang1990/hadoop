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

package org.apache.hadoop.yarn.server.nodemanager.amrmproxy;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.apache.hadoop.metrics2.lib.MutableQuantiles;
import org.apache.hadoop.metrics2.lib.MutableRate;

import static org.apache.hadoop.metrics2.lib.Interns.info;

/**
 * AMRMProxy代理组件的指标采集类，负责统计AMRMProxy处理各类请求的成功/失败次数和延迟情况。
 */
@Metrics(about = "Metrics for AMRMProxy", context = "fedr")
public final class AMRMProxyMetrics {

  private static final MetricsInfo RECORD_INFO =
      info("AMRMProxyMetrics", "Metrics for the AMRMProxy");
  // 失败的应用启动请求数量
  @Metric("# of failed applications start requests")
  private MutableGaugeLong failedAppStartRequests;
  // 失败的AM注册请求数量
  @Metric("# of failed register AM requests")
  private MutableGaugeLong failedRegisterAMRequests;
  // 失败的AM结束请求数量
  @Metric("# of failed finish AM requests")
  private MutableGaugeLong failedFinishAMRequests;
  // 失败的资源分配请求数量
  @Metric("# of failed allocate requests ")
  private MutableGaugeLong failedAllocateRequests;
  // 失败的应用恢复数量
  @Metric("# of failed application recoveries")
  private MutableGaugeLong failedAppRecoveryCount;
  // 失败的应用停止请求数量
  @Metric("# of failed application stop")
  private MutableGaugeLong failedAppStopRequests;
  // 失败的AMRM令牌更新请求数量
  @Metric("# of failed update token")
  private MutableGaugeLong failedUpdateAMRMTokenRequests;
  // 总资源分配请求数量
  @Metric("# all allocate requests count")
  private MutableGaugeLong allocateCount;
  // 总请求数量
  @Metric("# all requests count")
  private MutableGaugeLong requestCount;


  // 成功应用启动请求总延迟统计
  @Metric("Application start request latency(ms)")
  private MutableRate totalSucceededAppStartRequests;
  // 成功AM注册请求总延迟统计
  @Metric("Register application master latency(ms)")
  private MutableRate totalSucceededRegisterAMRequests;
  // 成功AM结束请求总延迟统计
  @Metric("Finish application master latency(ms)")
  private MutableRate totalSucceededFinishAMRequests;
  // 成功资源分配请求总延迟统计
  @Metric("Allocate latency(ms)")
  private MutableRate totalSucceededAllocateRequests;
  // 成功应用停止请求总延迟统计
  @Metric("Application stop request latency(ms)")
  private MutableRate totalSucceededAppStopRequests;
  // 成功应用恢复请求总延迟统计
  @Metric("Recover latency(ms)")
  private MutableRate totalSucceededRecoverRequests;
  // 成功AMRM令牌更新请求总延迟统计
  @Metric("UpdateAMRMToken latency(ms)")
  private MutableRate totalSucceededUpdateAMRMTokenRequests;

  // 应用启动延迟分位数统计，用于计算SLA指标（95%、99%分位等）
  private MutableQuantiles applicationStartLatency;
  // AM注册延迟分位数统计
  private MutableQuantiles registerAMLatency;
  // AM结束延迟分位数统计
  private MutableQuantiles finishAMLatency;
  // 资源分配延迟分位数统计
  private MutableQuantiles allocateLatency;
  // 应用恢复延迟分位数统计
  private MutableQuantiles recoverLatency;
  // 应用停止延迟分位数统计
  private MutableQuantiles applicationStopLatency;
  // AMRM令牌更新延迟分位数统计
  private MutableQuantiles updateAMRMTokenLatency;

  // AMRMProxyMetrics单例实例
  private static volatile AMRMProxyMetrics instance = null;
  // 指标注册表
  private MetricsRegistry registry;

  /**
   * 私有构造函数，初始化各类分位数统计指标并注册到指标注册表。
   */
  private AMRMProxyMetrics() {
    registry = new MetricsRegistry(RECORD_INFO);
    registry.tag(RECORD_INFO, "AMRMProxy");

    // 注册应用启动延迟分位数统计，每10秒滚动计算一次
    applicationStartLatency = registry
        .newQuantiles("applicationStartLatency", "latency of app start", "ops",
            "latency", 10);
    // 注册AM注册延迟分位数统计，每10秒滚动计算一次
    registerAMLatency = registry
        .newQuantiles("registerAMLatency", "latency of register AM", "ops",
            "latency", 10);
    // 注册AM结束延迟分位数统计，每10秒滚动计算一次
    finishAMLatency = registry
        .newQuantiles("finishAMLatency", "latency of finish AM", "ops",
            "latency", 10);
    // 注册资源分配延迟分位数统计，每10秒滚动计算一次
    allocateLatency = registry
        .newQuantiles("allocateLatency", "latency of allocate", "ops",
            "latency", 10);
    // 注册应用停止延迟分位数统计，每10秒滚动计算一次
    applicationStopLatency = registry
        .newQuantiles("applicationStopLatency", "latency of app stop", "ops",
            "latency", 10);
    // 注册应用恢复延迟分位数统计，每10秒滚动计算一次
    recoverLatency = registry
        .newQuantiles("recoverLatency", "latency of recover", "ops",
            "latency", 10);
    // 注册AMRM令牌更新延迟分位数统计，每10秒滚动计算一次
    updateAMRMTokenLatency = registry
        .newQuantiles("updateAMRMTokenLatency", "latency of update amrm token", "ops",
            "latency", 10);
  }

  /**
   * 获取AMRMProxyMetrics单例实例，如果未初始化则完成初始化并注册到默认指标系统。
   *
   * @return AMRMProxyMetrics单例实例
   */
  public static AMRMProxyMetrics getMetrics() {
    synchronized (AMRMProxyMetrics.class) {
      if (instance == null) {
        instance = DefaultMetricsSystem.instance()
            .register("AMRMProxyMetrics", "Metrics for the Yarn AMRMProxy",
                new AMRMProxyMetrics());
      }
    }
    return instance;
  }

  @VisibleForTesting
  long getNumSucceededAppStartRequests() {
    return totalSucceededAppStartRequests.lastStat().numSamples();
  }

  @VisibleForTesting
  double getLatencySucceededAppStartRequests() {
    return totalSucceededAppStartRequests.lastStat().mean();
  }

  /**
   * 记录成功的应用启动请求，添加延迟到统计中。
   * @param duration 请求处理耗时（毫秒）
   */
  public void succeededAppStartRequests(long duration) {
    totalSucceededAppStartRequests.add(duration);
    applicationStartLatency.add(duration);
  }

  @VisibleForTesting
  long getNumSucceededRegisterAMRequests() {
    return totalSucceededRegisterAMRequests.lastStat().numSamples();
  }

  @VisibleForTesting
  double getLatencySucceededRegisterAMRequests() {
    return totalSucceededRegisterAMRequests.lastStat().mean();
  }

  /**
   * 记录成功的AM注册请求，添加延迟到统计中。
   * @param duration 请求处理耗时（毫秒）
   */
  public void succeededRegisterAMRequests(long duration) {
    totalSucceededRegisterAMRequests.add(duration);
    registerAMLatency.add(duration);
  }

  @VisibleForTesting
  long getNumSucceededFinishAMRequests() {
    return totalSucceededFinishAMRequests.lastStat().numSamples();
  }

  @VisibleForTesting
  double getLatencySucceededFinishAMRequests() {
    return totalSucceededFinishAMRequests.lastStat().mean();
  }

  /**
   * 记录成功的AM结束请求，添加延迟到统计中。
   * @param duration 请求处理耗时（毫秒）
   */
  public void succeededFinishAMRequests(long duration) {
    totalSucceededFinishAMRequests.add(duration);
    finishAMLatency.add(duration);
  }

  @VisibleForTesting
  long getNumSucceededAllocateRequests() {
    return totalSucceededAllocateRequests.lastStat().numSamples();
  }

  @VisibleForTesting
  long getNumSucceededAppStopRequests() {
    return totalSucceededAppStopRequests.lastStat().numSamples();
  }

  @VisibleForTesting
  long getNumSucceededRecoverRequests() {
    return totalSucceededRecoverRequests.lastStat().numSamples();
  }

  @VisibleForTesting
  long getNumSucceededUpdateAMRMTokenRequests() {
    return totalSucceededUpdateAMRMTokenRequests.lastStat().numSamples();
  }


  @VisibleForTesting
  double getLatencySucceededAllocateRequests() {
    return totalSucceededAllocateRequests.lastStat().mean();
  }

  @VisibleForTesting
  double getLatencySucceededAppStopRequests() {
    return totalSucceededAppStopRequests.lastStat().mean();
  }

  @VisibleForTesting
  double getLatencySucceededRecoverRequests() {
    return totalSucceededRecoverRequests.lastStat().mean();
  }

  /**
   * 记录成功的资源分配请求，添加延迟到统计中。
   * @param duration 请求处理耗时（毫秒）
   */
  public void succeededAllocateRequests(long duration) {
    totalSucceededAllocateRequests.add(duration);
    allocateLatency.add(duration);
  }

  /**
   * 记录成功的应用停止请求，添加延迟到统计中。
   * @param duration 请求处理耗时（毫秒）
   */
  public void succeededAppStopRequests(long duration) {
    totalSucceededAppStopRequests.add(duration);
    applicationStopLatency.add(duration);
  }

  /**
   * 记录成功的应用恢复请求，添加延迟到统计中。
   * @param duration 请求处理耗时（毫秒）
   */
  public void succeededRecoverRequests(long duration) {
    totalSucceededRecoverRequests.add(duration);
    recoverLatency.add(duration);
  }

  /**
   * 记录成功的AMRM令牌更新请求，添加延迟到统计中。
   * @param duration 请求处理耗时（毫秒）
   */
  public void succeededUpdateTokenRequests(long duration) {
    totalSucceededUpdateAMRMTokenRequests.add(duration);
    updateAMRMTokenLatency.add(duration);
  }

  long getFailedAppStartRequests() {
    return failedAppStartRequests.value();
  }

  /**
   * 失败应用启动请求计数加一。
   */
  public void incrFailedAppStartRequests() {
    failedAppStartRequests.incr();
  }

  long getFailedRegisterAMRequests() {
    return failedRegisterAMRequests.value();
  }

  /**
   * 失败AM注册请求计数加一。
   */
  public void incrFailedRegisterAMRequests() {
    failedRegisterAMRequests.incr();
  }

  long getFailedFinishAMRequests() {
    return failedFinishAMRequests.value();
  }

  /**
   * 失败AM结束请求计数加一。
   */
  public void incrFailedFinishAMRequests() {
    failedFinishAMRequests.incr();
  }

  long getFailedAllocateRequests() {
    return failedAllocateRequests.value();
  }

  /**
   * 失败资源分配请求计数加一。
   */
  public void incrFailedAllocateRequests() {
    failedAllocateRequests.incr();
  }

  long getFailedAppRecoveryCount() {
    return failedAppRecoveryCount.value();
  }

  /**
   * 失败应用恢复计数加一。
   */
  public void incrFailedAppRecoveryCount() {
    failedAppRecoveryCount.incr();
  }

  long getFailedAppStopRequests() {
    return failedAppStopRequests.value();
  }

  /**
   * 失败应用停止请求计数加一。
   */
  public void incrFailedAppStopRequests() {
    failedAppStopRequests.incr();
  }

  long getFailedUpdateAMRMTokenRequests() {
    return failedUpdateAMRMTokenRequests.value();
  }

  /**
   * 失败AMRM令牌更新请求计数加一。
   */
  public void incrFailedUpdateAMRMTokenRequests() {
    failedUpdateAMRMTokenRequests.incr();
  }

  /**
   * 总资源分配请求计数加一。
   */
  public void incrAllocateCount() {
    allocateCount.incr();
  }

  /**
   * 总请求计数加一。
   */
  public void incrRequestCount() {
    requestCount.incr();
  }

  long getAllocateCount() {
    return allocateCount.value();
  }

  long getRequestCount() {
    return requestCount.value();
  }
}