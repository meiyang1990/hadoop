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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;

import static org.apache.hadoop.metrics2.lib.Interns.info;
import org.apache.hadoop.metrics2.lib.MutableRate;

/**
 * 公平调度器操作耗时指标收集类，采用单例模式实现，
 * 用于统计公平调度器各类核心操作的执行耗时，供监控系统采集。
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
@Metrics(context="fairscheduler-op-durations")
public class FSOpDurations implements MetricsSource {

  @Deprecated
  @Metric("Duration for a continuous scheduling run")
  MutableRate continuousSchedulingRun;

  @Metric("Duration to handle a node update")
  MutableRate nodeUpdateCall;

  @Metric("Duration for a update thread run")
  MutableRate updateThreadRun;

  private static final MetricsInfo RECORD_INFO =
      info("FSOpDurations", "Durations of FairScheduler calls or thread-runs");

  private final MetricsRegistry registry;

  // 是否开启扩展指标统计
  private boolean isExtended = false;

  private static final FSOpDurations INSTANCE = new FSOpDurations();

  /**
   * 获取FSOpDurations单例实例，并设置是否开启扩展指标统计。
   * @param isExtended 是否开启扩展指标统计
   * @return FSOpDurations单例实例
   */
  public static FSOpDurations getInstance(boolean isExtended) {
    INSTANCE.setExtended(isExtended);
    return INSTANCE;
  }

  /**
   * 私有构造函数，完成指标注册表初始化并注册到默认指标系统。
   */
  private FSOpDurations() {
    registry = new MetricsRegistry(RECORD_INFO);
    registry.tag(RECORD_INFO, "FSOpDurations");

    MetricsSystem ms = DefaultMetricsSystem.instance();
    if (ms != null) {
      ms.register(RECORD_INFO.name(), RECORD_INFO.description(), this);
    }
  }

  /**
   * 同步设置所有指标的扩展统计开关。
   * @param isExtended 是否开启扩展统计
   */
  private synchronized void setExtended(boolean isExtended) {
    if (isExtended == INSTANCE.isExtended)
      return;

    continuousSchedulingRun.setExtended(isExtended);
    nodeUpdateCall.setExtended(isExtended);
    updateThreadRun.setExtended(isExtended);

    INSTANCE.isExtended = isExtended;
  }

  @Override
  public synchronized void getMetrics(MetricsCollector collector, boolean all) {
    registry.snapshot(collector.addRecord(registry.info()), all);
  }

  @Deprecated
  public void addContinuousSchedulingRunDuration(long value) {
    continuousSchedulingRun.add(value);
  }

  /**
   * 添加节点更新操作的耗时记录。
   * @param value 本次操作耗时
   */
  public void addNodeUpdateDuration(long value) {
    nodeUpdateCall.add(value);
  }

  /**
   * 添加更新线程一次运行的耗时记录。
   * @param value 本次运行耗时
   */
  public void addUpdateThreadRunDuration(long value) {
    updateThreadRun.add(value);
  }

  @VisibleForTesting
  public boolean hasUpdateThreadRunChanged() {
    return updateThreadRun.changed();
  }
}