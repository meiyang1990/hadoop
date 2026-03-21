// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.resourcemanager.recovery;

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
 * ZKRMStateStore ZooKeeper基于RM状态存储操作耗时性能指标收集类，
 * 采用单例模式全局维护指标。
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
@Metrics(context="ZKRMStateStore-op-durations")
public final class ZKRMStateStoreOpDurations implements MetricsSource {

  @Metric("Duration for a load state call")
  MutableRate loadStateCall;

  @Metric("Duration for a store application state call")
  MutableRate storeApplicationStateCall;

  @Metric("Duration for a update application state call")
  MutableRate updateApplicationStateCall;

  @Metric("Duration to handle a remove application state call")
  MutableRate removeApplicationStateCall;

  // 指标记录信息定义
  protected static final MetricsInfo RECORD_INFO =
      info("ZKRMStateStoreOpDurations", "Durations of ZKRMStateStore calls");

  private final MetricsRegistry registry;

  // 单例实例
  private static final ZKRMStateStoreOpDurations INSTANCE
      = new ZKRMStateStoreOpDurations();

  /**
   * 获取单例实例。
   * @return 单例指标对象
   */
  public static ZKRMStateStoreOpDurations getInstance() {
    return INSTANCE;
  }

  private ZKRMStateStoreOpDurations() {
    registry = new MetricsRegistry(RECORD_INFO);
    registry.tag(RECORD_INFO, "ZKRMStateStoreOpDurations");

    // 获取默认指标系统实例
    MetricsSystem ms = DefaultMetricsSystem.instance();
    if (ms != null) {
      // 将当前指标注册到指标系统
      ms.register(RECORD_INFO.name(), RECORD_INFO.description(), this);
    }
  }

  @Override
  public synchronized void getMetrics(MetricsCollector collector, boolean all) {
    // 生成指标快照并输出给收集器
    registry.snapshot(collector.addRecord(registry.info()), all);
  }

  /**
   * 添加加载状态操作的耗时样本。
   * @param value 耗时值
   */
  public void addLoadStateCallDuration(long value) {
    loadStateCall.add(value);
  }

  /**
   * 添加存储应用状态操作的耗时样本。
   * @param value 耗时值
   */
  public void addStoreApplicationStateCallDuration(long value) {
    storeApplicationStateCall.add(value);
  }

  /**
   * 添加更新应用状态操作的耗时样本。
   * @param value 耗时值
   */
  public void addUpdateApplicationStateCallDuration(long value) {
    updateApplicationStateCall.add(value);
  }

  /**
   * 添加删除应用状态操作的耗时样本。
   * @param value 耗时值
   */
  public void addRemoveApplicationStateCallDuration(long value) {
    removeApplicationStateCall.add(value);
  }
}