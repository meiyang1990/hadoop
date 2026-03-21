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

package org.apache.hadoop.yarn.server.metrics;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableGaugeInt;
import org.apache.hadoop.metrics2.lib.MutableQuantiles;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.hadoop.metrics2.lib.Interns.info;

/**
 * 机会调度器（ Opportunistic Scheduler ）指标收集类，负责收集YARN机会调度相关的各项运行指标。
 * 机会调度允许应用利用集群空闲资源调度容器，提高集群资源利用率，此类负责统计相关调度数据。
 */
@InterfaceAudience.Private
@Metrics(context="yarn")
public class OpportunisticSchedulerMetrics {
  // CHECKSTYLE:OFF:VisibilityModifier
  // 标记指标是否已经完成初始化
  private static AtomicBoolean isInitialized = new AtomicBoolean(false);

  // 指标记录信息，用于metrics2系统注册
  private static final MetricsInfo RECORD_INFO =
      info("OpportunisticSchedulerMetrics",
          "Metrics for the Yarn Opportunistic Scheduler");

  // 单例实例对象
  private static volatile OpportunisticSchedulerMetrics INSTANCE = null;
  // metrics2指标注册表
  private static MetricsRegistry registry;

  /**
   * 获取机会调度器指标单例对象，懒加载初始化。
   * @return 机会调度器指标实例
   */
  public static OpportunisticSchedulerMetrics getMetrics() {
    if(!isInitialized.get()){
      synchronized (OpportunisticSchedulerMetrics.class) {
        if(INSTANCE == null){
          INSTANCE = new OpportunisticSchedulerMetrics();
          registerMetrics();
          isInitialized.set(true);
        }
      }
    }
    return INSTANCE;
  }

  /**
   * 重置指标单例，用于单元测试。
   */
  @VisibleForTesting
  public static void resetMetrics() {
    synchronized (OpportunisticSchedulerMetrics.class) {
      isInitialized.set(false);
      INSTANCE = null;
      MetricsSystem ms = DefaultMetricsSystem.instance();
      if (ms != null) {
        ms.unregisterSource("OpportunisticSchedulerMetrics");
      }
    }
  }

  /**
   * 向metrics2系统注册机会调度指标。
   */
  private static void registerMetrics() {
    registry = new MetricsRegistry(RECORD_INFO);
    registry.tag(RECORD_INFO, "ResourceManager");
    MetricsSystem ms = DefaultMetricsSystem.instance();
    if (ms != null) {
      ms.register("OpportunisticSchedulerMetrics",
          "Metrics for the Yarn Opportunistic Scheduler", INSTANCE);
    }
  }

  // 当前已分配的机会容器数量
  @Metric("# of allocated opportunistic containers")
  MutableGaugeInt allocatedOContainers;
  // 累计分配的机会容器总数
  @Metric("Aggregate # of allocated opportunistic containers")
  MutableCounterLong aggregateOContainersAllocated;
  // 累计释放的机会容器总数
  @Metric("Aggregate # of released opportunistic containers")
  MutableCounterLong aggregateOContainersReleased;

  // 累计分配的节点本地位机会容器总数
  @Metric("Aggregate # of allocated node-local opportunistic containers")
  MutableCounterLong aggregateNodeLocalOContainersAllocated;
  // 累计分配的机架本地位机会容器总数
  @Metric("Aggregate # of allocated rack-local opportunistic containers")
  MutableCounterLong aggregateRackLocalOContainersAllocated;
  // 累计分配的跨交换机机会容器总数
  @Metric("Aggregate # of allocated off-switch opportunistic containers")
  MutableCounterLong aggregateOffSwitchOContainersAllocated;

  // 机会容器分配延迟分位数统计
  @Metric("Aggregate latency for opportunistic container allocation")
  MutableQuantiles allocateLatencyOQuantiles;

  @VisibleForTesting
  public int getAllocatedContainers() {
    return allocatedOContainers.value();
  }

  @VisibleForTesting
  public long getAggregatedAllocatedContainers() {
    return aggregateOContainersAllocated.value();
  }

  @VisibleForTesting
  public long getAggregatedReleasedContainers() {
    return aggregateOContainersReleased.value();
  }

  @VisibleForTesting
  public long getAggregatedNodeLocalContainers() {
    return aggregateNodeLocalOContainersAllocated.value();
  }

  @VisibleForTesting
  public long getAggregatedRackLocalContainers() {
    return aggregateRackLocalOContainersAllocated.value();
  }

  @VisibleForTesting
  public long getAggregatedOffSwitchContainers() {
    return aggregateOffSwitchOContainersAllocated.value();
  }

  /**
   * 增加已分配机会容器计数。
   * @param numContainers 新增分配的容器数量
   */
  public void incrAllocatedOppContainers(int numContainers) {
    allocatedOContainers.incr(numContainers);
    aggregateOContainersAllocated.incr(numContainers);
  }

  /**
   * 增加已释放机会容器计数。
   * @param numContainers 释放的容器数量
   */
  public void incrReleasedOppContainers(int numContainers) {
    aggregateOContainersReleased.incr(numContainers);
    allocatedOContainers.decr(numContainers);
  }

  /**
   * 增加节点本地位机会容器分配计数。
   */
  public void incrNodeLocalOppContainers() {
    aggregateNodeLocalOContainersAllocated.incr();
  }

  /**
   * 增加机架本地位机会容器分配计数。
   */
  public void incrRackLocalOppContainers() {
    aggregateRackLocalOContainersAllocated.incr();
  }

  /**
   * 增加跨交换机机会容器分配计数。
   */
  public void incrOffSwitchOppContainers() {
    aggregateOffSwitchOContainersAllocated.incr();
  }

  /**
   * 添加一次机会容器分配延迟样本，用于分位数统计。
   * @param latency 本次分配延迟
   */
  public void addAllocateOLatencyEntry(long latency) {
    allocateLatencyOQuantiles.add(latency);
  }
}