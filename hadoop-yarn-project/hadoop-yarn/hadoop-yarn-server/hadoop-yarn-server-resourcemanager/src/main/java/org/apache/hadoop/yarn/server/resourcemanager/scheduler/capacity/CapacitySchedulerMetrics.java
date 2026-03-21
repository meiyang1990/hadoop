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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableQuantiles;
import org.apache.hadoop.metrics2.lib.MutableRate;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.hadoop.metrics2.lib.Interns.info;

/**
 * 容量调度器的指标统计类，收集调度过程中的各类性能指标。
 */
@InterfaceAudience.Private
@Metrics(context="yarn")
public class CapacitySchedulerMetrics {

  // 标记指标系统是否已初始化
  private static AtomicBoolean isInitialized = new AtomicBoolean(false);

  private static final MetricsInfo RECORD_INFO =
      info("CapacitySchedulerMetrics",
          "Metrics for the Yarn Capacity Scheduler");

  // 容器分配操作指标，统计次数和延迟
  @Metric("Scheduler allocate containers") MutableRate allocate;
  // 容器提交成功指标，统计次数和延迟
  @Metric("Scheduler commit success") MutableRate commitSuccess;
  // 容器提交失败指标，统计次数和延迟
  @Metric("Scheduler commit failure") MutableRate commitFailure;
  // 节点更新操作指标，统计次数和延迟
  @Metric("Scheduler node update") MutableRate nodeUpdate;
  // 节点心跳间隔分位数统计
  @Metric("Scheduler node heartbeat interval") MutableQuantiles
      schedulerNodeHBInterval;

  // 容量调度器指标单例实例
  private static volatile CapacitySchedulerMetrics INSTANCE = null;
  // 指标注册表
  private static MetricsRegistry registry;

  /**
   * 获取容量调度器指标单例，懒汉式双检锁初始化。
   * @return 容量调度器指标实例
   */
  public static CapacitySchedulerMetrics getMetrics() {
    if(!isInitialized.get()){
      synchronized (CapacitySchedulerMetrics.class) {
        if(INSTANCE == null){
          INSTANCE = new CapacitySchedulerMetrics();
          registerMetrics();
          isInitialized.set(true);
        }
      }
    }
    return INSTANCE;
  }

  /**
   * 将容量调度器指标注册到Hadoop默认指标系统。
   */
  private static void registerMetrics() {
    registry = new MetricsRegistry(RECORD_INFO);
    registry.tag(RECORD_INFO, "ResourceManager");
    MetricsSystem ms = DefaultMetricsSystem.instance();
    if (ms != null) {
      ms.register("CapacitySchedulerMetrics",
          "Metrics for the Yarn Capacity Scheduler", INSTANCE);
    }
  }

  /**
   * 销毁指标实例，从指标系统中注销，供测试使用。
   */
  @VisibleForTesting
  public synchronized static void destroy() {
    isInitialized.set(false);
    INSTANCE = null;
    MetricsSystem ms = DefaultMetricsSystem.instance();
    if (ms != null) {
      ms.unregisterSource("CapacitySchedulerMetrics");
    }
  }

  /**
   * 添加一次容器分配延迟记录。
   * @param latency 分配操作耗时
   */
  public void addAllocate(long latency) {
    this.allocate.add(latency);
  }

  /**
   * 添加一次容器提交成功延迟记录。
   * @param latency 提交操作耗时
   */
  public void addCommitSuccess(long latency) {
    this.commitSuccess.add(latency);
  }

  /**
   * 添加一次容器提交失败延迟记录。
   * @param latency 提交操作耗时
   */
  public void addCommitFailure(long latency) {
    this.commitFailure.add(latency);
  }

  /**
   * 添加一次节点更新延迟记录。
   * @param latency 更新操作耗时
   */
  public void addNodeUpdate(long latency) {
    this.nodeUpdate.add(latency);
  }

  /**
   * 获取节点更新操作总次数，供测试使用。
   * @return 节点更新总次数
   */
  @VisibleForTesting
  public long getNumOfNodeUpdate() {
    return this.nodeUpdate.lastStat().numSamples();
  }

  /**
   * 获取容器分配操作总次数，供测试使用。
   * @return 容器分配总次数
   */
  @VisibleForTesting
  public long getNumOfAllocates() {
    return this.allocate.lastStat().numSamples();
  }

  /**
   * 获取容器提交成功总次数，供测试使用。
   * @return 提交成功总次数
   */
  @VisibleForTesting
  public long getNumOfCommitSuccess() {
    return this.commitSuccess.lastStat().numSamples();
  }

  /**
   * 添加一次节点心跳间隔记录。
   * @param heartbeatInterval 心跳间隔值
   */
  public void addSchedulerNodeHBInterval(long heartbeatInterval) {
    schedulerNodeHBInterval.add(heartbeatInterval);
  }

  /**
   * 获取节点心跳间隔记录总条数，供测试使用。
   * @return 心跳间隔记录总条数
   */
  @VisibleForTesting
  public long getNumOfSchedulerNodeHBInterval() {
    return this.schedulerNodeHBInterval.getEstimator().getCount();
  }
}