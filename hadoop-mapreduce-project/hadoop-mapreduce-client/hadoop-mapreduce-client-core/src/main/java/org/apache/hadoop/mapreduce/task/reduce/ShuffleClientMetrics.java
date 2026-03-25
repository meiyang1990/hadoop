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
package org.apache.hadoop.mapreduce.task.reduce;

import org.apache.hadoop.classification.VisibleForTesting;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.TaskAttemptID;

import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;

import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableCounterInt;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableGaugeInt;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;


import java.util.concurrent.ThreadLocalRandom;

import static org.apache.hadoop.metrics2.lib.Interns.info;

/**
 * Shuffle阶段客户端的指标收集类，用于统计Reduce任务拉取Map输出数据过程中的各项运行指标
 */
@SuppressWarnings("checkstyle:finalclass")
@InterfaceAudience.LimitedPrivate({"MapReduce"})
@InterfaceStability.Unstable
@Metrics(name="ShuffleClientMetrics", context="mapred")
public class ShuffleClientMetrics {

  /**
   * 指标记录元信息，描述Shuffle客户端指标
   */
  private static final MetricsInfo RECORD_INFO =
      info("ShuffleClientMetrics", "Metrics for Shuffle client");

  /**
   * 拉取失败的次数计数器
   */
  @Metric
  private MutableCounterInt numFailedFetches;
  /**
   * 拉取成功的次数计数器
   */
  @Metric
  private MutableCounterInt numSuccessFetches;
  /**
   * 拉取总字节数计数器
   */
  @Metric
  private MutableCounterLong numBytes;
  /**
   * 当前忙等待的拉取线程数指标
   */
  @Metric
  private MutableGaugeInt numThreadsBusy;

  /**
   * 指标注册表，用于管理指标和标签
   */
  private final MetricsRegistry metricsRegistry =
      new MetricsRegistry(RECORD_INFO);

  /**
   * 私有构造方法，通过create方法创建实例
   */
  private ShuffleClientMetrics() {
  }

  /**
   * 创建并注册Shuffle客户端指标实例到Metrics系统
   * @param reduceId Reduce任务尝试ID
   * @param jobConf 作业配置对象
   * @return 注册后的ShuffleClientMetrics实例
   */
  public static ShuffleClientMetrics create(
      TaskAttemptID reduceId,
      JobConf jobConf) {
    // 初始化默认指标系统，关联JobTracker上下文
    MetricsSystem ms = DefaultMetricsSystem.initialize("JobTracker");

    ShuffleClientMetrics shuffleClientMetrics = new ShuffleClientMetrics();
    // 添加作业、任务相关标签
    shuffleClientMetrics.addTags(reduceId, jobConf);

    // 注册到指标系统，使用随机数生成唯一名称
    return ms.register("ShuffleClientMetrics-" +
        ThreadLocalRandom.current().nextInt(), null,
            shuffleClientMetrics);
  }

  /**
   * 累加已拉取的字节数
   * @param bytes 本次拉取的字节数
   */
  public void inputBytes(long bytes) {
    numBytes.incr(bytes);
  }

  /**
   * 记录一次拉取失败
   */
  public void failedFetch() {
    numFailedFetches.incr();
  }

  /**
   * 记录一次拉取成功
   */
  public void successFetch() {
    numSuccessFetches.incr();
  }

  /**
   * 增加一个忙状态拉取线程计数
   */
  public void threadBusy() {
    numThreadsBusy.incr();
  }

  /**
   * 减少一个忙状态拉取线程计数
   */
  public void threadFree() {
    numThreadsBusy.decr();
  }

  /**
   * 添加作业和任务相关的标签，用于指标分组查询
   * @param reduceId Reduce任务尝试ID
   * @param jobConf 作业配置对象
   */
  private void addTags(TaskAttemptID reduceId, JobConf jobConf) {
    metricsRegistry.tag("user", "", jobConf.getUser())
        .tag("jobName", "", jobConf.getJobName())
        .tag("jobId", "", reduceId.getJobID().toString())
        .tag("taskId", "", reduceId.toString());
  }

  /**
   * 获取指标注册表，仅用于测试
   * @return 指标注册表实例
   */
  @VisibleForTesting
  MetricsRegistry getMetricsRegistry() {
    return metricsRegistry;
  }
}