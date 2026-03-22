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
package org.apache.hadoop.mapred;

import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableCounterInt;
import org.apache.hadoop.metrics2.lib.MutableGaugeInt;

import java.util.concurrent.ThreadLocalRandom;

/**
 * LocalJobRunner 本地作业运行器的指标统计类，用于统计本地运行MapReduce作业时Map和Reduce任务的运行状态指标
 */
@Metrics(name="LocalJobRunnerMetrics", context="mapred")
final class LocalJobRunnerMetrics {

  @Metric
  private MutableCounterInt numMapTasksLaunched;
  @Metric
  private MutableCounterInt numMapTasksCompleted;
  @Metric
  private MutableCounterInt numReduceTasksLaunched;
  @Metric
  private MutableGaugeInt numReduceTasksCompleted;

  /**
   * 私有构造方法，禁止外部直接实例化
   */
  private LocalJobRunnerMetrics() {
  }

  /**
   * 创建并注册LocalJobRunner指标实例到默认metrics系统
   * @return 注册完成的LocalJobRunnerMetrics实例
   */
  public static LocalJobRunnerMetrics create() {
    // 初始化JobTracker级别的metrics系统
    MetricsSystem ms = DefaultMetricsSystem.initialize("JobTracker");
    // 随机生成唯一实例名称，注册到metrics系统
    return ms.register("LocalJobRunnerMetrics-" +
            ThreadLocalRandom.current().nextInt(), null,
        new LocalJobRunnerMetrics());
  }

  /**
   * 记录一个Map任务启动，递增已启动Map任务计数
   * @param taskAttemptID 任务尝试ID
   */
  public synchronized void launchMap(TaskAttemptID taskAttemptID) {
    numMapTasksLaunched.incr();
  }

  /**
   * 记录一个Map任务完成，递增已完成Map任务计数
   * @param taskAttemptID 任务尝试ID
   */
  public void completeMap(TaskAttemptID taskAttemptID) {
    numMapTasksCompleted.incr();
  }

  /**
   * 记录一个Reduce任务启动，递增已启动Reduce任务计数
   * @param taskAttemptID 任务尝试ID
   */
  public synchronized void launchReduce(TaskAttemptID taskAttemptID) {
    numReduceTasksLaunched.incr();
  }

  /**
   * 记录一个Reduce任务完成，递增已完成Reduce任务计数
   * @param taskAttemptID 任务尝试ID
   */
  public void completeReduce(TaskAttemptID taskAttemptID) {
    numReduceTasksCompleted.incr();
  }
}