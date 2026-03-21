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
package org.apache.hadoop.yarn.server.sharedcachemanager.metrics;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsAnnotations;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MetricsSourceBuilder;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 文件级注释：YARN共享缓存清理器指标统计类，维护清理服务的各项活动统计数据，并通过Hadoop metrics2框架对外发布指标
 * This class is for maintaining the various Cleaner activity statistics and
 * publishing them through the metrics interfaces.
 */
@Private
@Evolving
@Metrics(name = "CleanerActivity", about = "Cleaner service metrics", context = "yarn")
public class CleanerMetrics {
  // 日志记录器
  public static final Logger LOG =
      LoggerFactory.getLogger(CleanerMetrics.class);
  // 指标注册表，管理本模块所有指标
  private final MetricsRegistry registry = new MetricsRegistry("cleaner");
  // 单例实例
  private final static CleanerMetrics INSTANCE = create();
  
  /** 获取单例实例 */
  public static CleanerMetrics getInstance() {
    return INSTANCE;
  }

  @Metric("number of deleted files over all runs")
  private MutableCounterLong totalDeletedFiles;

  /** 获取累计删除文件总数 */
  public long getTotalDeletedFiles() {
    return totalDeletedFiles.value();
  }

  private @Metric("number of deleted files in the last run")
  MutableGaugeLong deletedFiles;

  /** 获取最近一次清理运行删除文件数 */
  public long getDeletedFiles() {
    return deletedFiles.value();
  }

  @Metric("number of processed files over all runs")
  private MutableCounterLong totalProcessedFiles;

  /** 获取累计处理文件总数 */
  public long getTotalProcessedFiles() {
    return totalProcessedFiles.value();
  }

  private @Metric("number of processed files in the last run")
  MutableGaugeLong processedFiles;

  /** 获取最近一次清理运行处理文件数 */
  public long getProcessedFiles() {
    return processedFiles.value();
  }

  @Metric("number of file errors over all runs")
  private MutableCounterLong totalFileErrors;

  /** 获取累计处理错误文件总数 */
  public long getTotalFileErrors() {
    return totalFileErrors.value();
  }

  private @Metric("number of file errors in the last run")
  MutableGaugeLong fileErrors;

  /** 获取最近一次清理运行错误文件数 */
  public long getFileErrors() {
    return fileErrors.value();
  }

  private CleanerMetrics() {
  }

  /**
   * The metric source obtained after parsing the annotations
   * 解析注解后得到的指标源，用于向metrics系统提供指标数据
   */
  MetricsSource metricSource;

  /** 创建并初始化清理器指标实例，向默认指标系统注册 */
  static CleanerMetrics create() {
    // 获取默认metrics系统实例
    MetricsSystem ms = DefaultMetricsSystem.instance();

    CleanerMetrics metricObject = new CleanerMetrics();
    // 根据注解构建指标源
    MetricsSourceBuilder sb = MetricsAnnotations.newSourceBuilder(metricObject);
    final MetricsSource s = sb.build();
    // 向metrics系统注册清理器指标
    ms.register("cleaner", "The cleaner service of truly shared cache", s);
    metricObject.metricSource = s;
    return metricObject;
  }

  /**
   * Report a delete operation at the current system time
   * 上报一次成功的文件删除操作，更新对应指标
   */
  public void reportAFileDelete() {
    // 累计处理数+1
    totalProcessedFiles.incr();
    // 本次处理数+1
    processedFiles.incr();
    // 累计删除数+1
    totalDeletedFiles.incr();
    // 本次删除数+1
    deletedFiles.incr();
  }

  /**
   * Report a process operation at the current system time
   * 上报一次文件处理操作（未删除），更新对应指标
   */
  public void reportAFileProcess() {
    // 累计处理数+1
    totalProcessedFiles.incr();
    // 本次处理数+1
    processedFiles.incr();
  }

  /**
   * Report a process operation error at the current system time
   * 上报一次文件处理错误，更新对应指标
   */
  public void reportAFileError() {
    // 累计处理数+1
    totalProcessedFiles.incr();
    // 本次处理数+1
    processedFiles.incr();
    // 累计错误数+1
    totalFileErrors.incr();
    // 本次错误数+1
    fileErrors.incr();
  }

  /**
   * Report the start a new run of the cleaner.
   * 上报新一轮清理运行开始，重置本次运行的指标计数器
   *
   */
  public void reportCleaningStart() {
    // 重置本次处理文件数为0
    processedFiles.set(0);
    // 重置本次删除文件数为0
    deletedFiles.set(0);
    // 重置本次错误文件数为0
    fileErrors.set(0);
  }

}