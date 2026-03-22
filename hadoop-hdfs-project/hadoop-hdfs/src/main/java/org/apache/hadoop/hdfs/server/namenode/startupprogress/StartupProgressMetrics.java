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
package org.apache.hadoop.hdfs.server.namenode.startupprogress;

import static org.apache.hadoop.metrics2.lib.Interns.info;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.Phase;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgressView;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;

/**
 * 文件功能：将NameNode启动进度信息暴露为Hadoop Metrics2指标，可通过JMX查看
 * 将{@link StartupProgress}与{@link MetricsSource}关联，通过JMX对外暴露NameNode启动进度信息
 */
@InterfaceAudience.Private
public class StartupProgressMetrics implements MetricsSource {

  /** 启动进度指标的元信息定义 */
  private static final MetricsInfo STARTUP_PROGRESS_METRICS_INFO =
    info("StartupProgress", "NameNode startup progress");

  private final StartupProgress startupProgress;

  /**
   * 注册与指定StartupProgress绑定的StartupProgressMetrics实例到指标系统
   * 
   * @param prog 要绑定的启动进度对象
   */
  public static void register(StartupProgress prog) {
    new StartupProgressMetrics(prog);
  }

  /**
   * 创建StartupProgressMetrics实例并注册到默认指标系统
   * 
   * @param startupProgress 要绑定的启动进度对象
   */
  public StartupProgressMetrics(StartupProgress startupProgress) {
    this.startupProgress = startupProgress;
    DefaultMetricsSystem.instance().register(
      STARTUP_PROGRESS_METRICS_INFO.name(),
      STARTUP_PROGRESS_METRICS_INFO.description(), this);
  }

  @Override
  /**
   * 收集并输出NameNode启动进度指标
   * @param collector 指标收集器
   * @param all 是否输出所有指标
   */
  public void getMetrics(MetricsCollector collector, boolean all) {
    // 创建不可变的启动进度视图用于指标采集
    StartupProgressView prog = startupProgress.createView();
    // 创建指标记录构建器
    MetricsRecordBuilder builder = collector.addRecord(
      STARTUP_PROGRESS_METRICS_INFO);

    // 添加整体耗时计数器
    builder.addCounter(info("ElapsedTime", "overall elapsed time"),
      prog.getElapsedTime());
    // 添加整体完成百分比指标
    builder.addGauge(info("PercentComplete", "overall percent complete"),
      prog.getPercentComplete());

    // 遍历所有启动阶段，添加各阶段的指标
    for (Phase phase: prog.getPhases()) {
      addCounter(builder, phase, "Count", " count", prog.getCount(phase));
      addCounter(builder, phase, "ElapsedTime", " elapsed time",
        prog.getElapsedTime(phase));
      addCounter(builder, phase, "Total", " total", prog.getTotal(phase));
      addGauge(builder, phase, "PercentComplete", " percent complete",
        prog.getPercentComplete(phase));
    }
  }

  /**
   * 为指定启动阶段添加计数器，自动拼接指标名称和描述
   * 
   * @param builder 指标记录构建器
   * @param phase 目标启动阶段
   * @param nameSuffix 指标名称后缀
   * @param descSuffix 指标描述后缀
   * @param value 计数器数值
   */
  private static void addCounter(MetricsRecordBuilder builder, Phase phase,
      String nameSuffix, String descSuffix, long value) {
    MetricsInfo metricsInfo = info(phase.getName() + nameSuffix,
      phase.getDescription() + descSuffix);
    builder.addCounter(metricsInfo, value);
  }

  /**
   * 为指定启动阶段添加计量指标，自动拼接指标名称和描述
   * 
   * @param builder 指标记录构建器
   * @param phase 目标启动阶段
   * @param nameSuffix 指标名称后缀
   * @param descSuffix 指标描述后缀
   * @param value 计量指标数值
   */
  private static void addGauge(MetricsRecordBuilder builder, Phase phase,
      String nameSuffix, String descSuffix, float value) {
    MetricsInfo metricsInfo = info(phase.getName() + nameSuffix,
      phase.getDescription() + descSuffix);
    builder.addGauge(metricsInfo, value);
  }
}