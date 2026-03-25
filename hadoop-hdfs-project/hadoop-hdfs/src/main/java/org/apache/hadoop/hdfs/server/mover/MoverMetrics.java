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
package org.apache.hadoop.hdfs.server.mover;

import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableGaugeInt;

/**
 * 单个块池HDFS数据块迁移任务（Mover）的指标统计类
 * 用于采集和暴露Mover工具运行过程中的各项监控指标，供Hadoop监控系统使用
 */
/**
 * Metrics for HDFS Mover of a blockpool.
 */
@Metrics(about="Mover metrics", context="dfs")
final class MoverMetrics {

  private final Mover mover;

  @Metric("If mover is processing namespace.")
  private MutableGaugeInt processingNamespace;

  @Metric("Number of blocks being scheduled.")
  private MutableCounterLong blocksScheduled;

  @Metric("Number of files being processed.")
  private MutableCounterLong filesProcessed;

  /**
   * 构造方法，绑定对应的Mover实例
   * @param m 对应的Mover实例
   */
  private MoverMetrics(Mover m) {
    this.mover = m;
  }

  /**
   * 创建并注册Mover指标到Hadoop默认指标系统
   * @param mover 对应的Mover实例
   * @return 创建完成的MoverMetrics实例
   */
  public static MoverMetrics create(Mover mover) {
    MoverMetrics m = new MoverMetrics(mover);
    return DefaultMetricsSystem.instance().register(
        m.getName(), null, m);
  }

  /**
   * 获取当前Mover指标的唯一名称，包含块池ID标识
   * @return 指标唯一名称
   */
  String getName() {
    return "Mover-" + mover.getNnc().getBlockpoolID();
  }

  /**
   * 获取Mover已经迁移完成的字节总数指标
   * @return 已迁移总字节数
   */
  @Metric("Bytes that already moved by mover.")
  public long getBytesMoved() {
    return mover.getNnc().getBytesMoved().get();
  }

  /**
   * 获取Mover迁移成功的数据块总数指标
   * @return 迁移成功的块总数
   */
  @Metric("Number of blocks that successfully moved by mover.")
  public long getBlocksMoved() {
    return mover.getNnc().getBlocksMoved().get();
  }

  /**
   * 获取Mover迁移失败的数据块总数指标
   * @return 迁移失败的块总数
   */
  @Metric("Number of blocks that failed moved by mover.")
  public long getBlocksFailed() {
    return mover.getNnc().getBlocksFailed().get();
  }

  /**
   * 设置当前是否正在处理命名空间的状态指标
   * @param processingNamespace 是否正在处理命名空间
   */
  void setProcessingNamespace(boolean processingNamespace) {
    this.processingNamespace.set(processingNamespace ? 1 : 0);
  }

  /**
   * 增加已调度待迁移的数据块计数
   */
  void incrBlocksScheduled() {
    this.blocksScheduled.incr();
  }

  /**
   * 增加已处理完成的文件计数
   */
  void incrFilesProcessed() {
    this.filesProcessed.incr();
  }
}