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
package org.apache.hadoop.hdfs.server.balancer;

import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableGaugeInt;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;

/**
 * 文件: BalancerMetrics.java
 * 所属模块: HDFS 数据均衡器模块
 * 核心职责: 维护单个HDFS数据均衡器实例的运行指标，向Hadoop metrics系统暴露均衡过程的状态数据，用于监控告警和运维分析
 */

/**
 * 单个数据均衡器实例的运行指标采集类
 * 负责存储、更新并向Metrics系统导出均衡过程中的关键状态指标
 */
@Metrics(about="Balancer metrics", context="dfs")
final class BalancerMetrics {

  private final Balancer balancer;

  @Metric("If a balancer iterate is running")
  private MutableGaugeInt iterateRunning;

  @Metric("Bytes left to move to make cluster balanced")
  private MutableGaugeLong bytesLeftToMove;

  @Metric("Number of under utilized nodes")
  private MutableGaugeInt numOfUnderUtilizedNodes;

  @Metric("Number of over utilized nodes")
  private MutableGaugeInt numOfOverUtilizedNodes;

  /**
   * 获取当前均衡任务所属块池ID，作为metrics标签
   * @return 当前块池ID
   */
  @Metric(value = {"BlockPoolID", "Current BlockPoolID"}, type = Metric.Type.TAG)
  public String getBlockPoolID() {
    return balancer.getNnc().getBlockpoolID();
  }

  /**
   * 构造方法，关联对应的均衡器实例
   * @param b 均衡器实例
   */
  private BalancerMetrics(Balancer b) {
    this.balancer = b;
  }

  /**
   * 创建并注册均衡器指标到默认metrics系统
   * @param b 关联的均衡器实例
   * @return 已注册的均衡器指标实例
   */
  public static BalancerMetrics create(Balancer b) {
    BalancerMetrics m = new BalancerMetrics(b);
    return DefaultMetricsSystem.instance().register(
        m.getName(), null, m);
  }

  /**
   * 获取当前均衡器指标的唯一名称
   * @return 指标名称，格式为 Balancer-块池ID
   */
  String getName() {
    return "Balancer-" + balancer.getNnc().getBlockpoolID();
  }

  /**
   * 获取当前均衡运行中已经移动的数据量
   * @return 已移动字节数
   */
  @Metric("Bytes that already moved in current doBalance run.")
  public long getBytesMovedInCurrentRun() {
    return balancer.getNnc().getBytesMoved().get();
  }

  /**
   * 设置均衡迭代是否运行中
   * @param iterateRunning 是否运行中
   */
  void setIterateRunning(boolean iterateRunning) {
    this.iterateRunning.set(iterateRunning ? 1 : 0);
  }

  /**
   * 更新待移动数据量指标
   * @param bytesLeftToMove 剩余需要移动的字节数
   */
  void setBytesLeftToMove(long bytesLeftToMove) {
    this.bytesLeftToMove.set(bytesLeftToMove);
  }

  /**
   * 更新低利用率节点数量指标
   * @param numOfUnderUtilizedNodes 当前低利用率节点数量
   */
  void setNumOfUnderUtilizedNodes(int numOfUnderUtilizedNodes) {
    this.numOfUnderUtilizedNodes.set(numOfUnderUtilizedNodes);
  }

  /**
   * 更新高利用率节点数量指标
   * @param numOfOverUtilizedNodes 当前高利用率节点数量
   */
  void setNumOfOverUtilizedNodes(int numOfOverUtilizedNodes) {
    this.numOfOverUtilizedNodes.set(numOfOverUtilizedNodes);
  }
}