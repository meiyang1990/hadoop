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

package org.apache.hadoop.hdfs.server.datanode.metrics;


import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.protocol.OutlierMetrics;
import org.apache.hadoop.metrics2.MetricsJsonBuilder;
import org.apache.hadoop.metrics2.lib.MutableRollingAverages;
import org.apache.hadoop.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_MIN_OUTLIER_DETECTION_NODES_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_MIN_OUTLIER_DETECTION_NODES_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_PEER_METRICS_MIN_OUTLIER_DETECTION_SAMPLES_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_PEER_METRICS_MIN_OUTLIER_DETECTION_SAMPLES_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_SLOWPEER_LOW_THRESHOLD_MS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_SLOWPEER_LOW_THRESHOLD_MS_KEY;

/**
 * 文件路径: hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/metrics/DataNodePeerMetrics.java
 * 
 * DataNode节点间Peer通信指标管理类，维护数据节点对端节点的操作性能指标，
 * 支持基于滑动窗口统计平均延迟，并检测出性能异常的慢节点。
 * 核心功能包括：发送数据包延迟统计、异常节点检测、指标JSON导出。
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class DataNodePeerMetrics {

  public static final Logger LOG = LoggerFactory.getLogger(
      DataNodePeerMetrics.class);

  /** 下游发送数据包滑动窗口平均延迟统计 */
  private final MutableRollingAverages sendPacketDownstreamRollingAverages;

  /** 指标实例名称 */
  private final String name;

  // 仅供测试使用，生产代码不应访问该字段
  private Map<String, OutlierMetrics> testOutlier = null;

  /** 异常节点检测器，用于识别慢节点 */
  private final OutlierDetector slowNodeDetector;

  /**
   * 异常检测所需最小样本数，低于该值则跳过异常检测
   */
  private volatile long minOutlierDetectionSamples;
  /**
   * 非慢节点延迟阈值，延迟低于该值的节点肯定不是慢节点
   */
  private volatile long lowThresholdMs;
  /**
   * 异常检测所需最小节点数量，低于该值则跳过异常检测
   */
  private volatile long minOutlierDetectionNodes;

  /**
   * 构造DataNodePeerMetrics实例，从配置加载异常检测参数
   * @param name 指标实例名称
   * @param conf Hadoop配置对象
   */
  public DataNodePeerMetrics(final String name, Configuration conf) {
    this.name = name;
    minOutlierDetectionSamples = conf.getLong(
        DFS_DATANODE_PEER_METRICS_MIN_OUTLIER_DETECTION_SAMPLES_KEY,
        DFS_DATANODE_PEER_METRICS_MIN_OUTLIER_DETECTION_SAMPLES_DEFAULT);
    lowThresholdMs =
        conf.getLong(DFS_DATANODE_SLOWPEER_LOW_THRESHOLD_MS_KEY,
            DFS_DATANODE_SLOWPEER_LOW_THRESHOLD_MS_DEFAULT);
    minOutlierDetectionNodes =
        conf.getLong(DFS_DATANODE_MIN_OUTLIER_DETECTION_NODES_KEY,
            DFS_DATANODE_MIN_OUTLIER_DETECTION_NODES_DEFAULT);
    this.slowNodeDetector =
        new OutlierDetector(minOutlierDetectionNodes, lowThresholdMs);
    sendPacketDownstreamRollingAverages = new MutableRollingAverages("Time");
  }

  /**
   * 获取指标实例名称
   * @return 指标实例名称
   */
  public String name() {
    return name;
  }

  /**
   * 获取异常检测所需最小样本数配置
   * @return 最小样本数
   */
  public long getMinOutlierDetectionSamples() {
    return minOutlierDetectionSamples;
  }

  /**
   * 创建DataNodePeerMetrics实例，用于指标系统注册
   * @param dnName DataNode节点名称
   * @param conf Hadoop配置对象
   * @return 创建完成的DataNodePeerMetrics实例
   */
  public static DataNodePeerMetrics create(String dnName, Configuration conf) {
    final String name = "DataNodePeerActivity-" + (dnName.isEmpty()
        ? "UndefinedDataNodeName" + ThreadLocalRandom.current().nextInt()
        : dnName.replace(':', '-'));

    return new DataNodePeerMetrics(name, conf);
  }

  /**
   * 添加一次对端节点下游发送数据包的执行记录与耗时
   * @param peerAddr 对端节点地址，格式已格式化用于生成指标名
   * @param elapsedMs 本次发送耗时，单位毫秒
   */
  public void addSendPacketDownstream(
      final String peerAddr,
      final long elapsedMs) {
    sendPacketDownstreamRollingAverages.add(peerAddr, elapsedMs);
  }

  /**
   * 将下游发送数据包平均延迟指标导出为JSON格式
   * @return 指标JSON字符串
   */
  public String dumpSendPacketDownstreamAvgInfoAsJson() {
    final MetricsJsonBuilder builder = new MetricsJsonBuilder(null);
    sendPacketDownstreamRollingAverages.snapshot(builder, true);
    return builder.toString();
  }

  /**
   * 收集ThreadLocal中缓存的指标状态，合并到全局统计
   */
  public void collectThreadLocalStates() {
    sendPacketDownstreamRollingAverages.collectThreadLocalStates();
  }

  /**
   * 获取所有检测出的慢异常节点
   * @return 异常节点地址与对应指标的映射
   */
  public Map<String, OutlierMetrics> getOutliers() {
    // 生产代码中testOutlier必须为null
    if (testOutlier == null) {
      // 获取满足最小样本数要求的节点平均延迟统计
      final Map<String, Double> stats =
          sendPacketDownstreamRollingAverages.getStats(minOutlierDetectionSamples);
      LOG.trace("DataNodePeerMetrics: Got stats: {}", stats);
      return slowNodeDetector.getOutlierMetrics(stats);
    } else {
      // 仅测试代码会进入该分支
      return testOutlier;
    }
  }

  /**
   * 仅供测试使用，直接设置异常节点结果，跳过实际计算
   * @param outlier 测试用异常节点映射
   */
  public void setTestOutliers(Map<String, OutlierMetrics> outlier) {
    this.testOutlier = outlier;
  }

  /**
   * 获取下游发送数据包滑动平均统计对象
   * @return 滑动平均统计对象
   */
  public MutableRollingAverages getSendPacketDownstreamRollingAverages() {
    return sendPacketDownstreamRollingAverages;
  }

  /**
   * 更新异常检测所需最小节点数配置
   * @param minNodes 最小节点数，必须大于0
   */
  public void setMinOutlierDetectionNodes(long minNodes) {
    Preconditions.checkArgument(minNodes > 0,
        DFS_DATANODE_MIN_OUTLIER_DETECTION_NODES_KEY + " should be larger than 0");
    minOutlierDetectionNodes = minNodes;
    this.slowNodeDetector.setMinNumResources(minNodes);
  }

  /**
   * 获取异常检测所需最小节点数配置
   * @return 最小节点数
   */
  public long getMinOutlierDetectionNodes() {
    return minOutlierDetectionNodes;
  }

  /**
   * 更新非慢节点延迟阈值配置
   * @param thresholdMs 延迟阈值，单位毫秒，必须大于0
   */
  public void setLowThresholdMs(long thresholdMs) {
    Preconditions.checkArgument(thresholdMs > 0,
        DFS_DATANODE_SLOWPEER_LOW_THRESHOLD_MS_KEY + " should be larger than 0");
    lowThresholdMs = thresholdMs;
    this.slowNodeDetector.setLowThresholdMs(thresholdMs);
  }

  /**
   * 获取非慢节点延迟阈值配置
   * @return 延迟阈值，单位毫秒
   */
  public long getLowThresholdMs() {
    return lowThresholdMs;
  }

  /**
   * 更新异常检测所需最小样本数配置
   * @param minSamples 最小样本数，必须大于0
   */
  public void setMinOutlierDetectionSamples(long minSamples) {
    Preconditions.checkArgument(minSamples > 0,
        DFS_DATANODE_PEER_METRICS_MIN_OUTLIER_DETECTION_SAMPLES_KEY +
            " should be larger than 0");
    minOutlierDetectionSamples = minSamples;
  }

  /**
   * 仅供测试使用，获取异常检测器实例
   * @return 异常检测器实例
   */
  @VisibleForTesting
  public OutlierDetector getSlowNodeDetector() {
    return this.slowNodeDetector;
  }
}