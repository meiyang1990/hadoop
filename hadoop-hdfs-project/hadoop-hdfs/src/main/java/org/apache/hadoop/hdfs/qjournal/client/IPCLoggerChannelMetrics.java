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
package org.apache.hadoop.hdfs.qjournal.client;

import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableQuantiles;

/**
 * 文件说明：QJournal日志写端IPC通道指标统计类，负责统计日志节点客户端通道的运行时性能指标
 * 
 * 类说明：IPC日志节点通道指标，从写者视角统计单个日志节点客户端通道的各类性能和状态指标
 * 核心职责：收集并提供日志写入延迟、同步状态、节点滞后情况等运行指标，供Hadoop监控系统采集展示
 */
@Metrics(about="Journal client metrics", context="dfs")
class IPCLoggerChannelMetrics {
  final MetricsRegistry registry = new MetricsRegistry("NameNode");

  private volatile IPCLoggerChannel ch;
  
  private final MutableQuantiles[] writeEndToEndLatencyQuantiles;
  private final MutableQuantiles[] writeRpcLatencyQuantiles;

  /**
   * 构造方法，初始化IPC通道指标，加载配置创建分位数统计器
   * @param ch 对应的IPC日志通道对象
   */
  private IPCLoggerChannelMetrics(IPCLoggerChannel ch) {
    this.ch = ch;
    
    Configuration conf = new HdfsConfiguration();
    // 从配置中获取百分位统计的时间间隔数组
    int[] intervals = 
        conf.getInts(DFSConfigKeys.DFS_METRICS_PERCENTILES_INTERVALS_KEY);
    if (intervals != null) {
      // 初始化端到端写入延迟分位数统计数组
      writeEndToEndLatencyQuantiles = new MutableQuantiles[intervals.length];
      // 初始化写入RPC延迟分位数统计数组
      writeRpcLatencyQuantiles = new MutableQuantiles[intervals.length];
      // 遍历每个间隔，创建对应分位数统计器
      for (int i = 0; i < writeEndToEndLatencyQuantiles.length; i++) {
        int interval = intervals[i];
        writeEndToEndLatencyQuantiles[i] = registry.newQuantiles(
            "writesE2E" + interval + "s",
            "End-to-end time for write operations", "ops", "LatencyMicros", interval);
        writeRpcLatencyQuantiles[i] = registry.newQuantiles(
            "writesRpc" + interval + "s",
            "RPC RTT for write operations", "ops", "LatencyMicros", interval);
      }
    } else {
      // 未配置百分位统计间隔，不创建统计器
      writeEndToEndLatencyQuantiles = null;
      writeRpcLatencyQuantiles = null;
    }
  }

  /**
   * 从指标系统注销当前通道的指标
   */
  public void unregister() {
    DefaultMetricsSystem.instance().unregisterSource(getName(ch));
  }

  /**
   * 创建并注册一个新的IPC通道指标实例到Hadoop指标系统
   * @param ch 对应的IPC日志通道
   * @return 初始化完成的指标实例
   */
  static IPCLoggerChannelMetrics create(IPCLoggerChannel ch) {
    String name = getName(ch);
    IPCLoggerChannelMetrics m = new IPCLoggerChannelMetrics(ch);
    DefaultMetricsSystem.instance().register(name, null, m);
    return m;
  }

  /**
   * 根据IPC通道信息生成合法的指标名称，适配MBean命名规则
   * @param ch IPC日志通道
   * @return 格式化后的指标名称
   */
  private static String getName(IPCLoggerChannel ch) {
    InetSocketAddress addr = ch.getRemoteAddress();
    String addrStr = addr.getAddress().getHostAddress();
    
    // IPv6地址包含冒号，不允许作为MBean名称的一部分，替换为点号
    addrStr = addrStr.replace(':', '.');
    
    return "IPCLoggerChannel-" + addrStr +
        "-" + addr.getPort();
  }

  /**
   * 指标方法：获取远程日志节点是否与集群仲裁节点不同步
   * @return 是否不同步的字符串标识
   */
  @Metric("Is the remote logger out of sync with the quorum")
  public String isOutOfSync() {
    return Boolean.toString(ch.isOutOfSync()); 
  }
  
  /**
   * 指标方法：获取远程日志节点滞后于仲裁集群的事务数量
   * @return 滞后的事务数
   */
  @Metric("The number of transactions the remote log is lagging behind the " +
          "quorum")
  public long getCurrentLagTxns() {
    return ch.getLagTxns();
  }
  
  /**
   * 指标方法：获取远程日志节点滞后于仲裁集群的时间（毫秒）
   * @return 滞后时间，单位毫秒
   */
  @Metric("The number of milliseconds the remote log is lagging behind the " +
          "quorum")
  public long getLagTimeMillis() {
    return ch.getLagTimeMillis();
  }
  
  /**
   * 指标方法：获取等待发送到远程节点的待处理数据字节数
   * @return 待发送编辑日志大小，单位字节
   */
  @Metric("The number of bytes of pending data to be sent to the remote node")
  public int getQueuedEditsSize() {
    return ch.getQueuedEditsSize();
  }

  /**
   * 添加一次端到端写入延迟样本到分位数统计
   * @param micros 延迟时间，单位微秒
   */
  public void addWriteEndToEndLatency(long micros) {
    if (writeEndToEndLatencyQuantiles != null) {
      for (MutableQuantiles q : writeEndToEndLatencyQuantiles) {
        q.add(micros);
      }
    }
  }
  
  /**
   * 添加一次写入RPC往返延迟样本到分位数统计
   * @param micros 延迟时间，单位微秒
   */
  public void addWriteRpcLatency(long micros) {
    if (writeRpcLatencyQuantiles != null) {
      for (MutableQuantiles q : writeRpcLatencyQuantiles) {
        q.add(micros);
      }
    }
  }
}