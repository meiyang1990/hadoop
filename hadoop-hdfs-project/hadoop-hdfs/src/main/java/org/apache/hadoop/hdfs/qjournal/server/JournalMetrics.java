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
package org.apache.hadoop.hdfs.qjournal.server;

import java.io.IOException;

import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metric.Type;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableQuantiles;
import org.apache.hadoop.metrics2.lib.MutableStat;

/**
 * 日志节点JournalNode侧单个日志Journal的服务端指标采集类，用于统计QJournal服务运行时各项性能与业务指标
 */
@Metrics(about="Journal metrics", context="dfs")
class JournalMetrics {
  final MetricsRegistry registry = new MetricsRegistry("JournalNode");
  
  @Metric("Number of batches written since startup")
  MutableCounterLong batchesWritten;
  
  @Metric("Number of txns written since startup")
  MutableCounterLong txnsWritten;
  
  @Metric("Number of bytes written since startup")
  MutableCounterLong bytesWritten;

  @Metric("Number of txns served via RPC")
  MutableCounterLong txnsServedViaRpc;

  @Metric("Number of bytes served via RPC")
  MutableCounterLong bytesServedViaRpc;

  private MutableStat rpcRequestCacheMissAmount;

  @Metric("Number of RPC requests with zero edits returned")
  MutableCounterLong rpcEmptyResponses;

  @Metric("Number of batches written where this node was lagging")
  MutableCounterLong batchesWrittenWhileLagging;

  @Metric("Number of edit logs downloaded by JournalNodeSyncer")
  private MutableCounterLong numEditLogsSynced;
  
  // 分位数统计时间窗口，单位秒
  private final int[] QUANTILE_INTERVALS = new int[] {
      1*60, // 1m
      5*60, // 5m
      60*60 // 1h
  };
  
  // 日志同步延迟分位数统计数组，对应不同时间窗口
  final MutableQuantiles[] syncsQuantiles;
  
  private final Journal journal;

  /**
   * 构造JournalMetrics，绑定对应Journal实例并初始化分位数统计
   * @param journal 绑定的Journal实例
   */
  JournalMetrics(Journal journal) {
    this.journal = journal;
    
    syncsQuantiles = new MutableQuantiles[QUANTILE_INTERVALS.length];
    // 遍历每个时间窗口，创建对应分位数统计对象
    for (int i = 0; i < syncsQuantiles.length; i++) {
      int interval = QUANTILE_INTERVALS[i];
      syncsQuantiles[i] = registry.newQuantiles(
          "syncs" + interval + "s",
          "Journal sync time", "ops", "latencyMicros", interval);
    }
    // 初始化RPC请求缓存未命中统计
    rpcRequestCacheMissAmount = registry
        .newStat("RpcRequestCacheMissAmount", "Number of RPC requests unable to be " +
                "served due to lack of availability in cache, and how many " +
                "transactions away the request was from being in the cache.",
            "Misses", "Txns");
  }
  
  /**
   * 创建并注册JournalMetrics实例到默认指标系统
   * @param j 要绑定的Journal实例
   * @return 创建并注册完成的JournalMetrics实例
   */
  public static JournalMetrics create(Journal j) {
    JournalMetrics m = new JournalMetrics(j);
    return DefaultMetricsSystem.instance().register(
        m.getName(), null, m);
  }

  /**
   * 获取当前Journal指标的名称，包含Journal编号
   * @return 指标名称字符串
   */
  String getName() {
    return "Journal-" + journal.getJournalId();
  }

  @Metric(value={"JournalId", "Current JournalId"}, type=Type.TAG)
  public String getJournalId() {
    return journal.getJournalId();
  }

  @Metric("Current writer's epoch")
  public long getLastWriterEpoch() {
    try {
      return journal.getLastWriterEpoch();
    } catch (IOException e) {
      return -1L;
    }
  }
  
  @Metric("Last accepted epoch")
  public long getLastPromisedEpoch() {
    try {
      return journal.getLastPromisedEpoch();
    } catch (IOException e) {
      return -1L;
    }
  }
  
  @Metric("The highest txid stored on this JN")
  public long getLastWrittenTxId() {
    return journal.getHighestWrittenTxId();
  }
  
  @Metric("Number of transactions that this JN is lagging")
  public long getCurrentLagTxns() {
    try {
      return journal.getCurrentLagTxns();
    } catch (IOException e) {
      return -1L;
    }
  }

  @Metric("The timestamp of last successfully written transaction")
  public long getLastJournalTimestamp() {
    return journal.getLastJournalTimestamp();
  }

  /**
   * 添加一次日志同步耗时到所有时间窗口的分位数统计中
   * @param us 同步耗时，单位微秒
   */
  void addSync(long us) {
    for (MutableQuantiles q : syncsQuantiles) {
      q.add(us);
    }
  }

  public MutableCounterLong getNumEditLogsSynced() {
    return numEditLogsSynced;
  }

  /**
   * 自增JournalNode同步器下载的日志文件计数
   */
  public void incrNumEditLogsSynced() {
    numEditLogsSynced.incr();
  }

  /**
   * 添加一次RPC请求缓存未命中的事务偏移量到统计
   * @param cacheMissAmount 当前未命中请求距离缓存边界的事务数量
   */
  public void addRpcRequestCacheMissAmount(long cacheMissAmount) {
    rpcRequestCacheMissAmount.add(cacheMissAmount);
  }
}