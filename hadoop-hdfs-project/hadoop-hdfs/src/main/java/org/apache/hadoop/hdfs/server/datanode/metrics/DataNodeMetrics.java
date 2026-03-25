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

import static org.apache.hadoop.metrics2.impl.MsInfo.SessionId;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.protocol.DataNodeUsageReport;
import org.apache.hadoop.hdfs.server.protocol.DataNodeUsageReportUtil;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableQuantiles;
import org.apache.hadoop.metrics2.lib.MutableRate;
import org.apache.hadoop.metrics2.lib.MutableGaugeInt;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.apache.hadoop.metrics2.lib.MutableRatesWithAggregation;
import org.apache.hadoop.metrics2.source.JvmMetrics;

import java.util.concurrent.ThreadLocalRandom;

/**
 * 文件: DataNode 指标统计类
 * 功能: 维护 DataNode 运行过程中的各类统计信息，并通过 Hadoop Metrics2 框架对外发布指标，同时注册JMX MBean支持监控查询。
 * 职责: 定义了DataNode运行时需要采集的各类指标（读写IO、块操作、网络错误、RamDisk、纠删码等），并提供更新指标的方法供DataNode组件调用。
 */
@InterfaceAudience.Private
@Metrics(about="DataNode metrics", context="dfs")
public class DataNodeMetrics {

  @Metric MutableCounterLong bytesWritten;
  @Metric("Milliseconds spent writing")
  MutableCounterLong totalWriteTime;
  @Metric MutableCounterLong bytesRead;
  @Metric("Milliseconds spent reading")
  MutableCounterLong totalReadTime;
  @Metric private MutableRate readTransferRate;
  final private MutableQuantiles[] readTransferRateQuantiles;
  @Metric MutableCounterLong blocksWritten;
  @Metric MutableCounterLong blocksRead;
  @Metric MutableCounterLong blocksReplicated;
  @Metric MutableCounterLong blocksRemoved;
  @Metric MutableCounterLong blocksVerified;
  @Metric MutableCounterLong blockVerificationFailures;
  @Metric MutableCounterLong blocksCached;
  @Metric MutableCounterLong blocksUncached;
  @Metric MutableCounterLong readsFromLocalClient;
  @Metric MutableCounterLong readsFromRemoteClient;
  @Metric MutableCounterLong writesFromLocalClient;
  @Metric MutableCounterLong writesFromRemoteClient;
  @Metric MutableCounterLong blocksGetLocalPathInfo;
  @Metric("Bytes read by remote client")
  MutableCounterLong remoteBytesRead;
  @Metric("Bytes written by remote client")
  MutableCounterLong remoteBytesWritten;

  // RamDisk 读写相关指标
  @Metric MutableCounterLong ramDiskBlocksWrite;
  @Metric MutableCounterLong ramDiskBlocksWriteFallback;
  @Metric MutableCounterLong ramDiskBytesWrite;
  @Metric MutableCounterLong ramDiskBlocksReadHits;

  // RamDisk 驱逐相关指标
  @Metric MutableCounterLong ramDiskBlocksEvicted;
  @Metric MutableCounterLong ramDiskBlocksEvictedWithoutRead;
  @Metric MutableRate        ramDiskBlocksEvictionWindowMs;
  final MutableQuantiles[]   ramDiskBlocksEvictionWindowMsQuantiles;


  // RamDisk 延迟持久化相关指标
  @Metric MutableCounterLong ramDiskBlocksLazyPersisted;
  @Metric MutableCounterLong ramDiskBlocksDeletedBeforeLazyPersisted;
  @Metric MutableCounterLong ramDiskBytesLazyPersisted;
  @Metric MutableRate        ramDiskBlocksLazyPersistWindowMs;
  final MutableQuantiles[]   ramDiskBlocksLazyPersistWindowMsQuantiles;

  @Metric MutableCounterLong fsyncCount;

  @Metric MutableCounterLong volumeFailures;

  @Metric("Count of network errors on the datanode")
  MutableCounterLong datanodeNetworkErrors;

  @Metric("Count of active dataNode xceivers")
  private MutableGaugeInt dataNodeActiveXceiversCount;

  @Metric("Count of read active dataNode xceivers")
  private MutableGaugeInt dataNodeReadActiveXceiversCount;

  @Metric("Count of write active dataNode xceivers")
  private MutableGaugeInt dataNodeWriteActiveXceiversCount;

  @Metric("Count of active DataNode packetResponder")
  private MutableGaugeInt dataNodePacketResponderCount;

  @Metric("Count of active DataNode block recovery worker")
  private MutableGaugeInt dataNodeBlockRecoveryWorkerCount;

  @Metric MutableRate readBlockOp;
  @Metric MutableRate writeBlockOp;
  @Metric MutableRate blockChecksumOp;
  @Metric MutableRate copyBlockOp;
  @Metric MutableRate replaceBlockOp;
  @Metric MutableRate heartbeats;
  @Metric MutableRate heartbeatsTotal;
  @Metric MutableRate lifelines;
  @Metric MutableRate blockReports;
  @Metric private MutableRate blockReportsCreateCostMills;
  @Metric MutableRate incrementalBlockReports;
  @Metric MutableRate cacheReports;
  @Metric MutableRate packetAckRoundTripTimeNanos;
  final MutableQuantiles[] packetAckRoundTripTimeNanosQuantiles;
  
  @Metric MutableRate flushNanos;
  final MutableQuantiles[] flushNanosQuantiles;
  
  @Metric MutableRate fsyncNanos;
  final MutableQuantiles[] fsyncNanosQuantiles;
  
  @Metric MutableRate sendDataPacketBlockedOnNetworkNanos;
  final MutableQuantiles[] sendDataPacketBlockedOnNetworkNanosQuantiles;
  @Metric MutableRate sendDataPacketTransferNanos;
  final MutableQuantiles[] sendDataPacketTransferNanosQuantiles;

  @Metric("Count of blocks in pending IBR")
  private MutableGaugeLong blocksInPendingIBR;
  @Metric("Count of blocks at receiving status in pending IBR")
  private MutableGaugeLong blocksReceivingInPendingIBR;
  @Metric("Count of blocks at received status in pending IBR")
  private MutableGaugeLong blocksReceivedInPendingIBR;
  @Metric("Count of blocks at deleted status in pending IBR")
  private MutableGaugeLong blocksDeletedInPendingIBR;
  @Metric("Count of erasure coding reconstruction tasks")
  MutableCounterLong ecReconstructionTasks;
  @Metric("Count of erasure coding failed reconstruction tasks")
  MutableCounterLong ecFailedReconstructionTasks;
  @Metric("Count of erasure coding invalidated reconstruction tasks")
  private MutableCounterLong ecInvalidReconstructionTasks;
  @Metric("Nanoseconds spent by decoding tasks")
  MutableCounterLong ecDecodingTimeNanos;
  @Metric("Bytes read by erasure coding worker")
  MutableCounterLong ecReconstructionBytesRead;
  @Metric("Bytes written by erasure coding worker")
  MutableCounterLong ecReconstructionBytesWritten;
  @Metric("Bytes remote read by erasure coding worker")
  MutableCounterLong ecReconstructionRemoteBytesRead;
  @Metric("Milliseconds spent on read by erasure coding worker")
  private MutableCounterLong ecReconstructionReadTimeMillis;
  @Metric("Milliseconds spent on decoding by erasure coding worker")
  private MutableCounterLong ecReconstructionDecodingTimeMillis;
  @Metric("Milliseconds spent on write by erasure coding worker")
  private MutableCounterLong ecReconstructionWriteTimeMillis;
  @Metric("Milliseconds spent on validating by erasure coding worker")
  private MutableCounterLong ecReconstructionValidateTimeMillis;
  @Metric("Sum of all BPServiceActors command queue length")
  private MutableCounterLong sumOfActorCommandQueueLength;
  @Metric("Num of processed commands of all BPServiceActors")
  private MutableCounterLong numProcessedCommands;
  @Metric("Rate of processed commands of all BPServiceActors")
  private MutableRate processedCommandsOp;
  @Metric("Number of blocks in IBRs that failed due to null storage")
  private MutableCounterLong nullStorageBlockReports;

  // FsDatasetImpl 本地文件操作相关指标
  @Metric private MutableRate createRbwOp;
  @Metric private MutableRate recoverRbwOp;
  @Metric private MutableRate convertTemporaryToRbwOp;
  @Metric private MutableRate createTemporaryOp;
  @Metric private MutableRate finalizeBlockOp;
  @Metric private MutableRate unfinalizeBlockOp;
  @Metric private MutableRate checkAndUpdateOp;
  @Metric private MutableRate updateReplicaUnderRecoveryOp;

  @Metric MutableCounterLong packetsReceived;
  @Metric MutableCounterLong packetsSlowWriteToMirror;
  @Metric MutableCounterLong packetsSlowWriteToDisk;
  @Metric MutableCounterLong packetsSlowWriteToOsCache;
  @Metric private MutableCounterLong slowFlushOrSyncCount;
  @Metric private MutableCounterLong slowAckToUpstreamCount;

  @Metric("Number of replaceBlock ops between" +
      " storage types on same host with local copy")
  private MutableCounterLong replaceBlockOpOnSameHost;
  @Metric("Number of replaceBlock ops between" +
      " storage types on same disk mount with same disk tiering feature")
  private MutableCounterLong replaceBlockOpOnSameMount;
  @Metric("Number of replaceBlock ops to another node")
  private MutableCounterLong replaceBlockOpToOtherHost;

  final MetricsRegistry registry = new MetricsRegistry("datanode");
  @Metric("Milliseconds spent on calling NN rpc")
  private MutableRatesWithAggregation
      nnRpcLatency = registry.newRatesWithAggregation("nnRpcLatency");
  @Metric("Nanoseconds spent on acquire dataset write lock")
  private MutableRate acquireDatasetWriteLock;
  @Metric("Nanoseconds spent on acquire dataset read lock")
  private MutableRate acquireDatasetReadLock;

  final String name;
  JvmMetrics jvmMetrics = null;
  private DataNodeUsageReportUtil dnUsageReportUtil;

  /**
   * 构造DataNodeMetrics实例，初始化各类分位数统计数组
   * @param name 指标名称前缀
   * @param sessionId 会话ID标签
   * @param intervals 分位数统计时间间隔数组
   * @param jvmMetrics JVM指标实例
   */
  public DataNodeMetrics(String name, String sessionId, int[] intervals,
      final JvmMetrics jvmMetrics) {
    this.name = name;
    this.jvmMetrics = jvmMetrics;    
    registry.tag(SessionId, sessionId);
    
    final int len = intervals.length;
    dnUsageReportUtil = new DataNodeUsageReportUtil();
    packetAckRoundTripTimeNanosQuantiles = new MutableQuantiles[len];
    flushNanosQuantiles = new MutableQuantiles[len];
    fsyncNanosQuantiles = new MutableQuantiles[len];
    sendDataPacketBlockedOnNetworkNanosQuantiles = new MutableQuantiles[len];
    sendDataPacketTransferNanosQuantiles = new MutableQuantiles[len];
    ramDiskBlocksEvictionWindowMsQuantiles = new MutableQuantiles[len];
    ramDiskBlocksLazyPersistWindowMsQuantiles = new MutableQuantiles[len];
    readTransferRateQuantiles = new MutableQuantiles[len];
    // 遍历初始化每个时间间隔对应的分位数统计对象
    for (int i = 0; i < len; i++) {
      int interval = intervals[i];
      packetAckRoundTripTimeNanosQuantiles[i] = registry.newQuantiles(
          "packetAckRoundTripTimeNanos" + interval + "s",
          "Packet Ack RTT in ns", "ops", "latency", interval);
      flushNanosQuantiles[i] = registry.newQuantiles(
          "flushNanos" + interval + "s", 
          "Disk flush latency in ns", "ops", "latency", interval);
      fsyncNanosQuantiles[i] = registry.newQuantiles(
          "fsyncNanos" + interval + "s", "Disk fsync latency in ns", 
          "ops", "latency", interval);
      sendDataPacketBlockedOnNetworkNanosQuantiles[i] = registry.newQuantiles(
          "sendDataPacketBlockedOnNetworkNanos" + interval + "s", 
          "Time blocked on network while sending a packet in ns",
          "ops", "latency", interval);
      sendDataPacketTransferNanosQuantiles[i] = registry.newQuantiles(
          "sendDataPacketTransferNanos" + interval + "s", 
          "Time reading from disk and writing to network while sending " +
          "a packet in ns", "ops", "latency", interval);
      ramDiskBlocksEvictionWindowMsQuantiles[i] = registry.newQuantiles(
          "ramDiskBlocksEvictionWindows" + interval + "s",
          "Time between the RamDisk block write and eviction in ms",
          "ops", "latency", interval);
      ramDiskBlocksLazyPersistWindowMsQuantiles[i] = registry.newQuantiles(
          "ramDiskBlocksLazyPersistWindows" + interval + "s",
          "Time between the RamDisk block write and disk persist in ms",
          "ops", "latency", interval);
      readTransferRateQuantiles[i] = registry.newInverseQuantiles(
          "readTransferRate" + interval + "s",
          "Rate at which bytes are read from datanode calculated in bytes per second",
          "ops", "rate", interval);
    }
  }

  /**
   * 根据配置创建并注册DataNodeMetrics实例到指标系统
   * @param conf Hadoop配置对象
   * @param dnName DataNode名称
   * @return 创建好的已注册DataNodeMetrics实例
   */
  public static DataNodeMetrics create(Configuration conf, String dnName) {
    String sessionId = conf.get(DFSConfigKeys.DFS_METRICS_SESSION_ID_KEY);
    MetricsSystem ms = DefaultMetricsSystem.instance();
    JvmMetrics jm = JvmMetrics.create("DataNode", sessionId, ms);
    // 构造唯一指标名称，替换冒号避免格式问题，空名称则生成随机名
    String name = "DataNodeActivity-"+ (dnName.isEmpty()
        ? "UndefinedDataNodeName"+ ThreadLocalRandom.current().nextInt()
            : dnName.replace(':', '-'));

    // 从配置读取分位数统计间隔，默认不开启
    int[] intervals = 
        conf.getInts(DFSConfigKeys.DFS_METRICS_PERCENTILES_INTERVALS_KEY);
    
    return ms.register(name, null, new DataNodeMetrics(name, sessionId,
        intervals, jm));
  }

  /**
   * 获取当前指标实例的名称
   * @return 指标名称
   */
  public String name() { return name; }

  /**
   * 获取关联的JVM指标实例
   * @return JVM指标实例
   */
  public JvmMetrics getJvmMetrics() {
    return jvmMetrics;
  }

  /**
   * 记录一次心跳RPC的延迟，并更新NameNode RPC延迟聚合指标
   * @param latency 心跳处理延迟（毫秒）
   * @param rpcMetricSuffix RPC指标后缀
   */
  public void addHeartbeat(long latency, String rpcMetricSuffix) {
    heartbeats.add(latency);
    if (rpcMetricSuffix != null) {
      nnRpcLatency.add("HeartbeatsFor" + rpcMetricSuffix, latency);
    }
  }

  /**
   * 记录一次心跳总延迟，并更新NameNode RPC延迟聚合指标
   * @param latency 心跳处理延迟（毫秒）
   * @param rpcMetricSuffix RPC指标后缀
   */
  public void addHeartbeatTotal(long latency, String rpcMetricSuffix) {
    heartbeatsTotal.add(latency);
    if (rpcMetricSuffix != null) {
      nnRpcLatency.add("HeartbeatsTotalFor" + rpcMetricSuffix, latency);
    }
  }

  /**
   * 记录一次生命线RPC的延迟，并更新NameNode RPC延迟聚合指标
   * @param latency 生命线处理延迟（毫秒）
   * @param rpcMetricSuffix RPC指标后缀
   */
  public void addLifeline(long latency, String rpcMetricSuffix) {
    lifelines.add(latency);
    if (rpcMetricSuffix != null) {
      nnRpcLatency.add("LifelinesFor" + rpcMetricSuffix, latency);
    }
  }

  /**
   * 记录一次块报告RPC