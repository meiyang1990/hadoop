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
package org.apache.hadoop.hdfs.server.namenode.metrics;

import static org.apache.hadoop.metrics2.impl.MsInfo.ProcessName;
import static org.apache.hadoop.metrics2.impl.MsInfo.SessionId;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NamenodeRole;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableGaugeInt;
import org.apache.hadoop.metrics2.lib.MutableQuantiles;
import org.apache.hadoop.metrics2.lib.MutableRate;
import org.apache.hadoop.metrics2.lib.MutableStat;
import org.apache.hadoop.metrics2.source.JvmMetrics;

/**
 * 文件级注释：NameNode运行指标统计类，维护NameNode各类活动的统计数据，并通过Hadoop metrics2框架对外发布指标，用于监控NameNode运行状态。
 *
 * This class is for maintaining  the various NameNode activity statistics
 * and publishing them through the metrics interfaces.
 */
/**
 * 类级注释：NameNode活动指标收集与发布类，负责统计NameNode各类操作、延迟、资源状态等运行指标，通过metrics2框架对外暴露，为监控系统提供NameNode运行数据。
 */
@Metrics(name="NameNodeActivity", about="NameNode metrics", context="dfs")
public class NameNodeMetrics {
  final MetricsRegistry registry = new MetricsRegistry("namenode");

  @Metric MutableCounterLong createFileOps;
  @Metric MutableCounterLong filesCreated;
  @Metric MutableCounterLong filesAppended;
  @Metric MutableCounterLong getBlockLocations;
  @Metric MutableCounterLong filesRenamed;
  @Metric MutableCounterLong filesTruncated;
  @Metric MutableCounterLong getListingOps;
  @Metric MutableCounterLong deleteFileOps;
  @Metric("Number of files/dirs deleted by delete or rename operations")
  MutableCounterLong filesDeleted;
  @Metric MutableCounterLong fileInfoOps;
  @Metric MutableCounterLong addBlockOps;
  @Metric MutableCounterLong getAdditionalDatanodeOps;
  @Metric MutableCounterLong createSymlinkOps;
  @Metric MutableCounterLong getLinkTargetOps;
  @Metric MutableCounterLong filesInGetListingOps;
  @Metric ("Number of successful re-replications")
  MutableCounterLong successfulReReplications;
  @Metric ("Number of times we failed to schedule a block re-replication.")
  MutableCounterLong numTimesReReplicationNotScheduled;
  @Metric("Number of timed out block re-replications")
  MutableCounterLong timeoutReReplications;
  @Metric("Number of allowSnapshot operations")
  MutableCounterLong allowSnapshotOps;
  @Metric("Number of disallowSnapshot operations")
  MutableCounterLong disallowSnapshotOps;
  @Metric("Number of createSnapshot operations")
  MutableCounterLong createSnapshotOps;
  @Metric("Number of deleteSnapshot operations")
  MutableCounterLong deleteSnapshotOps;
  @Metric("Number of renameSnapshot operations")
  MutableCounterLong renameSnapshotOps;
  @Metric("Number of listSnapshottableDirectory operations")
  MutableCounterLong listSnapshottableDirOps;
  @Metric("Number of listSnapshots operations")
  MutableCounterLong listSnapshotOps;
  @Metric("Number of snapshotDiffReport operations")
  MutableCounterLong snapshotDiffReportOps;
  @Metric("Number of blockReceivedAndDeleted calls")
  MutableCounterLong blockReceivedAndDeletedOps;
  @Metric("Number of blockReports and blockReceivedAndDeleted queued")
  MutableGaugeInt blockOpsQueued;
  @Metric("Number of blockReports and blockReceivedAndDeleted batch processed")
  MutableCounterLong blockOpsBatched;
  @Metric("Number of pending edits")
  MutableGaugeInt pendingEditsCount;
  @Metric("Number of delete blocks Queued")
  MutableGaugeInt deleteBlocksQueued;
  @Metric("Number of pending deletion blocks")
  MutableGaugeInt pendingDeleteBlocksCount;

  @Metric("Number of file system operations")
  public long totalFileOps(){
    return
      getBlockLocations.value() +
      createFileOps.value() +
      filesAppended.value() +
      addBlockOps.value() +
      getAdditionalDatanodeOps.value() +
      filesRenamed.value() +
      filesTruncated.value() +
      deleteFileOps.value() +
      getListingOps.value() +
      fileInfoOps.value() +
      getLinkTargetOps.value() +
      createSnapshotOps.value() +
      deleteSnapshotOps.value() +
      allowSnapshotOps.value() +
      disallowSnapshotOps.value() +
      renameSnapshotOps.value() +
      listSnapshottableDirOps.value() +
      listSnapshotOps.value() +
      createSymlinkOps.value() +
      snapshotDiffReportOps.value();
  }


  @Metric("Journal transactions") MutableRate transactions;
  @Metric("Journal syncs") MutableRate syncs;
  final MutableQuantiles[] syncsQuantiles;
  @Metric("Journal transactions batched in sync")
  MutableCounterLong transactionsBatchedInSync;
  @Metric("Journal transactions batched in sync")
  final MutableQuantiles[] numTransactionsBatchedInSync;
  @Metric("Number of blockReports from individual storages")
  MutableRate storageBlockReport;
  final MutableQuantiles[] storageBlockReportQuantiles;
  @Metric("Cache report") MutableRate cacheReport;
  final MutableQuantiles[] cacheReportQuantiles;
  @Metric("Generate EDEK time") private MutableRate generateEDEKTime;
  private final MutableQuantiles[] generateEDEKTimeQuantiles;
  @Metric("Warm-up EDEK time") private MutableRate warmUpEDEKTime;
  private final MutableQuantiles[] warmUpEDEKTimeQuantiles;
  @Metric("Resource check time") private MutableRate resourceCheckTime;
  private final MutableQuantiles[] resourceCheckTimeQuantiles;

  @Metric("Duration in SafeMode at startup in msec")
  MutableGaugeInt safeModeTime;
  @Metric("Time loading FS Image at startup in msec")
  MutableGaugeInt fsImageLoadTime;

  @Metric("Time tailing edit logs in msec")
  MutableRate editLogTailTime;
  private final MutableQuantiles[] editLogTailTimeQuantiles;
  @Metric MutableRate editLogFetchTime;
  private final MutableQuantiles[] editLogFetchTimeQuantiles;
  @Metric(value = "Number of edits loaded", valueName = "Count")
  MutableStat numEditLogLoaded;
  private final MutableQuantiles[] numEditLogLoadedQuantiles;
  @Metric("Time between edit log tailing in msec")
  MutableRate editLogTailInterval;
  private final MutableQuantiles[] editLogTailIntervalQuantiles;

  @Metric("GetImageServlet getEdit")
  MutableRate getEdit;
  @Metric("GetImageServlet getImage")
  MutableRate getImage;
  @Metric("GetImageServlet getAliasMap")
  MutableRate getAliasMap;
  @Metric("GetImageServlet putImage")
  MutableRate putImage;

  JvmMetrics jvmMetrics = null;
  
  /**
   * 构造方法：初始化NameNodeMetrics，创建各个百分位数统计对象
   * @param processName NameNode进程角色名称（如active/standby）
   * @param sessionId 指标会话ID
   * @param intervals 百分位数统计时间间隔数组
   * @param jvmMetrics JVM指标对象
   */
  NameNodeMetrics(String processName, String sessionId, int[] intervals,
      final JvmMetrics jvmMetrics) {
    this.jvmMetrics = jvmMetrics;
    // 注册进程名称和会话ID标签
    registry.tag(ProcessName, processName).tag(SessionId, sessionId);
    
    final int len = intervals.length;
    syncsQuantiles = new MutableQuantiles[len];
    numTransactionsBatchedInSync = new MutableQuantiles[len];
    storageBlockReportQuantiles = new MutableQuantiles[len];
    cacheReportQuantiles = new MutableQuantiles[len];
    generateEDEKTimeQuantiles = new MutableQuantiles[len];
    warmUpEDEKTimeQuantiles = new MutableQuantiles[len];
    resourceCheckTimeQuantiles = new MutableQuantiles[len];
    editLogTailTimeQuantiles = new MutableQuantiles[len];
    editLogFetchTimeQuantiles = new MutableQuantiles[len];
    numEditLogLoadedQuantiles = new MutableQuantiles[len];
    editLogTailIntervalQuantiles = new MutableQuantiles[len];

    // 遍历每个时间间隔，创建对应的百分位数统计对象
    for (int i = 0; i < len; i++) {
      int interval = intervals[i];
      syncsQuantiles[i] = registry.newQuantiles(
          "syncs" + interval + "s",
          "Journal syncs", "ops", "latency", interval);
      numTransactionsBatchedInSync[i] = registry.newQuantiles(
          "numTransactionsBatchedInSync" + interval + "s",
          "Number of Transactions batched in sync", "ops",
          "count", interval);
      storageBlockReportQuantiles[i] = registry.newQuantiles(
          "storageBlockReport" + interval + "s",
          "Storage block report", "ops", "latency", interval);
      cacheReportQuantiles[i] = registry.newQuantiles(
          "cacheReport" + interval + "s",
          "Cache report", "ops", "latency", interval);
      generateEDEKTimeQuantiles[i] = registry.newQuantiles(
          "generateEDEKTime" + interval + "s",
          "Generate EDEK time", "ops", "latency", interval);
      warmUpEDEKTimeQuantiles[i] = registry.newQuantiles(
          "warmupEDEKTime" + interval + "s",
          "Warm up EDEK time", "ops", "latency", interval);
      resourceCheckTimeQuantiles[i] = registry.newQuantiles(
          "resourceCheckTime" + interval + "s",
          "resource check time", "ops", "latency", interval);
      editLogTailTimeQuantiles[i] = registry.newQuantiles(
          "editLogTailTime" + interval + "s",
          "Edit log tailing time", "ops", "latency", interval);
      editLogFetchTimeQuantiles[i] = registry.newQuantiles(
          "editLogFetchTime" + interval + "s",
          "Edit log fetch time", "ops", "latency", interval);
      numEditLogLoadedQuantiles[i] = registry.newQuantiles(
          "numEditLogLoaded" + interval + "s",
          "Number of edits loaded", "ops", "count", interval);
      editLogTailIntervalQuantiles[i] = registry.newQuantiles(
          "editLogTailInterval" + interval + "s",
          "Edit log tailing interval", "ops", "latency", interval);
    }
  }

  /**
   * 创建并注册NameNode指标对象到指标系统
   * @param conf Hadoop配置对象
   * @param r NameNode角色（active/standby等）
   * @return 初始化完成的NameNodeMetrics实例
   */
  public static NameNodeMetrics create(Configuration conf, NamenodeRole r) {
    String sessionId = conf.get(DFSConfigKeys.DFS_METRICS_SESSION_ID_KEY);
    String processName = r.toString();
    MetricsSystem ms = DefaultMetricsSystem.instance();
    JvmMetrics jm = JvmMetrics.create(processName, sessionId, ms);
    
    // 从配置获取百分位数统计间隔，默认关闭（空数组）
    int[] intervals = 
        conf.getInts(DFSConfigKeys.DFS_METRICS_PERCENTILES_INTERVALS_KEY);
    // 注册并返回NameNode指标对象
    return ms.register(new NameNodeMetrics(processName, sessionId,
        intervals, jm));
  }

  /**
   * 获取关联的JVM指标对象
   * @return JVM指标实例
   */
  public JvmMetrics getJvmMetrics() {
    return jvmMetrics;
  }
  
  /**
   * 关闭指标系统，清理资源
   */
  public void shutdown() {
    DefaultMetricsSystem.shutdown();
  }

  /**
   * 增加获取块位置操作计数
   */
  public void incrGetBlockLocations() {
    getBlockLocations.incr();
  }

  /**
   * 增加创建文件计数
   */
  public void incrFilesCreated() {
    filesCreated.incr();
  }

  /**
   * 增加创建文件操作计数
   */
  public void incrCreateFileOps() {
    createFileOps.incr();
  }

  /**
   * 增加文件追加操作计数
   */
  public void incrFilesAppended() {
    filesAppended.incr();
  }

  /**
   * 增加添加块操作计数
   */
  public void incrAddBlockOps() {
    addBlockOps.incr();
  }
  
  /**
   * 增加获取额外DataNode操作计数
   */
  public void incrGetAdditionalDatanodeOps() {
    getAdditionalDatanodeOps.incr();
  }

  /**
   * 增加文件重命名操作计数
   */
  public void incrFilesRenamed() {
    filesRenamed.incr();
  }

  /**
   * 增加文件截断操作计数
   */
  public void incrFilesTruncated() {
    filesTruncated.incr();
  }

  /**
   * 增加删除文件/目录数量
   * @param delta 新增删除数量
   */
  public void incrFilesDeleted(long delta) {
    filesDeleted.incr(delta);
  }

  /**
   * 增加删除文件操作计数
   */
  public void incrDeleteFileOps() {
    deleteFileOps.incr();
  }

  /**
   * 增加获取文件列表操作计数
   */
  public void incrGetListingOps() {
    getListingOps.incr();
  }

  /**
   * 增加getListing操作返回的文件总数
   * @param delta 新增文件数量
   */
  public void incrFilesInGetListingOps(int delta) {
    filesInGetListingOps.incr(delta);
  }

  /**
   * 增加获取文件信息操作计数
   */
  public void incrFileInfoOps() {
    fileInfoOps.incr();
  }

  /**
   * 增加创建符号链接操作计数
   */
  public void incrCreateSymlinkOps() {
    createSymlinkOps.incr();
  }

  /**
   * 增加获取符号链接目标操作计数
   */
  public void incrGetLinkTargetOps() {
    getLinkTargetOps.incr();
  }

  /**
   * 增加允许快照操作计数
   */
  public void incrAllowSnapshotOps() {
    allowSnapshotOps.incr();
  }
  
  /**
   * 增加禁止快照操作计数
   */
  public void incrDisAllowSnapshotOps() {
    disallowSnapshotOps.incr();
  }
  
  /**
   * 增加创建快照操作计数
   */
  public void incrCreateSnapshotOps() {
    createSnapshotOps.incr();
  }
  
  /**
   * 增加删除快照操作计数
   */
  public void incrDeleteSnapshotOps() {
    deleteSnapshotOps.incr();
  }
  
  /**
   * 增加重命名快照操作计数
   */
  public void incrRenameSnapshotOps() {
    renameSnapshotOps.incr();
  }
  
  /**
   * 增加获取可快照目录列表操作计数
   */
  public void incrListSnapshottableDirOps() {
    listSnapshottableDirOps.incr();
  }

  /**
   * 增加获取快照列表操作计数
   */
  public void incrListSnapshotsOps() {
    listSnapshotOps.incr();
  }
  
  /**
   * 增加快照差异报告操作计数
   */
  public void incrSnapshotDiffReportOps() {
    snapshotDiffReportOps.incr();
  }
  
  /**
   * 增加块接收删除通知操作计数
   */
  public void inc