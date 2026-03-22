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
package org.apache.hadoop.hdfs.server.datanode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi.ScanInfo;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.collect.ArrayListMultimap;
import org.apache.hadoop.thirdparty.com.google.common.collect.ListMultimap;

/**
 * 数据节点目录扫描器，定期扫描数据目录下的块文件和元数据文件，
 * 并与内存中维护的块信息进行差异比对，修正不一致数据。
 * 是DataNode保证磁盘数据和内存元数据一致性的核心后台组件。
 */
@InterfaceAudience.Private
public class DirectoryScanner implements Runnable {
  private static final Logger LOG =
      LoggerFactory.getLogger(DirectoryScanner.class);

  private static final int DEFAULT_MAP_SIZE = 32768;
  private final int reconcileBlocksBatchSize;
  private final long reconcileBlocksBatchInterval;
  private final FsDatasetSpi<?> dataset;
  private final ExecutorService reportCompileThreadPool;
  private final ScheduledExecutorService masterThread;
  private final long scanPeriodMsecs;
  private final long throttleLimitMsPerSec;
  private final AtomicBoolean shouldRun = new AtomicBoolean();

  private boolean retainDiffs = false;

  /**
   * 报告编译线程总共花费的运行时间（毫秒），仅用于测试。
   */
  @VisibleForTesting
  final AtomicLong timeRunningMs = new AtomicLong(0L);

  /**
   * 报告编译线程总共花费的等待时间（毫秒），仅用于测试。
   */
  @VisibleForTesting
  final AtomicLong timeWaitingMs = new AtomicLong(0L);

  /**
   * 按块池ID索引的所有块差异完整列表。
   */
  @VisibleForTesting
  final BlockPoolReport diffs = new BlockPoolReport();

  /**
   * 按块池ID索引的每个块池的差异统计信息。
   */
  @VisibleForTesting
  final Map<String, Stats> stats;

  /**
   * 设置是否保留差异结果，用于单元测试和问题分析，默认关闭。
   *
   * @param b 是否保留差异
   */
  @VisibleForTesting
  public void setRetainDiffs(boolean b) {
    retainDiffs = b;
  }

  /**
   * 每个块池的扫描差异统计信息，用于日志输出和测试验证。
   */
  @VisibleForTesting
  static class Stats {
    final String bpid;
    long totalBlocks = 0;
    long missingMetaFile = 0;
    long missingBlockFile = 0;
    long missingMemoryBlocks = 0;
    long mismatchBlocks = 0;
    long duplicateBlocks = 0;

    /**
     * 为指定块池创建统计对象。
     *
     * @param bpid 块池ID
     */
    public Stats(String bpid) {
      this.bpid = bpid;
    }

    @Override
    public String toString() {
      return "BlockPool " + bpid + " Total blocks: " + totalBlocks
          + ", missing metadata files: " + missingMetaFile
          + ", missing block files: " + missingBlockFile
          + ", missing blocks in memory: " + missingMemoryBlocks
          + ", mismatched blocks: " + mismatchBlocks
          + ", duplicated blocks: " + duplicateBlocks;
    }
  }

  /**
   * 报告编译线程收集块信息的辅助类，保存一个存储卷的块扫描结果，
   * 按块池ID组织所有ScanInfo对象。
   */
  @VisibleForTesting
  public static class ScanInfoVolumeReport {

    @SuppressWarnings("unused")
    private static final long serialVersionUID = 1L;

    private final FsVolumeSpi volume;

    private final BlockPoolReport blockPoolReport;

    /**
     * 创建一个空的卷扫描结果对象。
     *
     * @param volume 目标存储卷
     */
    ScanInfoVolumeReport(final FsVolumeSpi volume) {
      this.volume = volume;
      this.blockPoolReport = new BlockPoolReport();
    }

    /**
     * 创建一个预分配了块池容量的卷扫描结果对象。
     *
     * @param volume 目标存储卷
     * @param blockPools 已知块池列表
     */
    ScanInfoVolumeReport(final FsVolumeSpi volume,
        final Collection<String> blockPools) {
      this.volume = volume;
      this.blockPoolReport = new BlockPoolReport(blockPools);
    }

    public void addAll(final String bpid,
        final Collection<ScanInfo> scanInfos) {
      this.blockPoolReport.addAll(bpid, scanInfos);
    }

    public Set<String> getBlockPoolIds() {
      return this.blockPoolReport.getBlockPoolIds();
    }

    public List<ScanInfo> getScanInfo(final String bpid) {
      return this.blockPoolReport.getScanInfo(bpid);
    }

    public FsVolumeSpi getVolume() {
      return volume;
    }

    @Override
    public String toString() {
      return "ScanInfoVolumeReport [volume=" + volume + ", blockPoolReport="
          + blockPoolReport + "]";
    }
  }

  /**
   * 按块池组织块扫描信息的辅助类，保存一个块池下所有扫描到的块信息。
   */
  @VisibleForTesting
  public static class BlockPoolReport {

    @SuppressWarnings("unused")
    private static final long serialVersionUID = 1L;

    private final Set<String> blockPools;

    private final ListMultimap<String, ScanInfo> map;

    /**
     * 创建一个空的块池报告对象。
     */
    BlockPoolReport() {
      this.blockPools = new HashSet<>(2);
      this.map = ArrayListMultimap.create(2, DEFAULT_MAP_SIZE);
    }

    /**
     * 创建一个预分配了块池容量的块池报告对象。
     *
     * @param blockPools 初始已知块池列表
     */
    BlockPoolReport(final Collection<String> blockPools) {
      this.blockPools = new HashSet<>(blockPools);
      this.map = ArrayListMultimap.create(blockPools.size(), DEFAULT_MAP_SIZE);

    }

    public void addAll(final String bpid,
        final Collection<ScanInfo> scanInfos) {
      this.blockPools.add(bpid);
      this.map.putAll(bpid, scanInfos);
    }

    /**
     * 对每个块池中的块按块ID排序，为后续双指针差异比对做准备。
     */
    public void sortBlocks() {
      for (final String bpid : this.map.keySet()) {
        final List<ScanInfo> list = this.map.get(bpid);
        // Sort array based on blockId
        Collections.sort(list);
      }
    }

    public Set<String> getBlockPoolIds() {
      return Collections.unmodifiableSet(this.blockPools);
    }

    public List<ScanInfo> getScanInfo(final String bpid) {
      return this.map.get(bpid);
    }

    public Collection<Map.Entry<String, ScanInfo>> getEntries() {
      return Collections.unmodifiableCollection(this.map.entries());
    }

    public void clear() {
      this.map.clear();
      this.blockPools.clear();
    }

    @Override
    public String toString() {
      return "BlockPoolReport [blockPools=" + blockPools + ", map=" + map + "]";
    }
  }

  /**
   * 构造目录扫描器实例，根据配置初始化参数，但不启动扫描任务。
   *
   * @param dataset 需要扫描的数据集对象
   * @param conf Hadoop配置对象
   */
  public DirectoryScanner(FsDatasetSpi<?> dataset, Configuration conf) {
    this.dataset = dataset;
    this.stats = new HashMap<>(DEFAULT_MAP_SIZE);
    int interval = (int) conf.getTimeDuration(
        DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_INTERVAL_KEY,
        DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_INTERVAL_DEFAULT,
        TimeUnit.SECONDS);

    scanPeriodMsecs = TimeUnit.SECONDS.toMillis(interval);

    int throttle = conf.getInt(
        DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_THROTTLE_LIMIT_MS_PER_SEC_KEY,
        DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_THROTTLE_LIMIT_MS_PER_SEC_DEFAULT);

    if (throttle >= TimeUnit.SECONDS.toMillis(1)) {
      LOG.warn(
          "{} set to value above 1000 ms/sec. Assuming default value of {}",
          DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_THROTTLE_LIMIT_MS_PER_SEC_KEY,
          DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_THROTTLE_LIMIT_MS_PER_SEC_DEFAULT);
      throttle =
          DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_THROTTLE_LIMIT_MS_PER_SEC_DEFAULT;
    }

    throttleLimitMsPerSec = throttle;

    int threads =
        conf.getInt(DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_THREADS_KEY,
            DFSConfigKeys.DFS_DATANODE_DIRECTORYSCAN_THREADS_DEFAULT);

    reportCompileThreadPool =
        Executors.newFixedThreadPool(threads, new Daemon.DaemonFactory());

    masterThread =
        new ScheduledThreadPoolExecutor(1, new Daemon.DaemonFactory());

    int reconcileBatchSize =
        conf.getInt(DFSConfigKeys.
                DFS_DATANODE_RECONCILE_BLOCKS_BATCH_SIZE,
            DFSConfigKeys.
                DFS_DATANODE_RECONCILE_BLOCKS_BATCH_SIZE_DEFAULT);

    if (reconcileBatchSize <= 0) {
      LOG.warn("Invalid value configured for " +
              "dfs.datanode.reconcile.blocks.batch.size, " +
              "should be greater than 0, Using default.");
      reconcileBatchSize =
          DFSConfigKeys.
              DFS_DATANODE_RECONCILE_BLOCKS_BATCH_SIZE_DEFAULT;
    }

    reconcileBlocksBatchSize = reconcileBatchSize;

    long reconcileBatchInterval =
        conf.getTimeDuration(DFSConfigKeys.
                DFS_DATANODE_RECONCILE_BLOCKS_BATCH_INTERVAL,
            DFSConfigKeys.
                DFS_DATANODE_RECONCILE_BLOCKS_BATCH_INTERVAL_DEFAULT,
            TimeUnit.MILLISECONDS);

    if (reconcileBatchInterval <= 0) {
      LOG.warn("Invalid value configured for " +
              "dfs.datanode.reconcile.blocks.batch.interval, " +
              "should be greater than 0, Using default.");
      reconcileBatchInterval =
          DFSConfigKeys.
              DFS_DATANODE_RECONCILE_BLOCKS_BATCH_INTERVAL_DEFAULT;
    }

    reconcileBlocksBatchInterval = reconcileBatchInterval;
  }

  /**
   * 启动周期目录扫描器，扫描任务将按照配置的间隔定期执行。
   */
  @VisibleForTesting
  public void start() {
    shouldRun.set(true);
    // 随机化首次扫描时间，避免所有DataNode同时启动扫描导致资源竞争
    long firstScanTime = ThreadLocalRandom.current().nextLong(scanPeriodMsecs);

    LOG.info(
        "Periodic Directory Tree Verification scan starting in {}ms with interval of {}ms and throttle limit of {}ms/s",
        firstScanTime, scanPeriodMsecs, throttleLimitMsPerSec);

    masterThread.scheduleAtFixedRate(this, firstScanTime, scanPeriodMsecs,
        TimeUnit.MILLISECONDS);
  }

  /**
   * 获取扫描器是否已启动的状态。
   *
   * @return 扫描器是否已启动
   */
  @VisibleForTesting
  boolean getRunStatus() {
    return shouldRun.get();
  }

  /**
   * 清空当前缓存的差异结果和统计信息。
   */
  private void clear() {
    synchronized (diffs) {
      diffs.clear();
    }
    stats.clear();
  }

  /**
   * 目录扫描器主循环方法，由定时任务调用，执行一次完整的差异比对与 reconcile，处理异常保证周期调度不中断。
   */
  @Override
  public void run() {
    if (!shouldRun.get()) {
      // 已经触发了关闭命令，直接终止本次周期任务
      LOG.warn(
          "This cycle terminating immediately because 'shouldRun' has been deactivated");
      return;
    }
    try {
      reconcile();
      dataset.setLastDirScannerFinishTime(System.currentTimeMillis());
    } catch (Exception e) {
      // 记录异常后继续，不影响下一个周期执行
      LOG.error(
          "Exception during DirectoryScanner execution - will continue next cycle",
          e);
    } catch (Error er) {
      // 不可恢复错误，记录后重新抛出，永久终止周期扫描
      LOG.error(
          "System Error during DirectoryScanner execution - permanently terminating periodic scanner",
          er);
      throw er;
    }
  }

  /**
   * 关闭目录扫描器，等待线程池退出，最长等待2分钟。
   */
  void shutdown() {
    LOG.info("Shutdown has been called");
    if (!shouldRun.getAndSet(false)) {
      LOG.warn("Shutdown has been called, but periodic scanner not started");
    }
    if (masterThread != null) {
      masterThread.shutdown();
    }
    if (reportCompileThreadPool != null) {
      reportCompileThreadPool.shutdownNow();
    }
    if (masterThread != null) {
      try {
        masterThread.awaitTermination(1, TimeUnit.MINUTES);
      } catch (InterruptedException e) {
        LOG.error(
            "interrupted while waiting for masterThread to " + "terminate", e);
      }
    }
    if (reportCompileThreadPool != null) {
      try {
        reportCompileThreadPool.awaitTermination(1, TimeUnit.MINUTES);
      } catch (InterruptedException e) {
        LOG.error("interrupted while waiting for reportCompileThreadPool to "
            + "terminate", e);
      }
    }
    if (!retainDiffs) {
      clear();
    }
  }

  /**
   *  reconcile磁盘和内存块信息的差异，修正不一致。
   * @throws IOException IO异常
   */
  @VisibleForTesting
  public void reconcile() throws IOException {
    LOG.debug("reconcile start DirectoryScanning");
    // 扫描磁盘生成差异列表
    scan();
    // 注入点，单元测试使用，等待存储移除操作完成
    DataNodeFaultInjector.get().waitUntilStorageRemoved();
    // 分批修改，避免长时间占用锁影响DataNode正常服务（HDFS-14476）
    int loopCount = 0;
    synchronized (diffs) {
      for (final Map.Entry<String, ScanInfo> entry : diffs.getEntries()) {
        // 调用数据集修正差异
        dataset.checkAndUpdate(entry.getKey(), entry.getValue());

        // 每处理完一批，休眠指定间隔，释放锁避免长时间阻塞
        if (loopCount % reconcileBlocksBatchSize == 0) {
          try {
            Thread.sleep(reconcileBlocksBatchInterval);
          } catch (InterruptedException e) {
            // 中断不处理，继续执行
          }
        }
        loopCount++;