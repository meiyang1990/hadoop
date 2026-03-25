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

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.cache.Cache;
import org.apache.hadoop.thirdparty.com.google.common.cache.CacheBuilder;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockLocalPathInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.datanode.BlockScanner.Conf;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeReference;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi.BlockIterator;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.datanode.metrics.DataNodeMetrics;
import org.apache.hadoop.hdfs.util.DataTransferThrottler;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.concurrent.SubjectInheritingThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 文件说明：DataNode单个存储卷的块扫描器，每个VolumeScanner拥有独立线程执行扫描任务
 * 核心职责：负责扫描指定存储卷上的所有数据块，校验块数据完整性，优先扫描可疑块，由DataNode的BlockScanner统一管理
 */
public class VolumeScanner extends SubjectInheritingThread {
  public static final Logger LOG =
      LoggerFactory.getLogger(VolumeScanner.class);

  /**
   * 一分钟包含的秒数
   */
  private final static int SECONDS_PER_MINUTE = 60;

  /**
   * 一小时包含的分钟数
   */
  private final static int MINUTES_PER_HOUR = 60;

  /**
   * 当前扫描器使用的块迭代器名称
   */
  private final static String BLOCK_ITERATOR_NAME = "scanner";

  /**
   * 块扫描配置对象
   */
  private Conf conf;

  @VisibleForTesting
  void setConf(Conf conf) {
    this.conf = conf;
  }

  /**
   * 当前扫描器所属的DataNode
   */
  private final DataNode datanode;

  private final DataNodeMetrics metrics;

  /**
   * 当前扫描的存储卷引用
   */
  private final FsVolumeReference ref;

  /**
   * 当前扫描的存储卷实例
   */
  final FsVolumeSpi volume;

  /**
   * 过去一小时内每分钟扫描字节数的循环缓冲区
   * 数组每个元素对应一分钟，用于计算平均扫描速率，控制扫描速度不超过带宽限制
   */
  private final long scannedBytes[] = new long[MINUTES_PER_HOUR];

  /**
   * 所有scannedBytes元素的总和，即过去一小时总扫描字节数
   */
  private long scannedBytesSum = 0;

  /**
   * 块发送流使用的流量限速器，控制扫描速度
   */
  private final DataTransferThrottler throttler = new DataTransferThrottler(1);

  /**
   * 空输出流，扫描时仅读取校验不输出数据
   */
  private final DataOutputStream nullStream =
      new DataOutputStream(new IOUtils.NullOutputStream());

  /**
   * 当前扫描器关联的块迭代器列表，每个块池对应一个迭代器
   */
  private final List<BlockIterator> blockIters =
      new ArrayList<BlockIterator>();

  /**
   * 待扫描可疑块集合，扫描器优先处理这些块，保证错误块及时发现
   */
  private final LinkedHashSet<ExtendedBlock> suspectBlocks =
      new LinkedHashSet<ExtendedBlock>();

  /**
   * 最近已扫描过的可疑块缓存，避免重复扫描同一个可疑块
   */
  private final Cache<ExtendedBlock, Boolean> recentSuspectBlocks =
      CacheBuilder.newBuilder().maximumSize(1000)
        .expireAfterAccess(10, TimeUnit.MINUTES).build();

  /**
   * 当前使用的块迭代器，无可用迭代器时为null
   */
  private BlockIterator curBlockIter = null;

  /**
   * 线程停止标志，由当前对象锁保护
   */
  private boolean stopping = false;

  /**
   * 扫描器启动时间（单位：分钟，单调时间）
   */
  private long startMinute = 0;

  /**
   * 当前时间（单位：分钟，单调时间）
   */
  private long curMinute = 0;

  /**
   * 扫描结果处理器，处理扫描成功/失败结果
   */
  private final ScanResultHandler resultHandler;

  private final Statistics stats = new Statistics();

  /**
   * 扫描统计信息类，保存卷扫描器的各类统计数据
   */
  static class Statistics {
    long bytesScannedInPastHour = 0;
    long blocksScannedInCurrentPeriod = 0;
    long blocksScannedSinceRestart = 0;
    long scansSinceRestart = 0;
    long scanErrorsSinceRestart = 0;
    long nextBlockPoolScanStartMs = -1;
    long blockPoolPeriodEndsMs = -1;
    ExtendedBlock lastBlockScanned = null;
    boolean eof = false;

    Statistics() {
    }

    Statistics(Statistics other) {
      this.bytesScannedInPastHour = other.bytesScannedInPastHour;
      this.blocksScannedInCurrentPeriod = other.blocksScannedInCurrentPeriod;
      this.blocksScannedSinceRestart = other.blocksScannedSinceRestart;
      this.scansSinceRestart = other.scansSinceRestart;
      this.scanErrorsSinceRestart = other.scanErrorsSinceRestart;
      this.nextBlockPoolScanStartMs = other.nextBlockPoolScanStartMs;
      this.blockPoolPeriodEndsMs = other.blockPoolPeriodEndsMs;
      this.lastBlockScanned = other.lastBlockScanned;
      this.eof = other.eof;
    }

    @Override
    public String toString() {
      return new StringBuilder().
          append("Statistics{").
          append("bytesScannedInPastHour=").append(bytesScannedInPastHour).
          append(", blocksScannedInCurrentPeriod=").
              append(blocksScannedInCurrentPeriod).
          append(", blocksScannedSinceRestart=").
              append(blocksScannedSinceRestart).
          append(", scansSinceRestart=").append(scansSinceRestart).
          append(", scanErrorsSinceRestart=").append(scanErrorsSinceRestart).
          append(", nextBlockPoolScanStartMs=").append(nextBlockPoolScanStartMs).
          append(", blockPoolPeriodEndsMs=").append(blockPoolPeriodEndsMs).
          append(", lastBlockScanned=").append(lastBlockScanned).
          append(", eof=").append(eof).
          append("}").toString();
    }
  }

  /**
   * 将毫秒时间转换为小时单位，非正数输入返回0
   * @param ms 输入毫秒数
   * @return 转换后的小时数
   */
  private static double positiveMsToHours(long ms) {
    if (ms <= 0) {
      return 0;
    } else {
      return TimeUnit.HOURS.convert(ms, TimeUnit.MILLISECONDS);
    }
  }

  /**
   * 将当前卷扫描器统计信息输出到字符串构建器，用于WebUI展示
   * @param p 字符串构建器
   */
  public void printStats(StringBuilder p) {
    p.append(String.format("Block scanner information for volume %s with base" +
        " path %s%n", volume.getStorageID(), volume));
    synchronized (stats) {
      p.append(String.format("Bytes verified in last hour       : %57d%n",
          stats.bytesScannedInPastHour))
          .append(String.format("Blocks scanned in current period  : %57d%n",
              stats.blocksScannedInCurrentPeriod))
          .append(String.format("Blocks scanned since restart      : %57d%n",
              stats.blocksScannedSinceRestart))
          .append(String.format("Block pool scans since restart    : %57d%n",
              stats.scansSinceRestart))
          .append(String.format("Block scan errors since restart   : %57d%n",
              stats.scanErrorsSinceRestart));
      if (stats.nextBlockPoolScanStartMs > 0) {
        p.append(String.format("Hours until next block pool scan  : %57.3f%n",
            positiveMsToHours(stats.nextBlockPoolScanStartMs -
                Time.monotonicNow())));
      }
      if (stats.blockPoolPeriodEndsMs > 0) {
        p.append(String.format("Hours until possible pool rescan  : %57.3f%n",
            positiveMsToHours(stats.blockPoolPeriodEndsMs -
                Time.now())));
      }
      p.append(String.format("Last block scanned                : %57s%n",
          ((stats.lastBlockScanned == null) ? "none" :
          stats.lastBlockScanned.toString())));
      p.append(String.format("More blocks to scan in period     : %57s%n",
          !stats.eof));
      p.append(System.lineSeparator());
    }
  }

  /**
   * 扫描结果处理器，处理块扫描的结果，上报坏块给DataNode
   */
  static class ScanResultHandler {
    private VolumeScanner scanner;

    /**
     * 初始化处理器，关联对应的卷扫描器
     * @param scanner 卷扫描器实例
     */
    public void setup(VolumeScanner scanner) {
      LOG.trace("Starting VolumeScanner {}",
          scanner.volume);
      this.scanner = scanner;
    }

    /**
     * 处理单个块的扫描结果
     * @param block 被扫描的块
     * @param e 扫描异常，扫描成功时为null
     */
    public void handle(ExtendedBlock block, IOException e) {
      FsVolumeSpi volume = scanner.volume;
      if (e == null) {
        LOG.trace("Successfully scanned {} on {}", block, volume);
        return;
      }
      // 如果块已经不存在了，则不算错误
      if (!volume.getDataset().contains(block)) {
        LOG.debug("Volume {}: block {} is no longer in the dataset.",
            volume, block);
        return;
      }
      // 文件找不到异常可能是写竞争导致，忽略不处理
      if (e instanceof FileNotFoundException ) {
        LOG.info("Volume {}: verification failed for {} because of " +
                "FileNotFoundException.  This may be due to a race with write.",
            volume, block);
        return;
      }
      LOG.warn("Reporting bad {} on {}", block, volume, e);
      scanner.datanode.handleBadBlock(block, e, true);
    }
  }

  /**
   * 构造卷扫描器实例
   * @param conf 块扫描配置
   * @param datanode 所属DataNode
   * @param ref 扫描的存储卷引用
   */
  VolumeScanner(Conf conf, DataNode datanode, FsVolumeReference ref) {
    this.conf = conf;
    this.datanode = datanode;
    this.metrics = datanode.getMetrics();
    this.ref = ref;
    this.volume = ref.getVolume();
    ScanResultHandler handler;
    try {
      handler = conf.resultHandler.newInstance();
    } catch (Throwable e) {
      LOG.error("unable to instantiate {}", conf.resultHandler, e);
      handler = new ScanResultHandler();
    }
    this.resultHandler = handler;
    setName("VolumeScannerThread(" + volume + ")");
    setDaemon(true);
  }

  private void saveBlockIterator(BlockIterator iter) {
    try {
      iter.save();
    } catch (IOException e) {
      LOG.warn("{}: error saving {}.", this, iter, e);
    }
  }

  /**
   * 过期清理循环缓冲区中过时的扫描字节记录，计算当前分钟
   * @param monotonicMs 当前单调时间（毫秒）
   */
  private void expireOldScannedBytesRecords(long monotonicMs) {
    long newMinute =
        TimeUnit.MINUTES.convert(monotonicMs, TimeUnit.MILLISECONDS);
    if (curMinute == newMinute) {
      return;
    }
    // 清零所有过时分钟对应的缓冲区槽位
    for (long m = curMinute + 1; m <= newMinute; m++) {
      int slotIdx = (int)(m % MINUTES_PER_HOUR);
      LOG.trace("{}: updateScannedBytes is zeroing out slotIdx {}.  " +
              "curMinute = {}; newMinute = {}", this, slotIdx,
              curMinute, newMinute);
      scannedBytesSum -= scannedBytes[slotIdx];
      scannedBytes[slotIdx] = 0;
    }
    curMinute = newMinute;
  }

  /**
   * 查找下一个可扫描的块迭代器，按轮询顺序遍历所有块池
   * @return 如果找到可用迭代器返回0，否则返回需要等待的毫秒数
   */
  private synchronized long findNextUsableBlockIter() {
    int numBlockIters = blockIters.size();
    if (numBlockIters == 0) {
      LOG.debug("{}: no block pools are registered.", this);
      return Long.MAX_VALUE;
    }
    int curIdx;
    if (curBlockIter == null) {
      curIdx = 0;
    } else {
      curIdx = blockIters.indexOf(curBlockIter);
      Preconditions.checkState(curIdx >= 0);
    }
    // 这里必须使用墙钟时间，因为迭代器保存的起始时间是墙钟时间
    long nowMs = Time.now();
    long minTimeoutMs = Long.MAX_VALUE;
    for (int i = 0; i < numBlockIters; i++) {
      int idx = (curIdx + i + 1) % numBlockIters;
      BlockIterator iter = blockIters.get(idx);
      if (!iter.atEnd()) {
        LOG.info("Now scanning bpid {} on volume {}",
            iter.getBlockPoolId(), volume);
        curBlockIter = iter;
        return 0L;
      }
      long iterStartMs = iter.getIterStartMs();
      long waitMs = (iterStartMs + conf.scanPeriodMs) - nowMs;
      if (waitMs <= 0) {
        iter.rewind();
        LOG.info("Now rescanning bpid {} on volume {}, after more than " +
            "{} hour(s)", iter.getBlockPoolId(), volume,
            TimeUnit.HOURS.convert(conf.scanPeriodMs, TimeUnit.MILLISECONDS));
        curBlockIter = iter;
        return 0L;
      }
      minTimeoutMs = Math.min(minTimeoutMs, waitMs);
    }
    LOG.info("{}: no suitable block pools found to scan.  Waiting {} ms.",
        this, minTimeoutMs);
    return minTimeoutMs;
  }

  /**
   * 扫描单个块，校验数据完整性
   * @param cblock 待扫描块
   * @param bytesPerSec 扫描速率上限（字节/秒）
   * @return 扫描成功返回块字节数，扫描失败返回-1
   */
  private long scanBlock(ExtendedBlock cblock, long bytesPerSec) {
    // 从数据集获取块最新的生成时间戳信息
    ExtendedBlock block = null;
    try {
      Block b = volume.getDataset().getStoredBlock(
          cblock.getBlockPoolId(), cblock.getBlockId());
      if (b == null) {
        LOG.info("Replica {} was not found in the VolumeMap for volume {}",
            cblock, volume);
      } else {
        block = new ExtendedBlock(cblock.getBlockPoolId(), b);
      }
    } catch (FileNotFoundException e) {
      LOG.info("FileNotFoundException while finding block {} on volume {}",
          cblock, volume);
    } catch (IOException e) {
      LOG.warn("I/O error while finding block {} on volume {}",
            cblock, volume);
    }
    if (block == null) {
      return -1; // 未找到块