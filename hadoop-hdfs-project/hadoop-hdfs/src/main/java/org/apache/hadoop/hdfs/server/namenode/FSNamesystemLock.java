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

package org.apache.hadoop.hdfs.server.namenode;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.log.LogThrottlingHelper;
import org.apache.hadoop.metrics2.lib.MutableRatesWithAggregation;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.Timer;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_LOCK_SUPPRESS_WARNING_INTERVAL_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_LOCK_SUPPRESS_WARNING_INTERVAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_FSLOCK_FAIR_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_FSLOCK_FAIR_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_LOCK_DETAILED_METRICS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_LOCK_DETAILED_METRICS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_READ_LOCK_REPORTING_THRESHOLD_MS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_READ_LOCK_REPORTING_THRESHOLD_MS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_WRITE_LOCK_REPORTING_THRESHOLD_MS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_WRITE_LOCK_REPORTING_THRESHOLD_MS_KEY;
import static org.apache.hadoop.ipc.ProcessingDetails.Timing;
import static org.apache.hadoop.log.LogThrottlingHelper.LogAction;

/**
 * FSNamesystem读写锁封装，扩展了ReentrantReadWriteLock，增加锁持有时间监控、慢锁日志输出和详细指标统计能力
 * 用于NameNode的FSNamesystem同步控制，能够帮助定位锁等待时间过长导致的性能问题。
 * 当开启详细指标后，会为每个获取锁的操作记录锁持有时间，指标格式为${LockName}(Read|Write)LockNanosOperationName
 */
public class FSNamesystemLock {
  @VisibleForTesting
  protected ReentrantReadWriteLock coarseLock;
  private final String lockName;

  private volatile boolean metricsEnabled;
  private final MutableRatesWithAggregation detailedHoldTimeMetrics;
  private final Timer timer;

  /**
   * 长锁持有警告日志的最小输出间隔，防止日志被刷屏
   */
  private final long lockSuppressWarningIntervalMs;

  /** 写锁慢锁报告阈值（毫秒），超过该阈值会触发日志记录 */
  private volatile long writeLockReportingThresholdMs;
  /** 写锁开始持有时间戳（纳秒），可重入场景只记录最外层第一次获取锁的时间 */
  private long writeLockHeldTimeStampNanos;
  /** 写锁长持有警告日志频率限制器 */
  private final LogThrottlingHelper writeLockReportLogger;

  /** 读锁慢锁报告阈值（毫秒），超过该阈值会触发日志记录 */
  private volatile long readLockReportingThresholdMs;
  /**
   * 读锁开始持有时间戳（纳秒），使用ThreadLocal存储支持多线程并发获取读锁
   * 可重入场景只记录最外层第一次获取锁的时间
   */
  private final ThreadLocal<Long> readLockHeldTimeStampNanos =
      new ThreadLocal<Long>() {
        @Override
        public Long initialValue() {
          return Long.MAX_VALUE;
        }
      };
  /** 已被抑制的读锁警告计数 */
  private final AtomicInteger numReadLockWarningsSuppressed =
      new AtomicInteger(0);
  /** 上次输出读锁警告日志的时间戳（毫秒） */
  private final AtomicLong timeStampOfLastReadLockReportMs = new AtomicLong(0);
  /**
   * 上次报告以来，持有时间最长的读锁信息（持有时间、堆栈、操作名）
   */
  private final AtomicReference<LockHeldInfo> longestReadLockHeldInfo =
      new AtomicReference<>(new LockHeldInfo());
  /** 上次报告以来，持有时间最长的写锁信息 */
  private LockHeldInfo longestWriteLockHeldInfo = new LockHeldInfo();
  /**
   * 读锁持有时间超过阈值的总次数
   */
  private final LongAdder numReadLockLongHold = new LongAdder();
  /**
   * 写锁持有时间超过阈值的总次数
   */
  private final LongAdder numWriteLockLongHold = new LongAdder();

  @VisibleForTesting
  static final String OP_NAME_OTHER = "OTHER";
  private final String readLockMetricPrefix;
  private final String writeLockMetricPrefix;
  private static final String LOCK_METRIC_SUFFIX = "Nanos";

  private static final String OVERALL_METRIC_NAME = "Overall";

  /**
   * 构造FSNamesystem锁，使用默认定时器
   * @param conf Hadoop配置对象
   * @param lockName 锁名称，用于指标和日志标识
   * @param detailedHoldTimeMetrics 用于存储锁持有时间指标的聚合对象
   */
  public FSNamesystemLock(Configuration conf, String lockName,
      MutableRatesWithAggregation detailedHoldTimeMetrics) {
    this(conf, lockName, detailedHoldTimeMetrics, new Timer());
  }

  @VisibleForTesting
  FSNamesystemLock(Configuration conf, String lockName,
      MutableRatesWithAggregation detailedHoldTimeMetrics, Timer timer) {
    this.lockName = lockName;
    this.readLockMetricPrefix = this.lockName + "ReadLock";
    this.writeLockMetricPrefix = this.lockName + "WriteLock";
    // 从配置读取锁是否使用公平模式
    boolean fair = conf.getBoolean(DFS_NAMENODE_FSLOCK_FAIR_KEY,
        DFS_NAMENODE_FSLOCK_FAIR_DEFAULT);
    FSNamesystem.LOG.info("{}Lock is fair: {}.", this.lockName, fair);
    this.coarseLock = new ReentrantReadWriteLock(fair);
    this.timer = timer;

    // 读取配置初始化各阈值
    this.writeLockReportingThresholdMs = conf.getLong(
        DFS_NAMENODE_WRITE_LOCK_REPORTING_THRESHOLD_MS_KEY,
        DFS_NAMENODE_WRITE_LOCK_REPORTING_THRESHOLD_MS_DEFAULT);
    this.readLockReportingThresholdMs = conf.getLong(
        DFS_NAMENODE_READ_LOCK_REPORTING_THRESHOLD_MS_KEY,
        DFS_NAMENODE_READ_LOCK_REPORTING_THRESHOLD_MS_DEFAULT);
    this.lockSuppressWarningIntervalMs = conf.getTimeDuration(
        DFS_LOCK_SUPPRESS_WARNING_INTERVAL_KEY,
        DFS_LOCK_SUPPRESS_WARNING_INTERVAL_DEFAULT, TimeUnit.MILLISECONDS);
    this.writeLockReportLogger =
        new LogThrottlingHelper(lockSuppressWarningIntervalMs);
    // 读取是否开启详细指标配置
    this.metricsEnabled = conf.getBoolean(
        DFS_NAMENODE_LOCK_DETAILED_METRICS_KEY,
        DFS_NAMENODE_LOCK_DETAILED_METRICS_DEFAULT);
    FSNamesystem.LOG.info("Detailed lock hold time metrics of {}Lock is {}.",
        this.lockName, this.metricsEnabled ? "enabled" : "disabled");
    this.detailedHoldTimeMetrics = detailedHoldTimeMetrics;
  }

  /**
   * 获取读锁（不响应中断）
   */
  public void readLock() {
    doLock(false);
  }

  /**
   * 获取可中断的读锁
   * @throws InterruptedException 获取锁过程中被中断则抛出异常
   */
  public void readLockInterruptibly() throws InterruptedException {
    doLockInterruptibly(false);
  }

  /**
   * 释放读锁，使用默认操作名OTHER
   */
  public void readUnlock() {
    readUnlock(OP_NAME_OTHER, null);
  }

  /**
   * 释放读锁，指定操作名
   * @param opName 当前操作名称
   */
  public void readUnlock(String opName) {
    readUnlock(opName, null);
  }

  /**
   * 释放读锁，指定操作名和额外报告信息
   * @param opName 当前操作名称
   * @param lockReportInfoSupplier 额外报告信息提供者
   */
  public void readUnlock(String opName,
      Supplier<String> lockReportInfoSupplier) {
    // 只有最外层释放锁时需要统计报告
    final boolean needReport = coarseLock.getReadHoldCount() == 1;
    // 计算锁持有时间
    final long readLockIntervalNanos =
        timer.monotonicNowNanos() - readLockHeldTimeStampNanos.get();
    final long currentTimeMs = timer.now();
    // 执行锁释放
    coarseLock.readLock().unlock();

    if (needReport) {
      // 添加锁持有时间指标
      addMetric(opName, readLockIntervalNanos, false);
      // 移除ThreadLocal中的时间戳防止内存泄漏
      readLockHeldTimeStampNanos.remove();
    }
    // 转换为毫秒便于阈值比较
    final long readLockIntervalMs =
        TimeUnit.NANOSECONDS.toMillis(readLockIntervalNanos);
    // 需要报告且持有时间超过阈值
    if (needReport && readLockIntervalMs >= this.readLockReportingThresholdMs) {
      numReadLockLongHold.increment();
      String lockReportInfo = null;
      boolean done = false;
      // CAS更新最长读锁信息
      while (!done) {
        LockHeldInfo localLockHeldInfo = longestReadLockHeldInfo.get();
        // 当前持有时间更长才更新
        if (localLockHeldInfo.getIntervalMs() <= readLockIntervalMs) {
          if (lockReportInfo == null) {
            lockReportInfo = lockReportInfoSupplier != null ? " (" +
                lockReportInfoSupplier.get() + ")" : "";
          }
          // CAS尝试更新
          if (longestReadLockHeldInfo.compareAndSet(localLockHeldInfo,
              new LockHeldInfo(currentTimeMs, readLockIntervalMs,
              StringUtils.getStackTrace(Thread.currentThread()), opName,
              lockReportInfo))) {
            done = true;
          }
        } else {
          done = true;
        }
      }

      long localTimeStampOfLastReadLockReport;
      long nowMs;
      // 检查是否可以输出日志（受间隔限制）
      do {
        nowMs = timer.monotonicNow();
        localTimeStampOfLastReadLockReport =
            timeStampOfLastReadLockReportMs.get();
        // 距离上次输出时间小于间隔，则抑制本次输出
        if (nowMs - localTimeStampOfLastReadLockReport <
            lockSuppressWarningIntervalMs) {
          numReadLockWarningsSuppressed.incrementAndGet();
          return;
        }
      } while (!timeStampOfLastReadLockReportMs.compareAndSet(
          localTimeStampOfLastReadLockReport, nowMs));
      // 获取并重置抑制计数和最长读锁信息
      int numSuppressedWarnings = numReadLockWarningsSuppressed.getAndSet(0);
      LockHeldInfo lockHeldInfo =
          longestReadLockHeldInfo.getAndSet(new LockHeldInfo());
      // 输出长读锁警告日志
      FSNamesystem.LOG.info(
          "\tNumber of suppressed read-lock reports of {}Lock is {}"
              + "\n\tLongest read-lock held at {} for {}ms by {}{} via {}",
          this.lockName, numSuppressedWarnings, Time.formatTime(lockHeldInfo.getStartTimeMs()),
          lockHeldInfo.getIntervalMs(), lockHeldInfo.getOpName(),
          lockHeldInfo.getLockReportInfo(), lockHeldInfo.getStackTrace());
    }
  }
  
  /**
   * 获取写锁（不响应中断）
   */
  public void writeLock() {
    doLock(true);
  }

  /**
   * 获取可中断的写锁
   * @throws InterruptedException 获取锁过程中被中断则抛出异常
   */
  public void writeLockInterruptibly() throws InterruptedException {
    doLockInterruptibly(true);
  }

  /**
   * 释放写锁，使用默认参数
   */
  public void writeUnlock() {
    writeUnlock(OP_NAME_OTHER, false, null);
  }

  /**
   * 释放写锁，指定操作名
   * @param opName 当前操作名称
   */
  public void writeUnlock(String opName) {
    writeUnlock(opName, false, null);
  }

  /**
   * 释放写锁，指定操作名和额外报告信息
   * @param opName 当前操作名称
   * @param lockReportInfoSupplier 额外报告信息提供者
   */
  public void writeUnlock(String opName,
      Supplier<String> lockReportInfoSupplier) {
    writeUnlock(opName, false, lockReportInfoSupplier);
  }

  /**
   * 释放写锁，指定操作名和是否抑制报告
   * @param opName 当前操作名称
   * @param suppressWriteLockReport 是否抑制长写锁报告
   */
  public void writeUnlock(String opName, boolean suppressWriteLockReport) {
    writeUnlock(opName, suppressWriteLockReport, null);
  }

  /**
   * 释放写锁，完整参数版本，处理长锁检测和指标记录
   * @param opName 当前操作名称
   * @param suppressWriteLockReport 是否抑制长写锁报告，true则不记录日志和指标
   * @param lockReportInfoSupplier 额外报告信息提供者
   */
  private void writeUnlock(String opName, boolean suppressWriteLockReport,
      Supplier<String> lockReportInfoSupplier) {
    // 只有最外层释放且不抑制报告且当前线程持有写锁才需要统计
    final boolean needReport = !suppressWriteLockReport && coarseLock
        .getWriteHoldCount() == 1 && coarseLock.isWriteLockedByCurrentThread();
    // 计算锁持有时间
    final long writeLockIntervalNanos =
        timer.monotonicNowNanos() - writeLockHeldTimeStampNanos;
    final long currentTimeMs = timer.now();
    final long writeLockIntervalMs =
        TimeUnit.NANOSECONDS.toMillis(writeLockIntervalNanos);

    LogAction logAction = LogThrottlingHelper.DO_NOT_LOG;
    if (needReport &&
        writeLockIntervalMs >= this.writeLockReportingThresholdMs) {
      numWriteLockLongHold.increment();
      // 更新最长写锁信息
      if (longestWriteLockHeldInfo.getIntervalMs() <= writeLockIntervalMs) {
        String lockReportInfo = lockReportInfoSupplier != null ? " (" +
            lockReportInfoSupplier.get() + ")" : "";
        longestWriteLockHeldInfo = new LockHeldInfo(currentTimeMs,
            writeLockIntervalMs,
            StringUtils.getStackTrace(Thread.currentThread()), opName,
            lockReportInfo);
      }

      // 通过频率限制器判断是否可以输出日志
      logAction = writeLockReportLogger
          .record("write", currentTimeMs, writeLockIntervalMs);
    }

    LockHeldInfo lockHeldInfo = longestWriteLockHeldInfo;
    if (logAction.shouldLog()) {
      // 重置最长写锁信息，等待下一轮收集
      longestWriteLockHeldInfo = new LockHeldInfo();
    }

    // 执行写锁释放
    coarseLock.writeLock().unlock();

    if (needReport) {
      // 添加锁持有时间指标
      addMetric(opName, writeLockIntervalNanos, true);
    }

    if (logAction.shouldLog()) {
      // 输出长写锁警告日志
      FSNamesystem.LOG.info(
          "\tNumber of suppressed write-lock reports of {}Lock is {}"
              + "\n\tLongest write-lock held at {} for {}ms by {}{} via {}"
              + "\n\tTotal suppressed write-lock held time: {}",
          this.lockName, logAction.getCount() - 1,
          Time.formatTime(lockHeldInfo.getStartTimeMs()),
          lockHeldInfo.getIntervalMs(), lockHeldInfo.getOpName(),
          lockHeldInfo.getLockReportInfo(), lockHeldInfo.getStackTrace(),
          logAction.getStats(0).getSum() - lockHeldInfo.getInterval