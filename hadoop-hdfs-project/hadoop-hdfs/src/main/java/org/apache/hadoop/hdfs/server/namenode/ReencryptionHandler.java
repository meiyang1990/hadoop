// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension.EncryptedKeyVersion;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.ReencryptionStatus;
import org.apache.hadoop.hdfs.protocol.ZoneReencryptionStatus;
import org.apache.hadoop.hdfs.protocol.ZoneReencryptionStatus.State;
import org.apache.hadoop.hdfs.server.namenode.FSTreeTraverser.TraverseInfo;
import org.apache.hadoop.hdfs.server.namenode.ReencryptionUpdater.FileEdekInfo;
import org.apache.hadoop.hdfs.server.namenode.ReencryptionUpdater.ReencryptionTask;
import org.apache.hadoop.hdfs.server.namenode.ReencryptionUpdater.ZoneSubmissionTracker;
import org.apache.hadoop.hdfs.util.RwLockMode;
import org.apache.hadoop.ipc.RetriableException;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_REENCRYPT_BATCH_SIZE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_REENCRYPT_BATCH_SIZE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_REENCRYPT_SLEEP_INTERVAL_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_REENCRYPT_SLEEP_INTERVAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_REENCRYPT_THROTTLE_LIMIT_HANDLER_RATIO_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_REENCRYPT_THROTTLE_LIMIT_HANDLER_RATIO_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_REENCRYPT_EDEK_THREADS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_REENCRYPT_EDEK_THREADS_KEY;

/**
 * 处理加密区域EDEK（加密数据加密密钥）重加密操作的后台处理器
 * <p>
 * 工作流程：对于每个加密区域（Encryption Zone），ReencryptionHandler以深度优先遍历目录树，
 * 将一批文件和已有EDEK组装为重加密任务提交给线程池。线程池中的每个线程联系KMS（密钥管理服务）
 * 重新加密EDEK。ReencryptionUpdater跟踪任务状态，并将新EDEK更新到文件扩展属性中。
 * <p>
 * 正在进行重加密的加密区域会禁用文件重命名。新创建的文件会直接生成新EDEK，
 * 因为提交重加密命令时已经清空了EDEK缓存。
 * <p>
 * 设计上只允许1个ReencryptionHandler运行，原因：
 *   1. 整个重加密流程的瓶颈在KMS，多处理器无法提升性能
 *   2. 即使多处理器，更新EDEK需要写锁且是单线程，性能提升有限
 * <p>
 * 该类使用FSDirectory锁进行同步控制。
 */
@InterfaceAudience.Private
public class ReencryptionHandler implements Runnable {

  public static final Logger LOG =
      LoggerFactory.getLogger(ReencryptionHandler.class);

  // 2000 is based on buffer size = 512 * 1024, and SetXAttr op size is
  // 100 - 200 bytes (depending on the xattr value).
  // The buffer size is hard-coded, see outputBufferCapacity from QJM.
  private static final int MAX_BATCH_SIZE_WITHOUT_FLOODING = 2000;

  private final EncryptionZoneManager ezManager;
  private final FSDirectory dir;
  private final long interval;
  private final int reencryptBatchSize;
  private double throttleLimitHandlerRatio;
  private final int reencryptThreadPoolSize;
  // 用于流量控制的计时器
  private final StopWatch throttleTimerAll = new StopWatch();
  private final StopWatch throttleTimerLocked = new StopWatch();

  private ExecutorCompletionService<ReencryptionTask> batchService;
  private BlockingQueue<Runnable> taskQueue;
  // protected by ReencryptionHandler object lock
  private final Map<Long, ZoneSubmissionTracker> submissions = new HashMap<>();

  // The current batch that the handler is working on. Handler is designed to
  // be single-threaded, see class javadoc for more details.
  private ReencryptionBatch currentBatch;

  private final ReencryptionPendingInodeIdCollector traverser;

  private final ReencryptionUpdater reencryptionUpdater;
  private ExecutorService updaterExecutor;

  // Vars for unit tests.
  private volatile boolean shouldPauseForTesting = false;
  private volatile int pauseAfterNthSubmission = 0;

  /**
   * 停止重加密更新线程，并取消所有已提交的EDEK重加密任务。
   */
  void stopThreads() {
    assert dir.hasWriteLock();
    synchronized (this) {
      for (ZoneSubmissionTracker zst : submissions.values()) {
        zst.cancelAllTasks();
      }
    }
    if (updaterExecutor != null) {
      updaterExecutor.shutdownNow();
    }
  }

  /**
   * 启动重加密结果更新线程。
   */
  void startUpdaterThread() {
    updaterExecutor = Executors.newSingleThreadExecutor(
        new ThreadFactoryBuilder().setDaemon(true)
            .setNameFormat("reencryptionUpdaterThread #%d").build());
    updaterExecutor.execute(reencryptionUpdater);
  }

  @VisibleForTesting
  synchronized void pauseForTesting() {
    shouldPauseForTesting = true;
    LOG.info("Pausing re-encrypt handler for testing.");
    notify();
  }

  @VisibleForTesting
  synchronized void resumeForTesting() {
    shouldPauseForTesting = false;
    LOG.info("Resuming re-encrypt handler for testing.");
    notify();
  }

  @VisibleForTesting
  void pauseForTestingAfterNthSubmission(final int count) {
    assert pauseAfterNthSubmission == 0;
    pauseAfterNthSubmission = count;
  }

  @VisibleForTesting
  void pauseUpdaterForTesting() {
    reencryptionUpdater.pauseForTesting();
  }

  @VisibleForTesting
  void resumeUpdaterForTesting() {
    reencryptionUpdater.resumeForTesting();
  }

  @VisibleForTesting
  void pauseForTestingAfterNthCheckpoint(final long zoneId, final int count) {
    reencryptionUpdater.pauseForTestingAfterNthCheckpoint(zoneId, count);
  }

  /**
   * 构造ReencryptionHandler，从配置中初始化参数和线程池。
   * @param ezMgr 加密区域管理器
   * @param conf Hadoop配置
   */
  ReencryptionHandler(final EncryptionZoneManager ezMgr,
      final Configuration conf) {
    this.ezManager = ezMgr;
    Preconditions.checkNotNull(ezManager.getProvider(),
        "No provider set, cannot re-encrypt");
    this.dir = ezMgr.getFSDirectory();
    this.interval =
        conf.getTimeDuration(DFS_NAMENODE_REENCRYPT_SLEEP_INTERVAL_KEY,
            DFS_NAMENODE_REENCRYPT_SLEEP_INTERVAL_DEFAULT,
            TimeUnit.MILLISECONDS);
    Preconditions.checkArgument(interval > 0,
        DFS_NAMENODE_REENCRYPT_SLEEP_INTERVAL_KEY + " is not positive.");
    this.reencryptBatchSize = conf.getInt(DFS_NAMENODE_REENCRYPT_BATCH_SIZE_KEY,
        DFS_NAMENODE_REENCRYPT_BATCH_SIZE_DEFAULT);
    Preconditions.checkArgument(reencryptBatchSize > 0,
        DFS_NAMENODE_REENCRYPT_BATCH_SIZE_KEY + " is not positive.");
    if (reencryptBatchSize > MAX_BATCH_SIZE_WITHOUT_FLOODING) {
      LOG.warn("Re-encryption batch size is {}. It could cause edit log buffer "
          + "to be full and trigger a logSync within the writelock, greatly "
          + "impacting namenode throughput.", reencryptBatchSize);
    }
    this.throttleLimitHandlerRatio =
        conf.getDouble(DFS_NAMENODE_REENCRYPT_THROTTLE_LIMIT_HANDLER_RATIO_KEY,
            DFS_NAMENODE_REENCRYPT_THROTTLE_LIMIT_HANDLER_RATIO_DEFAULT);
    LOG.info("Configured throttleLimitHandlerRatio={} for re-encryption",
        throttleLimitHandlerRatio);
    Preconditions.checkArgument(throttleLimitHandlerRatio > 0.0f,
        DFS_NAMENODE_REENCRYPT_THROTTLE_LIMIT_HANDLER_RATIO_KEY
            + " is not positive.");
    this.reencryptThreadPoolSize =
        conf.getInt(DFS_NAMENODE_REENCRYPT_EDEK_THREADS_KEY,
            DFS_NAMENODE_REENCRYPT_EDEK_THREADS_DEFAULT);

    taskQueue = new LinkedBlockingQueue<>();
    ThreadPoolExecutor threadPool =
        new ThreadPoolExecutor(reencryptThreadPoolSize, reencryptThreadPoolSize,
            60, TimeUnit.SECONDS, taskQueue, new Daemon.DaemonFactory() {
              private final AtomicInteger ind = new AtomicInteger(0);

              @Override
              public Thread newThread(Runnable r) {
                Thread t = super.newThread(r);
                t.setName("reencryption edek Thread-" + ind.getAndIncrement());
                return t;
              }
            }, new ThreadPoolExecutor.CallerRunsPolicy() {

              @Override
              public void rejectedExecution(Runnable runnable,
                  ThreadPoolExecutor e) {
                LOG.info("Execution rejected, executing in current thread");
                super.rejectedExecution(runnable, e);
              }
            });

    threadPool.allowCoreThreadTimeOut(true);
    this.batchService = new ExecutorCompletionService(threadPool);
    reencryptionUpdater =
        new ReencryptionUpdater(dir, batchService, this, conf);
    currentBatch = new ReencryptionBatch(reencryptBatchSize);
    traverser = new ReencryptionPendingInodeIdCollector(dir, this, conf);
  }

  ReencryptionStatus getReencryptionStatus() {
    return ezManager.getReencryptionStatus();
  }

  /**
   * 取消指定加密区域的重加密任务。
   * @param zoneId 加密区域ID
   * @param zoneName 加密区域名称
   * @throws IOException 如果加密区域不在重加密状态则抛出异常
   */
  void cancelZone(final long zoneId, final String zoneName) throws IOException {
    assert dir.hasWriteLock();
    final ZoneReencryptionStatus zs =
        getReencryptionStatus().getZoneStatus(zoneId);
    if (zs == null || zs.getState() == State.Completed) {
      throw new IOException("Zone " + zoneName + " is not under re-encryption");
    }
    zs.cancel();
    removeZoneTrackerStopTasks(zoneId);
  }

  /**
   * 从重加密处理器中移除已完成的加密区域。
   * @param zoneId 加密区域ID
   */
  void removeZone(final long zoneId) {
    assert dir.hasWriteLock();
    LOG.info("Removing zone {} from re-encryption.", zoneId);
    removeZoneTrackerStopTasks(zoneId);
    getReencryptionStatus().removeZone(zoneId);
  }

  synchronized private void removeZoneTrackerStopTasks(final long zoneId) {
    final ZoneSubmissionTracker zst = submissions.get(zoneId);
    if (zst != null) {
      zst.cancelAllTasks();
      submissions.remove(zoneId);
    }
  }

  /**
   * 获取指定加密区域的提交跟踪器，需要持有FSDirectory读锁。
   * @param zoneId 加密区域ID
   * @return 提交跟踪器
   */
  ZoneSubmissionTracker getTracker(final long zoneId) {
    assert dir.hasReadLock();
    return unprotectedGetTracker(zoneId);
  }

  /**
   * 在不持有FSDirectory锁的情况下获取指定加密区域的提交跟踪器，
   * submissions对象由ReencryptionHandler对象锁保护。
   * @param zoneId 加密区域ID
   * @return 提交跟踪器
   */
  synchronized ZoneSubmissionTracker unprotectedGetTracker(final long zoneId) {
    return submissions.get(zoneId);
  }

  /**
   * 为不需要重加密任何文件的加密区域添加空的虚拟跟踪器。
   * 这是因为完成重加密需要升级到写锁修改区域扩展属性，而当前处理器只持有读锁，
   * 通过添加空任务让更新线程完成最终的重加密完成流程。
   * @param zoneId 加密区域ID
   * @param zst 已有的提交跟踪器，如果为null则新建
   */
  void addDummyTracker(final long zoneId, ZoneSubmissionTracker zst) {
    assert dir.hasReadLock();
    if (zst == null) {
      zst = new ZoneSubmissionTracker();
    }
    zst.setSubmissionDone();

    final Future future = batchService.submit(
        new EDEKReencryptCallable(zoneId, new ReencryptionBatch(), this));
    zst.addTask(future);
    synchronized (this) {
      submissions.put(zoneId, zst);
    }
  }

  /**
   * 后台线程主循环，每次循环最多处理一个加密区域，直到该区域重加密完成。
   */
  @Override
  public void run() {
    LOG.info("Starting up re-encrypt thread with interval={} millisecond.",
        interval);
    while (true) {
      try {
        synchronized (this) {
          wait(interval);
        }
        traverser.checkPauseForTesting();
      } catch (InterruptedException ie) {
        LOG.info("Re-encrypt handler interrupted. Exiting");
        Thread.currentThread().interrupt();
        return;
      }

      final Long zoneId;
      // 获取文件系统读锁
      dir.getFSNamesystem().readLock(RwLockMode.FS);
      try {
        // 获取下一个待处理的加密区域
        zoneId = getReencryptionStatus().getNextUnprocessedZone();
        if (zoneId == null) {
          // 无待处理区域，继续下一轮循环
          continue;
        }
        LOG.info("Executing re-encrypt commands on zone {}. Current zones:{}",
            zoneId, getReencryptionStatus());
        // 标记加密区域已开始处理
        getReencryptionStatus().markZoneStarted(zoneId);
        // 重置提交跟踪器
        resetSubmissionTracker(zoneId);
      } finally {
        // 释放文件系统读锁
        dir.getFSNamesystem().readUnlock(RwLockMode.FS, "reEncryptThread");
      }

      try {
        // 执行该加密区域的重加密
        reencryptEncryptionZone(zoneId);
      } catch (RetriableException | SafeModeException re) {
        LOG.info("Re-encryption caught exception, will retry", re);
        // 标记为可重试，下次重新处理
        getReencryptionStatus().markZoneForRetry(zoneId);
      } catch (IOException ioe) {
        LOG.warn("IOException caught when re-encrypting zone {}", zoneId, ioe);
      } catch (InterruptedException ie) {
        LOG.info("Re-encrypt handler interrupted. Exiting.");
        Thread.currentThread().interrupt();
        return;
      } catch (Throwable t) {
        LOG.error("Re-encrypt handler thread exiting. Exception caught when"
            + " re-encrypting zone {}.", zoneId, t);