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

package org.apache.hadoop.hdfs.server.datanode.checker;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.FutureCallback;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.Futures;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ListenableFuture;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.MoreExecutors;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeReference;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi.VolumeCheckContext;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;
import org.apache.hadoop.util.Sets;
import org.apache.hadoop.util.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.channels.ClosedChannelException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DISK_CHECK_MIN_GAP_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DISK_CHECK_TIMEOUT_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DISK_CHECK_TIMEOUT_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_FAILED_VOLUMES_TOLERATED_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_FAILED_VOLUMES_TOLERATED_KEY;

/**
 * 文件：DatasetVolumeChecker.java
 * 所属模块：HDFS DataNode 卷健康检查模块
 * 核心职责：封装对FsDatasetSpi所有卷的磁盘健康检查逻辑，支持同步全量检查和异步单卷检查，收集并返回检查失败的卷列表
 * 设计目的：将原本分散在DataNode、FsDatasetImpl、FsVolumeList中的磁盘检查逻辑抽离统一，实现模块化解耦
 */
public class DatasetVolumeChecker {

  public static final Logger LOG =
      LoggerFactory.getLogger(DatasetVolumeChecker.class);

  /** 异步检查执行器代理，实现限流调度 */
  private AsyncChecker<VolumeCheckContext, VolumeCheckResult> delegateChecker;

  /** 单卷检查总次数统计 */
  private final AtomicLong numVolumeChecks = new AtomicLong(0);
  /** 全量同步检查总次数统计 */
  private final AtomicLong numSyncDatasetChecks = new AtomicLong(0);
  /** 因时间间隔不足跳过检查的次数统计 */
  private final AtomicLong numSkippedChecks = new AtomicLong(0);

  /** 单磁盘检查最大允许超时时间（毫秒），超时则判定磁盘失效 */
  private final long maxAllowedTimeForCheckMs;

  /** 容忍的最大卷故障数，超过该数值则触发DataNode级致命错误 */
  private final int maxVolumeFailuresTolerated;

  /** 同一卷两次连续检查的最小时间间隔（毫秒），用于限流避免频繁检查 */
  private final long minDiskCheckGapMs;
  /** 磁盘检查超时时间配置 */
  private final long diskCheckTimeout;

  /** 上次全量检查所有卷的时间戳 */
  private long lastAllVolumesCheck;

  /** 定时器对象，用于时间计算和限流判断 */
  private final Timer timer;

  /** 空上下文对象，所有检查复用此实例 */
  private static final VolumeCheckContext IGNORED_CONTEXT =
      new VolumeCheckContext();

  /** 单卷异步检查结果处理线程池 */
  private final ExecutorService checkVolumeResultHandlerExecutorService;

  /**
   * 构造方法，从配置初始化卷检查器
   * @param conf Hadoop配置对象
   * @param timer 定时器对象，用于限流判断
   * @throws DiskErrorException 当配置参数非法时抛出异常
   */
  public DatasetVolumeChecker(Configuration conf, Timer timer)
      throws DiskErrorException {
    maxAllowedTimeForCheckMs = conf.getTimeDuration(
        DFS_DATANODE_DISK_CHECK_TIMEOUT_KEY,
        DFS_DATANODE_DISK_CHECK_TIMEOUT_DEFAULT,
        TimeUnit.MILLISECONDS);

    // 校验超时配置必须为正
    if (maxAllowedTimeForCheckMs <= 0) {
      throw new HadoopIllegalArgumentException("Invalid value configured for "
          + DFS_DATANODE_DISK_CHECK_TIMEOUT_KEY + " - "
          + maxAllowedTimeForCheckMs + " (should be > 0)");
    }

    this.timer = timer;

    maxVolumeFailuresTolerated = conf.getInt(
        DFS_DATANODE_FAILED_VOLUMES_TOLERATED_KEY,
        DFS_DATANODE_FAILED_VOLUMES_TOLERATED_DEFAULT);

    minDiskCheckGapMs = conf.getTimeDuration(
        DFSConfigKeys.DFS_DATANODE_DISK_CHECK_MIN_GAP_KEY,
        DFSConfigKeys.DFS_DATANODE_DISK_CHECK_MIN_GAP_DEFAULT,
        TimeUnit.MILLISECONDS);

    // 校验最小间隔配置不能为负
    if (minDiskCheckGapMs < 0) {
      throw new HadoopIllegalArgumentException("Invalid value configured for "
          + DFS_DATANODE_DISK_CHECK_MIN_GAP_KEY + " - "
          + minDiskCheckGapMs + " (should be >= 0)");
    }

    diskCheckTimeout = conf.getTimeDuration(
        DFSConfigKeys.DFS_DATANODE_DISK_CHECK_TIMEOUT_KEY,
        DFSConfigKeys.DFS_DATANODE_DISK_CHECK_TIMEOUT_DEFAULT,
        TimeUnit.MILLISECONDS);

    // 二次校验超时配置不能为负
    if (diskCheckTimeout < 0) {
      throw new HadoopIllegalArgumentException("Invalid value configured for "
          + DFS_DATANODE_DISK_CHECK_TIMEOUT_KEY + " - "
          + diskCheckTimeout + " (should be >= 0)");
    }

    // 初始化上次检查时间，保证首次检查不会被跳过
    lastAllVolumesCheck = timer.monotonicNow() - minDiskCheckGapMs;

    // 校验最大容忍故障数不超过系统允许上限
    if (maxVolumeFailuresTolerated < DataNode.MAX_VOLUME_FAILURE_TOLERATED_LIMIT) {
      throw new HadoopIllegalArgumentException("Invalid value configured for "
          + DFS_DATANODE_FAILED_VOLUMES_TOLERATED_KEY + " - "
          + maxVolumeFailuresTolerated + " "
          + DataNode.MAX_VOLUME_FAILURES_TOLERATED_MSG);
    }

    // 初始化带限流的异步检查器，使用缓存线程池执行检查任务
    delegateChecker = new ThrottledAsyncChecker<>(
        timer, minDiskCheckGapMs, diskCheckTimeout,
        Executors.newCachedThreadPool(
            new ThreadFactoryBuilder()
                .setNameFormat("DataNode DiskChecker thread %d")
                .setDaemon(true)
                .build()));

    // 初始化结果处理线程池
    checkVolumeResultHandlerExecutorService = Executors.newCachedThreadPool(
        new ThreadFactoryBuilder()
            .setNameFormat("VolumeCheck ResultHandler thread %d")
            .setDaemon(true)
            .build());
  }

  /**
   * 同步检查数据集的所有卷，返回检查失败的卷集合
   * 执行时机：DataNode启动时、定期周期性检查，用于及时发现故障磁盘并处理
   * @param dataset 待检查的文件系统数据集
   * @return 检查失败的卷集合，返回空集合表示全部健康或检查被跳过
   * @throws InterruptedException 等待检查完成时被中断抛出
   */
  public Set<FsVolumeSpi> checkAllVolumes(
      final FsDatasetSpi<? extends FsVolumeSpi> dataset)
      throws InterruptedException {
    // 计算距离上次全量检查的时间间隔
    final long gap = timer.monotonicNow() - lastAllVolumesCheck;
    // 间隔小于最小要求，跳过本次检查
    if (gap < minDiskCheckGapMs) {
      numSkippedChecks.incrementAndGet();
      LOG.trace(
          "Skipped checking all volumes, time since last check {} is less " +
          "than the minimum gap between checks ({} ms).",
          gap, minDiskCheckGapMs);
      return Collections.emptySet();
    }

    // 获取所有卷的引用，防止检查过程中卷被释放
    final FsDatasetSpi.FsVolumeReferences references =
        dataset.getFsVolumeReferences();

    // 没有可引用的卷，直接返回
    if (references.size() == 0) {
      LOG.warn("checkAllVolumesAsync - no volumes can be referenced");
      return Collections.emptySet();
    }

    // 更新上次全量检查时间戳
    lastAllVolumesCheck = timer.monotonicNow();
    final Set<FsVolumeSpi> healthyVolumes = new HashSet<>();
    final Set<FsVolumeSpi> failedVolumes = new HashSet<>();
    final Set<FsVolumeSpi> allVolumes = new HashSet<>();

    final AtomicLong numVolumes = new AtomicLong(references.size());
    // 倒计时锁，用于等待所有检查完成
    final CountDownLatch latch = new CountDownLatch(1);

    // 遍历所有卷，调度异步检查
    for (int i = 0; i < references.size(); ++i) {
      final FsVolumeReference reference = references.getReference(i);
      Optional<ListenableFuture<VolumeCheckResult>> olf =
          delegateChecker.schedule(reference.getVolume(), IGNORED_CONTEXT);
      LOG.info("Scheduled health check for volume {}", reference.getVolume());
      if (olf.isPresent()) {
        allVolumes.add(reference.getVolume());
        // 注册结果回调处理器
        Futures.addCallback(olf.get(),
            new ResultHandler(reference, healthyVolumes, failedVolumes,
                numVolumes, new Callback() {
                  @Override
                  public void call(Set<FsVolumeSpi> ignored1,
                                   Set<FsVolumeSpi> ignored2) {
                    latch.countDown();
                  }
                }), MoreExecutors.directExecutor());
      } else {
        // 调度失败，释放卷引用
        IOUtils.cleanupWithLogger(null, reference);
        // 所有检查已完成，解锁等待
        if (numVolumes.decrementAndGet() == 0) {
          latch.countDown();
        }
      }
    }

    // 等待所有检查完成，超时则直接返回，未完成的卷判定为失败
    if (!latch.await(maxAllowedTimeForCheckMs, TimeUnit.MILLISECONDS)) {
      LOG.warn("checkAllVolumes timed out after {} ms",
          maxAllowedTimeForCheckMs);
    }

    numSyncDatasetChecks.incrementAndGet();
    synchronized (this) {
      // 所有未被标记为健康的卷都视为失败，超时未完成的也包含在内
      // 拷贝差异结果，避免并发修改导致异常
      return new HashSet<>(Sets.difference(allVolumes, healthyVolumes));
    }
  }

  /**
   * 多卷异步检查完成回调接口，当所有检查完成后触发用户自定义处理逻辑
   */
  public interface Callback {
    /**
     * 检查完成后的回调方法
     * @param healthyVolumes 检查通过的健康卷集合
     * @param failedVolumes 检查失败的卷集合
     */
    void call(Set<FsVolumeSpi> healthyVolumes,
              Set<FsVolumeSpi> failedVolumes);
  }

  /**
   * 异步检查单个卷，检查完成后通过回调返回结果
   * 用于不定期触发的单个卷健康检查，不阻塞调用线程
   * @param volume 待检查的卷
   * @param callback 检查完成后的回调处理器
   * @return true 检查已成功调度，回调会被执行；false 检查调度失败，回调不会执行
   */
  public boolean checkVolume(
      final FsVolumeSpi volume,
      Callback callback) {
    if (volume == null) {
      LOG.debug("Cannot schedule check on null volume");
      return false;
    }

    FsVolumeReference volumeReference;
    try {
      // 获取卷引用，防止检查过程中卷被关闭释放
      volumeReference = volume.obtainReference();
    } catch (ClosedChannelException e) {
      // 卷已经关闭，无法检查
      return false;
    }

    // 调度异步检查
    Optional<ListenableFuture<VolumeCheckResult>> olf =
        delegateChecker.schedule(volume, IGNORED_CONTEXT);
    if (olf.isPresent()) {
      numVolumeChecks.incrementAndGet();
      // 注册结果回调，使用专用线程池处理结果
      Futures.addCallback(olf.get(),
          new ResultHandler(volumeReference, new HashSet<>(), new HashSet<>(),
              new AtomicLong(1), callback),
          checkVolumeResultHandlerExecutorService
      );
      return true;
    } else {
      // 调度失败，释放卷引用
      IOUtils.cleanupWithLogger(null, volumeReference);
    }
    return false;
  }

  /**
   * 单卷检查结果回调处理器，处理单个卷检查成功/失败结果，释放资源，触发上层回调
   */
  private class ResultHandler
      implements FutureCallback<VolumeCheckResult> {
    /** 待检查卷的引用，检查完成后需要释放 */
    private final FsVolumeReference reference;
    /** 失败卷集合，检查失败将卷加入此集合 */
    private final Set<FsVolumeSpi> failedVolumes;
    /** 健康卷集合，检查成功将卷加入此集合 */
    private final Set<FsVolumeSpi> healthyVolumes;
    /** 剩余未完成检查计数，计数到0触发上层回调 */
    private final AtomicLong volumeCounter;

    @Nullable
    private final Callback callback;

    /**
     * 构造结果处理器
     * @param reference 待检查卷的引用，检查完成后释放
     * @param healthyVolumes 健康卷集合
     * @param failedVolumes 失败卷集合
     * @param volumeCounter 剩余未完成检查计数器
     * @param callback 所有检查完成后的回调
     */
    ResultHandler(FsVolumeReference reference,
                  Set<FsVolumeSpi> healthyVolumes,
                  Set<FsVolumeSpi> failedVolumes,
                  AtomicLong volumeCounter,
                  @Nullable Callback callback) {
      Preconditions.checkState(reference != null);
      this.reference = reference;
      this.healthyVolumes = healthyVolumes;
      this.failedVolumes = failedVolumes;
      this.volumeCounter = volumeCounter;
      this.callback = callback;
    }

    /**
     * 检查成功完成后的处理逻辑
     * @param result 卷检查结果
     */
    @Override
    public void onSuccess(VolumeCheckResult result) {
      if (result == null) {
        LOG.error("Unexpected health check result null for volume {}",
            reference.getVolume());
        // 结果为空，默认标记为健康
        markHealthy();
      } else {
        // 根据结果类型处理
        switch(result) {
        case HEALTHY:
        case DEGRADED:
          // 健康或降级都视为可用，标记为健康
          LOG.debug("Volume {} is {}.", reference.getVolume(), result);
          markHealthy();
          break;
        case FAILED:
          // 检查失败，标记为失败
          LOG.warn("Volume {} detected as being unhealthy",
              reference.getVolume());
          markFailed();
          break;
        default:
          // 未知结果类型，默认标记为健康
          LOG.error("Unexpected health check result {} for volume {}",
              result, reference.getVolume());
          markHealthy();
          break;
        }
      }
      cleanup();
    }

    /**
     * 检查过程发生异常的处理逻辑
     * @param t 异常对象
     */
    @Override
    public void onFailure(@Nonnull Throwable t) {
      // 解包ExecutionException获取真实异常
      Throwable exception = (t instanceof ExecutionException