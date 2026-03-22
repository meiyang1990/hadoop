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

import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.FutureCallback;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.Futures;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.FluentFuture;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ListenableFuture;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.MoreExecutors;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.util.Timer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.WeakHashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 文件概述：限流异步检查器实现，用于数据节点对可检查对象执行异步检查，通过限制同一对象的检查频率避免资源过度消耗
 * 限流异步检查器实现，继承AsyncChecker接口，对最近检查过的对象跳过重复检查，
 * 强制同一对象的两次连续检查间隔至少为{@link #minMsBetweenChecks}毫秒。
 * 
 * 假设系统中可检查对象总数较小（不超过几十个），因为该检查器占用O(可检查对象数)的存储空间，
 * 并且可能占用O(可检查对象数)的线程资源。
 * 调用方需要合理配置{@link #minMsBetweenChecks}，避免频繁创建过多线程。
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class ThrottledAsyncChecker<K, V> implements AsyncChecker<K, V> {
  public static final Logger LOG =
      LoggerFactory.getLogger(ThrottledAsyncChecker.class);

  private final Timer timer;

  /**
   * 用于调度异步检查的执行器服务
   */
  private final ListeningExecutorService executorService;
  private final ScheduledExecutorService scheduledExecutorService;

  /**
   * 同一对象两次连续检查之间的最小间隔毫秒数，用于限流
   */
  private final long minMsBetweenChecks;
  private final long diskCheckTimeout;

  /**
   * 当前正在进行中的检查任务映射，键为可检查对象，值为对应的异步Future。
   * 由当前对象锁保护线程安全。
   */
  private final Map<Checkable, ListenableFuture<V>> checksInProgress;

  /**
   * 已完成检查结果缓存，键为可检查对象，值为上次检查结果。
   * 由当前对象锁保护线程安全。
   */
  private final Map<Checkable, LastCheckResult<V>> completedChecks;

  /**
   * 构造限流异步检查器实例
   * @param timer 计时器，用于计算检查间隔
   * @param minMsBetweenChecks 同一对象两次检查的最小间隔毫秒数
   * @param diskCheckTimeout 磁盘检查超时时间（毫秒），0表示不设置超时
   * @param executorService 执行异步检查的线程池
   */
  public ThrottledAsyncChecker(final Timer timer,
                        final long minMsBetweenChecks,
                        final long diskCheckTimeout,
                        final ExecutorService executorService) {
    this.timer = timer;
    this.minMsBetweenChecks = minMsBetweenChecks;
    this.diskCheckTimeout = diskCheckTimeout;
    this.executorService = MoreExecutors.listeningDecorator(executorService);
    this.checksInProgress = new HashMap<>();
    this.completedChecks = new WeakHashMap<>();

    if (this.diskCheckTimeout > 0) {
      ScheduledThreadPoolExecutor scheduledThreadPoolExecutor = new
          ScheduledThreadPoolExecutor(1);
      this.scheduledExecutorService = MoreExecutors
          .getExitingScheduledExecutorService(scheduledThreadPoolExecutor);
    } else {
      this.scheduledExecutorService = null;
    }
  }

  /**
   * 调度指定可检查对象的检查任务，遵循限流规则：
   * 如果对象最近已检查过则跳过检查；同一对象同时只会有一个并发检查
   * @param target 待检查的目标对象
   * @param context 检查上下文
   * @return 如果成功调度检查返回包含ListenableFuture的Optional，否则返回空Optional
   */
  @Override
  public synchronized Optional<ListenableFuture<V>> schedule(
      Checkable<K, V> target, K context) {
    // 同一对象已有检查正在进行，跳过调度
    if (checksInProgress.containsKey(target)) {
      return Optional.empty();
    }

    // 获取该对象上次检查结果
    final LastCheckResult<V> result = completedChecks.get(target);
    if (result != null) {
      // 计算距离上次检查的时间间隔
      final long msSinceLastCheck = timer.monotonicNow() - result.completedAt;
      // 间隔小于最小要求，跳过本次检查
      if (msSinceLastCheck < minMsBetweenChecks) {
        LOG.debug("Skipped checking {}. Time since last check {}ms " +
                "is less than the min gap {}ms.",
            target, msSinceLastCheck, minMsBetweenChecks);
        return Optional.empty();
      }
    }

    LOG.info("Scheduling a check for {}", target);
    // 提交检查任务到线程池
    final ListenableFuture<V> lfWithoutTimeout = executorService.submit(
        new Callable<V>() {
          @Override
          public V call() throws Exception {
            return target.check(context);
          }
        });
    final ListenableFuture<V> lf;

    // 如果配置了超时，添加超时处理
    if (diskCheckTimeout > 0) {
      lf = FluentFuture.from(lfWithoutTimeout)
          .withTimeout(diskCheckTimeout, TimeUnit.MILLISECONDS, scheduledExecutorService);
    } else {
      lf = lfWithoutTimeout;
    }

    // 记录正在进行的检查
    checksInProgress.put(target, lf);
    // 注册结果缓存回调
    addResultCachingCallback(target, lf);
    return Optional.of(lf);
  }

  /**
   * 注册回调函数，用于在检查完成后缓存检查结果
   * @param target 目标可检查对象
   * @param lf 检查任务的异步Future
   */
  private void addResultCachingCallback(
      Checkable<K, V> target, ListenableFuture<V> lf) {
    Futures.addCallback(lf, new FutureCallback<V>() {
      @Override
      public void onSuccess(V result) {
        synchronized (ThrottledAsyncChecker.this) {
          // 从进行中集合移除
          checksInProgress.remove(target);
          // 缓存成功结果
          completedChecks.put(target, new LastCheckResult<>(
              result, timer.monotonicNow()));
        }
      }

      @Override
      public void onFailure(@Nonnull Throwable t) {
        synchronized (ThrottledAsyncChecker.this) {
          // 从进行中集合移除
          checksInProgress.remove(target);
          // 缓存失败结果
          completedChecks.put(target, new LastCheckResult<>(
              t, timer.monotonicNow()));
        }
      }
    }, MoreExecutors.directExecutor());
  }

  /**
   * 关闭检查器并等待所有任务终止，中断所有正在执行的检查以加速关闭过程
   * {@inheritDoc}.
   *
   * 进行中检查的结果在关闭过程中无用，因此通过中断所有活跃检查实现更快关闭。
   * @param timeout 等待超时时间
   * @param timeUnit 超时时间单位
   * @throws InterruptedException 等待终止时被中断抛出
   */
  @Override
  public void shutdownAndWait(long timeout, TimeUnit timeUnit)
      throws InterruptedException {
    if (scheduledExecutorService != null) {
      // 立即关闭定时执行器
      scheduledExecutorService.shutdownNow();
      // 等待终止完成
      scheduledExecutorService.awaitTermination(timeout, timeUnit);
    }

    // 立即关闭检查执行器
    executorService.shutdownNow();
    // 等待终止完成
    executorService.awaitTermination(timeout, timeUnit);
  }

  /**
   * 存储上次检查结果的内部类，保存检查完成时间和结果（或异常）
   * @param <V> 检查结果类型
   */
  private static final class LastCheckResult<V> {
    /**
     * 检查完成的时间戳
     */
    private final long completedAt;

    /**
     * 检查成功时的结果，检查失败时为null
     */
    @Nullable
    private final V result;

    /**
     * 检查失败时抛出的异常，检查成功时为null
     */
    private final Throwable exception; // null on success.

    /**
     * 构造成功检查结果
     * @param result 检查结果
     * @param completedAt 完成时间戳
     */
    private LastCheckResult(V result, long completedAt) {
      this.result = result;
      this.exception = null;
      this.completedAt = completedAt;
    }

    /**
     * 构造失败检查结果
     * @param t 检查抛出的异常
     * @param completedAt 完成时间戳
     */
    private LastCheckResult(Throwable t, long completedAt) {
      this.result = null;
      this.exception = t;
      this.completedAt = completedAt;
    }
  }
}