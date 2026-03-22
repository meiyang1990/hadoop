// 这个文件已经全部加上中文注释
/*
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

package org.apache.hadoop.mapreduce.lib.output.committer.manifest.stages;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.statistics.DurationTracker;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsStore;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.AbstractManifestData;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.FileEntry;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.TaskManifest;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.ManifestStoreOperations;
import org.apache.hadoop.util.OperationDuration;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.util.functional.CallableRaisingIOE;
import org.apache.hadoop.util.functional.RemoteIterators;
import org.apache.hadoop.util.functional.TaskPool;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.OP_DELETE;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.OP_GET_FILE_STATUS;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.OP_IS_FILE;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.OP_LIST_STATUS;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.OP_MKDIRS;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.STORE_IO_RATE_LIMITED;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.createTracker;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.trackDuration;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.trackDurationOfInvocation;
import static org.apache.hadoop.io.retry.RetryPolicies.retryUpToMaximumCountWithProportionalSleep;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.MANIFEST_SUFFIX;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_COMMIT_FILE_RENAME;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_COMMIT_FILE_RENAME_RECOVERED;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_DELETE_DIR;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_LOAD_MANIFEST;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_MSYNC;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_RENAME_DIR;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_RENAME_FILE;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_SAVE_TASK_MANIFEST;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.AuditingIntegration.enterStageWorker;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.InternalConstants.SAVE_SLEEP_INTERVAL;

/**
 * 文件级输出提交器Manifest协议中作业/任务提交阶段的抽象基类。
 * 定义了提交阶段的通用执行框架，所有具体提交阶段都需要继承此类，
 * 保证每个阶段只能执行一次，并提供了统计收集、通用文件操作等能力。
 * @param <IN> 阶段入参类型
 * @param <OUT> 阶段返回结果类型
 */
public abstract class AbstractJobOrTaskStage<IN, OUT>
    implements JobOrTaskStage<IN, OUT> {

  private static final Logger LOG = LoggerFactory.getLogger(
      AbstractJobOrTaskStage.class);

  /**
   * 重命名失败错误信息前缀。
   */
  public static final String FAILED_TO_RENAME_PREFIX = "Failed to ";

  /**
   * 标记是否为任务级阶段。
   */
  private final boolean isTaskStage;

  /**
   * 整个提交操作的共享配置。
   */
  private final StageConfig stageConfig;

  /**
   * 阶段统计名称，用于统计和日志输出。
   */
  private final String stageStatisticName;

  /**
   * 存储操作回调接口，封装对文件系统的操作。
   * 本类会对操作添加统计和日志，子类无需直接操作底层接口。
   */
  private final ManifestStoreOperations operations;

  /**
   * IO操作并行执行器提交器。
   */
  private final TaskPool.Submitter ioProcessors;

  /**
   * 执行标记，保证阶段只能执行一次，防止重入。
   */
  private final AtomicBoolean executed = new AtomicBoolean(false);

  /**
   * 阶段执行时长追踪器，执行完成后赋值。
   */
  private DurationTracker stageExecutionTracker;

  /**
   * 日志打印用名称。
   */
  private final String name;

  /**
   * 构造函数，初始化阶段基础信息并做参数校验。
   * @param isTaskStage 是否为任务级阶段
   * @param stageConfig 全局提交配置
   * @param stageStatisticName 阶段统计名称
   * @param requireIOProcessors 是否需要IO并行处理器
   */
  protected AbstractJobOrTaskStage(
      final boolean isTaskStage,
      final StageConfig stageConfig,
      final String stageStatisticName,
      final boolean requireIOProcessors) {
    this.isTaskStage = isTaskStage;
    this.stageStatisticName = stageStatisticName;
    this.stageConfig = stageConfig;
    requireNonNull(stageConfig.getDestinationDir(), "Destination Directory");
    requireNonNull(stageConfig.getJobId(), "Job ID");
    requireNonNull(stageConfig.getJobAttemptDir(), "Job attempt directory");
    this.operations = requireNonNull(stageConfig.getOperations(),
        "Operations callbacks");
    // 根据需求绑定IO处理器
    this.ioProcessors = bindProcessor(
        requireIOProcessors,
        stageConfig.getIoProcessors());
    String stageName;
    if (isTaskStage) {
      // 任务阶段提前校验任务信息，快速失败
      getRequiredTaskId();
      getRequiredTaskAttemptId();
      getRequiredTaskAttemptDir();
      stageName = String.format("[Task-Attempt %s]", getRequiredTaskAttemptId());
    } else {
      stageName = String.format("[Job-Attempt %s/%02d]",
          stageConfig.getJobId(),
          stageConfig.getJobAttemptNumber());
    }
    name = stageName;
  }

  /**
   * 按需绑定IO处理器，如果要求必须存在但传入为null则抛出空指针异常。
   * @param required 是否必须需要处理器
   * @param processor 传入的处理器实例
   * @return 绑定后的处理器
   */
  private TaskPool.Submitter bindProcessor(
      final boolean required,
      final TaskPool.Submitter processor) {
    return required
        ? requireNonNull(processor, "required IO processor is null")
        : null;
  }

  /**
   * 阶段入口方法，保证只执行一次，执行前做校验，收集执行时长统计，
   * 调用子类{@link #executeStage(Object)}完成实际阶段逻辑。
   * @param arguments 阶段入参
   * @return 阶段执行结果
   * @throws IOException 执行失败抛出IO异常
   */
  @Override
  public final OUT apply(final IN arguments) throws IOException {
    // 校验只能执行一次
    executeOnlyOnce();
    // 通知MapReduce任务进度，防止超时
    progress();
    String stageName = getStageName(arguments);
    // 进入阶段，更新配置中当前阶段信息
    getStageConfig().enterStage(stageName);
    String statisticName = getStageStatisticName(arguments);
    LOG.info("{}: Executing Stage {}", getName(), stageName);
    // 创建阶段执行时长追踪器
    stageExecutionTracker = createTracker(getIOStatistics(), statisticName);
    try {
      // 调用子类执行阶段逻辑
      final OUT out = executeStage(arguments);
      LOG.info("{}: Stage {} completed after {}",
          getName(),
          stageName,
          OperationDuration.humanTime(
              stageExecutionTracker.asDuration().toMillis()));
      return out;
    } catch (IOException | RuntimeException e) {
      LOG.error("{}: Stage {} failed: after {}: {}",
          getName(),
          stageName,
          OperationDuration.humanTime(
              stageExecutionTracker.asDuration().toMillis()),
          e.toString());
      LOG.debug("{}: Stage failure:", getName(), e);
      // 标记执行失败
      stageExecutionTracker.failed();
      throw e;
    } finally {
      // 关闭追踪器，完成统计
      stageExecutionTracker.close();
      progress();
      getStageConfig().exitStage(stageName);
    }
  }

  /**
   * 子类需要实现的实际阶段逻辑，保证只被调用一次。
   * @param arguments 阶段入参
   * @return 阶段执行结果
   * @throws IOException 执行失败抛出IO异常
   */
  protected abstract OUT executeStage(IN arguments) throws IOException;

  /**
   * 原子校验阶段是否重复执行，重复执行抛出非法状态异常。
   * @throws IllegalStateException 重复执行时抛出
   */
  private void executeOnlyOnce() {
    Preconditions.checkState(
        !executed.getAndSet(true),
        "Stage attempted twice");
  }

  /**
   * 获取阶段统计名称。
   * @param arguments 阶段入参
   * @return 统计名称
   */
  protected String getStageStatisticName(IN arguments) {
    return stageStatisticName;
  }

  /**
   * 获取用于报告的阶段名称，默认使用统计名称。
   * @param arguments 阶段入参
   * @return 报告用阶段名称
   */
  protected String getStageName(IN arguments) {
    return getStageStatisticName(arguments);
  }

  /**
   * 获取阶段执行时长追踪器，执行完成后非空。
   * @return 时长追踪器
   */
  public DurationTracker getStageExecutionTracker() {
    return stageExecutionTracker;
  }

  /**
   * 将本阶段执行时长添加到IO统计存储中。
   * @param iostats IO统计存储
   * @param statistic 统计名称
   */
  public void addExecutionDurationToStatistics(IOStatisticsStore iostats,
      String statistic) {
    iostats.addTimedOperation(
        statistic,
        getStageExecutionTracker().asDuration());
  }

  /**
   * 如果限流等待时长不为零，记录限流耗时到统计。
   * @param statistic 统计键名
   * @param wait 等待时长
   */
  private void noteAnyRateLimiting(String statistic, Duration wait) {
    if (!wait.isZero()) {
      getIOStatistics().addTimedOperation(
          statistic,
          wait.toMillis());
    }
  }

  /**
   * 获取存储操作回调实例。
   * @return 存储操作实例
   */
  public ManifestStoreOperations getOperations() {
    return operations;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(
        "AbstractJobOrTaskStage{");
    sb.append(isTaskStage ? "Task Stage" : "Job Stage");
    sb.append(" name='").append(name).append('\'');
    sb.append(" stage='").append(stageStatisticName).append('\'');
    sb.append('}');
    return sb.toString();
  }

  /**
   * 获取本阶段使用的全局提交配置。
   * @return 全局提交配置
   */
  protected StageConfig getStageConfig() {
    return stageConfig;
  }

  /**
   * 更新审计上下文，注入作业ID和阶段名称。
   * 辅助线程执行任务前必须调用此方法保证审计信息正确。
   * @param stage 阶段名称
   */
  protected void updateAuditContext(final String stage) {
    enterStageWorker(stageConfig.getJobId(), stage);
  }

  /**
   * 获取共享的IO统计存储实例，统计信息存放在StageConfig中全局共享。
   * @return IO统计存储
   */
  @Override
  public final IOStatisticsStore getIOStatistics() {
    return stageConfig.getIOStatistics();
  }

  /**
   * 调用进度回调通知MapReduce任务进度，防止长时间操作被误杀。
   */
  protected final void progress() {
    if (stageConfig.getProgressable() != null) {
      LOG.trace("{}: Progressing", getName());
      stageConfig.getProgressable().progress();
    }
  }

  /**
   * 获取文件状态，如果路径不存在返回null，不抛出异常。
   * @param path 目标路径
   * @return 文件状态或null
   * @throws IOException IO异常
   */
  protected final FileStatus getFileStatusOrNull(
      final Path path)
      throws IOException {
    try {
      return getFileStatus(path);
    } catch (FileNotFoundException e) {
      return null;
    }
  }

  /**
   * 获取文件状态，统计操作耗时。
   * @param path 目标路径
   * @return 文件状态
   * @throws IOException IO异常
   */
  protected final FileStatus getFileStatus(
      final Path path)
      throws IOException {
    LOG.trace("{}: getFileStatus('{}')", getName(), path);
    requireNonNull(path,
        () -> String.format("%s: Null path for getFileStatus() call", getName()));
    return trackDuration(getIOStatistics(), OP_GET_FILE_STATUS, () ->
        operations.getFileStatus(path));
  }

  /**
   * 判断路径是否为文件，统计操作耗时。
   * @param path 目标路径
   * @return 是否为文件
   * @throws IOException IO异常
   */
  protected final boolean isFile(
      final Path path)
      throws IOException {
    LOG.trace("{}: isFile('{}')", getName(), path);
    return trackDuration(getIOStatistics(), OP_IS_FILE, () -> {
      return operations.isFile(path);
    });
  }

  /**
   * 删除路径，默认使用OP_DELETE统计。
   * @param path 目标路径
   * @param recursive 是否递归删除
   * @return 删除是否成功
   * @throws IOException IO异常
   */
  public final boolean delete(
      final Path path,
      final boolean recursive)
      throws IOException {
    LOG.trace("{}: delete('{}, {}')", getName(), path, recursive);
    return delete(path, recursive, OP_DELETE);
  }

  /**
   * 删除路径，使用指定统计键更新统计。
   * @param path 目标路径
   * @param recursive 是否递归删除
   * @param statistic 统计键名
   * @return 删除是否成功
   * @throws IOException IO异常
   */
  public Boolean delete(
      final Path path,
      final boolean recursive,
      final String statistic)
      throws IOException {
    if (recursive) {
      return deleteRecursive(path, statistic);
    } else {
      return deleteFile(path, statistic);
    }
  }

  /**
   * 删除单个文件，统计操作耗时。
   * @param path 目标路径
   * @param statistic 统计键名
   * @return 删除是否成功
   * @throws IOException IO异常
   */
  public boolean deleteFile(
      final Path path,
      final String statistic)
      throws IOException {
    return trackDuration(getIOStatistics(), statistic, () ->
        operations.deleteFile(path));
  }

  /**
   * 创建目录，统计操作耗时。
   * @param path 目标路径
   * @param escalateFailure 创建失败是否抛出异常
   * @return 创建是否成功
   * @throws IOException IO异常
   */
  public final boolean mkdirs(
      final Path path,
      final boolean escalateFailure)
      throws IOException {
    LOG.trace("{}: mkdirs('{}')", getName(), path);
    return trackDuration(getIOStatistics(), OP_MKDIRS, () -> {
      boolean success = operations.mkdirs(path);
      if (!success && escalateFailure) {
        throw new PathIOException(path.toUri().toString(),
            stageStatisticName + ": mkdirs() returned false");
      }