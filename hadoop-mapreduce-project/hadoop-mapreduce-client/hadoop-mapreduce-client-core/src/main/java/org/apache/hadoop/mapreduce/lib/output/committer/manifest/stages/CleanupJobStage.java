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
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.util.DurationInfo;
import org.apache.hadoop.util.functional.RemoteIterators;
import org.apache.hadoop.util.functional.TaskPool;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.fs.statistics.IOStatisticsSupport.retrieveIOStatistics;
import static org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter.FILEOUTPUTCOMMITTER_CLEANUP_FAILURES_IGNORED;
import static org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter.FILEOUTPUTCOMMITTER_CLEANUP_FAILURES_IGNORED_DEFAULT;
import static org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter.FILEOUTPUTCOMMITTER_CLEANUP_SKIPPED;
import static org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter.FILEOUTPUTCOMMITTER_CLEANUP_SKIPPED_DEFAULT;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.OPT_CLEANUP_PARALLEL_DELETE;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.OPT_CLEANUP_PARALLEL_DELETE_BASE_FIRST;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.OPT_CLEANUP_PARALLEL_DELETE_BASE_FIRST_DEFAULT;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.OPT_CLEANUP_PARALLEL_DELETE_DIRS_DEFAULT;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_DELETE_DIR;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_STAGE_JOB_CLEANUP;

/**
 * 基于Manifest提交协议的作业临时目录清理阶段，通过并行删除加速清理过程
 * 针对云存储（GCS、Azure ADLS）的目录删除性能问题进行了优化：
 * 1. 支持并行删除任务尝试目录，加速大作业清理，降低云存储目录删除超时风险
 * 2. 支持先尝试删除根临时目录，成功则直接完成，失败后回退到并行删除
 * 3. 支持完全禁用清理，满足多作业同时输出到同一目录的场景
 * 4. 支持忽略清理失败异常，避免清理失败导致整个作业失败
 */
public class CleanupJobStage extends
    AbstractJobOrTaskStage<
            CleanupJobStage.Arguments,
            CleanupJobStage.Result> {

  private static final Logger LOG = LoggerFactory.getLogger(
      CleanupJobStage.class);

  /**
   * 已删除目录计数，线程安全
   */
  private final AtomicInteger deleteDirCount = new AtomicInteger();

  /**
   * 删除失败计数，线程安全
   */
  private final AtomicInteger deleteFailureCount = new AtomicInteger();

  /**
   * 最后一次删除异常，如果删除失败计数大于0则不为空
   */
  private IOException lastDeleteException;

  /**
   * 阶段统计名称，从入参获取
   */
  private String stageName = OP_STAGE_JOB_CLEANUP;

  /**
   * 构造函数，初始化清理阶段
   * @param stageConfig 阶段配置
   */
  public CleanupJobStage(final StageConfig stageConfig) {
    super(false, stageConfig, OP_STAGE_JOB_CLEANUP, true);
  }

  /**
   * 从入参获取阶段统计名称
   * @param arguments 阶段入参
   * @return 阶段统计名称
   */
  @Override
  protected String getStageStatisticName(Arguments arguments) {
    return arguments.statisticName;
  }

  /**
   * 执行作业临时目录树清理
   * @param args 清理阶段参数
   * @return 清理结果
   * @throws IOException 如果不抑制异常，清理失败时抛出
   */
  @Override
  protected Result executeStage(
      final Arguments args)
      throws IOException {
    stageName = getStageName(args);
    // 待清理目录：目标输出目录下的_temporary
    final Path baseDir = requireNonNull(getStageConfig().getOutputTempSubDir());
    LOG.debug("{}: Cleanup of directory {} with {}", getName(), baseDir, args);
    if (!args.enabled) {
      LOG.info("{}: Cleanup of {} disabled", getName(), baseDir);
      return new Result(Outcome.DISABLED, baseDir,
          0, null);
    }
    // 先检查目录是否存在，不存在直接返回
    if (getFileStatusOrNull(baseDir) == null) {
      return new Result(Outcome.NOTHING_TO_CLEAN_UP,
          baseDir,
          0, null);
    }

    Outcome outcome = null;
    IOException exception = null;
    boolean baseDirDeleted = false;


    LOG.info("{}: Deleting job directory {}", getName(), baseDir);
    final long directoryCount = args.directoryCount;
    if (directoryCount > 0) {
      // 打印预期目录数量，帮助排查云存储超时问题
      LOG.info("{}: Expected directory count: {}", getName(), directoryCount);
    }

    progress();
    // 如果开启了并行删除任务尝试目录
    if (args.deleteTaskAttemptDirsInParallel) {


      if (args.parallelDeleteAttemptBaseDeleteFirst) {
        // 先尝试直接删除根临时目录，如果成功可以减少IO操作
        // 在云存储上可能超时，超时后自动回退到并行删除
        try (DurationInfo info = new DurationInfo(LOG, true,
            "Initial delete of %s", baseDir)) {
          exception = deleteOneDir(baseDir);
          if (exception == null) {
            // 删除成功，直接结束流程
            outcome = Outcome.DELETED;
            baseDirDeleted = true;
          } else {
            // 删除失败，打印日志后回退到并行删除
            LOG.warn("{}: Exception on initial attempt at deleting base dir {}"
                    + " with directory count {}. Falling back to parallel delete",
                getName(), baseDir, directoryCount, exception);
          }
        }
      }
      if (!baseDirDeleted) {
        // 根目录未删除（未尝试或删除失败），执行任务尝试目录并行删除
        Path taskSubDir
            = getStageConfig().getJobAttemptTaskSubDir();
        try (DurationInfo info = new DurationInfo(LOG, true,
            "parallel deletion of task attempts in %s",
            taskSubDir)) {
          // 过滤出任务尝试目录
          RemoteIterator<FileStatus> dirs =
              RemoteIterators.filteringRemoteIterator(
                  listStatusIterator(taskSubDir),
                  FileStatus::isDirectory);
          // 使用线程池并行删除每个目录
          TaskPool.foreach(dirs)
              .executeWith(getIOProcessors())
              .stopOnFailure()
              .suppressExceptions(false)
              .run(this::rmTaskAttemptDir);
          // 聚合IO统计信息
          getIOStatistics().aggregate((retrieveIOStatistics(dirs)));

          if (getLastDeleteException() != null) {
            // 有删除失败，抛出异常
            throw getLastDeleteException();
          } else {
            // 并行删除成功
            outcome = Outcome.PARALLEL_DELETE;
          }
        } catch (FileNotFoundException ex) {
          // 任务尝试目录不存在，不算失败
          LOG.debug("{}: Task attempt dir {} not found", getName(), taskSubDir);
          outcome = Outcome.DELETED;
        } catch (IOException ex) {
          // 列举或删除过程中发生异常，打印日志后继续删除根目录
          LOG.info(
              "{}: Exception while listing/deleting task attempts under {}; continuing",
              getName(),
              taskSubDir, ex);
        }
      }
    }
    // 如果根目录还没删除，最后执行一次根目录删除
    if (!baseDirDeleted) {
      exception = deleteOneDir(baseDir);
      if (exception != null) {
        // 最终删除失败，记录结果
        LOG.warn("{}: Exception on final attempt at deleting base dir {}"
                + " with directory count {}",
            getName(), baseDir, directoryCount, exception);
        outcome = Outcome.FAILURE;
      } else {
        // 删除成功，如果之前没有记录结果，标记为简单删除成功
        if (outcome == null) {
          outcome = Outcome.DELETED;
        }
      }
    }

    Result result = new Result(
        outcome,
        baseDir,
        deleteDirCount.get(),
        exception);
    // 如果清理失败且不抑制异常，抛出异常
    if (!result.succeeded() && !args.suppressExceptions) {
      result.maybeRethrowException();
    }

    return result;
  }

  /**
   * 在并行任务中删除单个任务尝试目录
   * 更新审计上下文和进度，保存第一个捕获的删除异常
   * @param status 待删除目录状态
   * @throws IOException 删除失败（不抑制异常时抛出）
   */
  private void rmTaskAttemptDir(FileStatus status) throws IOException {
    // 更新审计上下文为当前阶段名称
    updateAuditContext(stageName);
    // 更新作业进度，避免长时间删除导致作业被误杀
    progress();
    deleteOneDir(status.getPath());
  }

  /**
   * 删除单个目录，记录删除失败信息
   * @param dir 待删除目录路径
   * @return 如果删除失败返回异常，成功返回null
   * @throws IOException 如果不抑制异常则抛出
   */
  private IOException deleteOneDir(final Path dir)
      throws IOException {

    deleteDirCount.incrementAndGet();
    return noteAnyDeleteFailure(
        deleteRecursiveSuppressingExceptions(dir, OP_DELETE_DIR));
  }

  /**
   * 记录删除失败异常，线程安全
   * @param ex 异常，null表示无异常
   * @return 原异常
   */
  private synchronized IOException noteAnyDeleteFailure(IOException ex) {
    if (ex != null) {
      deleteFailureCount.incrementAndGet();
      lastDeleteException = ex;
    }
    return ex;
  }

  /**
   * 获取最后一次删除异常，线程安全
   * @return 最后一次删除异常，无异常返回null
   */
  public synchronized IOException getLastDeleteException() {
    return lastDeleteException;
  }

  /**
   * 清理阶段参数类，保存清理配置
   */
  public static final class Arguments {

    /**
     * 统计名称
     */
    private final String statisticName;

    /** 是否启用清理 */
    private final boolean enabled;

    /** 是否并行删除任务尝试目录 */
    private final boolean deleteTaskAttemptDirsInParallel;

    /** 并行删除模式下是否先尝试删除根目录 */
    private final boolean parallelDeleteAttemptBaseDeleteFirst;

    /** 是否忽略清理失败异常 */
    private final boolean suppressExceptions;

    /** 待清理目录总数，0表示未知 */
    private long directoryCount;

    /**
     * 构造清理阶段参数
     * @param statisticName 统计名称
     * @param enabled 是否启用清理
     * @param deleteTaskAttemptDirsInParallel 是否并行删除任务尝试目录
     * @param parallelDeleteAttemptBaseDeleteFirst 是否先尝试删除根目录
     * @param suppressExceptions 是否抑制异常
     * @param directoryCount 目录数量，0表示未知
     */
    public Arguments(
        final String statisticName,
        final boolean enabled,
        final boolean deleteTaskAttemptDirsInParallel,
        final boolean parallelDeleteAttemptBaseDeleteFirst,
        final boolean suppressExceptions,
        long directoryCount) {
      this.statisticName = statisticName;
      this.enabled = enabled;
      this.deleteTaskAttemptDirsInParallel = deleteTaskAttemptDirsInParallel;
      this.suppressExceptions = suppressExceptions;
      this.parallelDeleteAttemptBaseDeleteFirst = parallelDeleteAttemptBaseDeleteFirst;
      this.directoryCount = directoryCount;
    }

    public String getStatisticName() {
      return statisticName;
    }

    public boolean isEnabled() {
      return enabled;
    }

    public boolean isDeleteTaskAttemptDirsInParallel() {
      return deleteTaskAttemptDirsInParallel;
    }

    public boolean isSuppressExceptions() {
      return suppressExceptions;
    }

    public boolean isParallelDeleteAttemptBaseDeleteFirst() {
      return parallelDeleteAttemptBaseDeleteFirst;
    }

    public long getDirectoryCount() {
      return directoryCount;
    }

    public void setDirectoryCount(final long directoryCount) {
      this.directoryCount = directoryCount;
    }

    @Override
    public String toString() {
      return "Arguments{" +
          "statisticName='" + statisticName + '\''
          + ", enabled=" + enabled
          + ", deleteTaskAttemptDirsInParallel="
          + deleteTaskAttemptDirsInParallel
          + ", parallelDeleteAttemptBaseDeleteFirst=" + parallelDeleteAttemptBaseDeleteFirst
          + ", suppressExceptions=" + suppressExceptions
          + '}';
    }
  }

  /**
   * 预定义的禁用清理参数实例
   */
  public static final Arguments DISABLED = new Arguments(OP_STAGE_JOB_CLEANUP,
      false,
      false,
      false,
      false,
      0);

  /**
   * 从Hadoop配置构建清理阶段参数，读取FileOutputCommitter和ManifestCommitter配置项
   * @param statisticName 统计名称
   * @param conf Hadoop配置
   * @return 清理阶段参数
   */
  public static Arguments cleanupStageOptionsFromConfig(
      String statisticName, Configuration conf) {

    boolean enabled = !conf.getBoolean(FILEOUTPUTCOMMITTER_CLEANUP_SKIPPED,
        FILEOUTPUTCOMMITTER_CLEANUP_SKIPPED_DEFAULT);
    boolean suppressExceptions = conf.getBoolean(
        FILEOUTPUTCOMMITTER_CLEANUP_FAILURES_IGNORED,
        FILEOUTPUTCOMMITTER_CLEANUP_FAILURES_IGNORED_DEFAULT);
    boolean deleteTaskAttemptDirsInParallel = conf.getBoolean(
        OPT_CLEANUP_PARALLEL_DELETE,
        OPT_CLEANUP_PARALLEL_DELETE_DIRS_DEFAULT);
    boolean parallelDeleteAttemptBaseDeleteFirst = conf.getBoolean(
        OPT_CLEANUP_PARALLEL_DELETE_BASE_FIRST,
        OPT_CLEANUP_PARALLEL_DELETE_BASE_FIRST_DEFAULT);
    return new Arguments(
        statisticName,
        enabled,
        deleteTaskAttemptDirsInParallel,
        parallelDeleteAttemptBaseDeleteFirst,
        suppressExceptions,
        0);
  }

  /**
   * 清理结果枚举，定义所有可能的清理结果
   */
  public enum Outcome {
    /** 清理已禁用 */
    DISABLED("Disabled", false),
    /** 没有需要清理的目录 */
    NOTHING_TO_CLEAN_UP("Nothing to clean up", true),
    /** 任务尝试目录并行删除完成 */
    PARALLEL_DELETE("Parallel Delete of Task Attempt Directories", true),
    /** 根目录删除成功 */
    DELETED("Delete of job directory", true),
    /** 删除失败 */
    FAILURE("Delete failed", false);

    private final String description;

    private final boolean success;

    Outcome(String description, boolean success) {
      this.description = description;
      this.success = success;
    }

    @Override
    public String toString() {
      return "Outcome{" + name() +
          " '" + description + '\'' +
          "}";
    }

    /**
     * 获取结果描述
     * @return 描述文本，用于日志输出
     */
    public String getDescription() {
      return description;
    }

    /**
     * 是否清理成功
     * @return true表示清理成功
     */
    public boolean isSuccess() {
      return success;
    }
  }

  /**
   * 清理结果类，保存清理执行结果信息
   */
  public static final class Result {

    /** 清理结果枚举 */
    private final Outcome outcome;

    /** 被清理的目录 */
    private final Path directory;

    /** 所有线程执行的删除调用总数 */
    private final int deleteCalls;

    /** 清理过程中抛出的异常 */
    private final IOException exception;

    /**
     * 构造清理结果
     * @param outcome 结果枚举
     * @param directory