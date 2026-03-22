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

package org.apache.hadoop.mapreduce.lib.output.committer.manifest;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsStore;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.PathOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.ManifestSuccessData;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.TaskManifest;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.AuditingIntegration;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.ManifestCommitterSupport;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.ManifestStoreOperations;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.ManifestStoreOperationsThroughFileSystem;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.stages.AbortTaskStage;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.stages.CleanupJobStage;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.stages.CommitJobStage;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.stages.CommitTaskStage;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.stages.SetupJobStage;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.stages.SetupTaskStage;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.stages.StageConfig;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.stages.StageEventCallbacks;
import org.apache.hadoop.util.functional.CloseableTaskPoolSubmitter;

import static org.apache.hadoop.fs.statistics.IOStatisticsLogging.ioStatisticsToPrettyString;
import static org.apache.hadoop.fs.statistics.IOStatisticsLogging.logIOStatisticsAtDebug;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.CAPABILITY_DYNAMIC_PARTITIONING;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.OPT_DIAGNOSTICS_MANIFEST_DIR;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.OPT_IO_PROCESSORS;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.OPT_IO_PROCESSORS_DEFAULT;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.OPT_SUMMARY_REPORT_DIR;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.COMMITTER_TASKS_COMPLETED_COUNT;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.COMMITTER_TASKS_FAILED_COUNT;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_COMMIT_FILE_RENAME_RECOVERED;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_STAGE_JOB_ABORT;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_STAGE_JOB_CLEANUP;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.DiagnosticKeys.STAGE;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.AuditingIntegration.updateCommonContextOnCommitterExit;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.AuditingIntegration.updateCommonContextOnCommitterEntry;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.ManifestCommitterSupport.createIOStatisticsStore;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.ManifestCommitterSupport.createJobSummaryFilename;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.ManifestCommitterSupport.createManifestOutcome;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.ManifestCommitterSupport.manifestPathForTask;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.stages.CleanupJobStage.cleanupStageOptionsFromConfig;

/**
 * 中间清单提交器，是MapReduce输出提交器的实现，采用清单机制处理作业输出文件提交。
 * 所有入口点都会更新线程审计上下文，记录当前阶段信息，支持除S3A外其他存储系统的审计扩展。
 * 该类为公开稳定API，是Manifest提交器的核心入口实现。
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class ManifestCommitter extends PathOutputCommitter implements
    IOStatisticsSource, StageEventCallbacks, StreamCapabilities {

  public static final Logger LOG = LoggerFactory.getLogger(
      ManifestCommitter.class);

  /**
   * 角色：任务提交器。
   */
  public static final String TASK_COMMITTER = "task committer";

  /**
   * 角色：作业提交器。
   */
  public static final String JOB_COMMITTER = "job committer";

  /**
   * 从作业/任务上下文中提取并构造的提交器配置。
   */
  private final ManifestCommitterConfig baseConfig;

  /**
   * 作业最终输出目标目录。
   */
  private final Path destinationDir;

  /**
   * 当前任务尝试的工作目录，作业级实例此值为null。
   */
  private final Path taskAttemptDir;

  /**
   * IO统计信息存储，用于收集整个提交过程的IO指标。
   */
  private final IOStatisticsStore iostatistics;

  /**
   * 作业提交成功后的成功标记数据，仅在作业成功提交后有效。
   */
  private ManifestSuccessData successReport;

  /**
   * 当前活跃阶段名称，由阶段执行回调更新。
   */
  private String activeStage;

  /**
   * 当前任务提交完成后的任务清单数据，仅任务级实例且任务提交成功后非空。
   */
  private TaskManifest taskAttemptCommittedManifest;

  /**
   * 构造Manifest提交器实例。
   * @param outputPath 输出路径
   * @param context 任务尝试上下文
   * @throws IOException 初始化失败时抛出
   */
  public ManifestCommitter(final Path outputPath,
      final TaskAttemptContext context) throws IOException {
    super(outputPath, context);
    this.destinationDir = resolveDestinationDirectory(outputPath,
        context.getConfiguration());
    this.iostatistics = createIOStatisticsStore().build();
    this.baseConfig = enterCommitter(
        context.getTaskAttemptID() != null,
        context);

    this.taskAttemptDir = baseConfig.getTaskAttemptDir();
    LOG.info("Created ManifestCommitter with JobID {},"
            + " Task Attempt {} and destination {}",
        context.getJobID(), context.getTaskAttemptID(), outputPath);
  }

  /**
   * 进入提交器入口，构造提交器配置并更新审计上下文。
   * @param isTask 是否为任务级入口
   * @param context 作业/任务上下文
   * @return 构造好的提交器配置
   */
  private ManifestCommitterConfig enterCommitter(boolean isTask,
      JobContext context) {
    ManifestCommitterConfig committerConfig =
        new ManifestCommitterConfig(
            getOutputPath(),
            isTask ? TASK_COMMITTER : JOB_COMMITTER,
            context,
            iostatistics,
            this);
    updateCommonContextOnCommitterEntry(committerConfig);
    return committerConfig;
  }

  /**
   * 通过SetupJobStage执行作业初始化。
   * @param jobContext 作业上下文
   * @throws IOException IO操作失败时抛出
   */
  @Override
  public void setupJob(final JobContext jobContext) throws IOException {
    ManifestCommitterConfig committerConfig = enterCommitter(false,
        jobContext);
    StageConfig stageConfig =
        committerConfig
            .createStageConfig()
            .withOperations(createManifestStoreOperations())
            .build();
    // 执行作业初始化
    new SetupJobStage(stageConfig)
        .apply(committerConfig.getCreateJobMarker());
    logCommitterStatisticsAtDebug();
  }

  /**
   * 通过SetupTaskStage执行任务初始化。
   * 传统FileOutputCommitter此处为空操作，依赖RecordWriter隐式创建目录，同时用目录存在标记任务需要提交。
   * @param context 任务上下文
   * @throws IOException IO操作失败时抛出
   */
  @Override
  public void setupTask(final TaskAttemptContext context)
      throws IOException {
    ManifestCommitterConfig committerConfig =
        enterCommitter(true, context);
    StageConfig stageConfig =
        committerConfig
            .createStageConfig()
            .withOperations(createManifestStoreOperations())
            .build();
    // 创建任务尝试目录，如果已存在则删除
    new SetupTaskStage(stageConfig).apply("");
    logCommitterStatisticsAtDebug();
  }

  /**
   * 始终返回true，确保即使任务无输出也能收集统计信息。
   * @param context 任务上下文
   * @return true 始终需要提交任务
   * @throws IOException IO操作失败时抛出
   */
  @Override
  public boolean needsTaskCommit(final TaskAttemptContext context)
      throws IOException {
    LOG.info("Probe for needsTaskCommit({})",
        context.getTaskAttemptID());
    return true;
  }

  /**
   * 作业提交失败后不可恢复重试，因此返回false。
   * @param jobContext 作业上下文
   * @return false 始终不支持重复提交作业
   * @throws IOException 不会抛出异常
   */
  @Override
  public boolean isCommitJobRepeatable(final JobContext jobContext)
      throws IOException {
    LOG.info("Probe for isCommitJobRepeatable({}): returning false",
        jobContext.getJobID());
    return false;
  }

  /**
   * 声明不支持任务恢复，目前该功能未实现。
   * @param jobContext 作业上下文
   * @return false 始终不支持任务恢复
   * @throws IOException 不会抛出异常
   */
  @Override
  public boolean isRecoverySupported(final JobContext jobContext)
      throws IOException {
    LOG.info("Probe for isRecoverySupported({}): returning false",
        jobContext.getJobID());
    return false;
  }

  /**
   * 不支持任务恢复，调用直接抛出异常。
   * @param taskContext 任务上下文
   * @throws IOException 始终抛出异常
   */
  @Override
  public void recoverTask(final TaskAttemptContext taskContext)
      throws IOException {
    LOG.warn("Rejecting recoverTask({}) call", taskContext.getTaskAttemptID());
    throw new IOException("Cannot recover task "
        + taskContext.getTaskAttemptID());
  }

  /**
   * 提交任务，扫描任务输出生成任务清单并保存。
   * @param context 任务上下文
   * @throws IOException IO操作失败时抛出
   */
  @Override
  public void commitTask(final TaskAttemptContext context)
      throws IOException {
    ManifestCommitterConfig committerConfig = enterCommitter(true,
        context);
    try {
      StageConfig stageConfig = committerConfig.createStageConfig()
          .withOperations(createManifestStoreOperations())
          .build();
      taskAttemptCommittedManifest = new CommitTaskStage(stageConfig)
          .apply(null).getTaskManifest();
      iostatistics.incrementCounter(COMMITTER_TASKS_COMPLETED_COUNT, 1);
    } catch (IOException e) {
      iostatistics.incrementCounter(COMMITTER_TASKS_FAILED_COUNT, 1);
      throw e;
    } finally {
      logCommitterStatisticsAtDebug();
      updateCommonContextOnCommitterExit();
    }

  }

  /**
   * 中止任务，清理任务尝试工作目录。
   * @param context 任务上下文
   * @throws IOException 删除操作失败时抛出
   */
  @Override
  public void abortTask(final TaskAttemptContext context)
      throws IOException {
    ManifestCommitterConfig committerConfig = enterCommitter(true,
        context);
    try {
      new AbortTaskStage(
          committerConfig.createStageConfig()
              .withOperations(createManifestStoreOperations())
              .build())
          .apply(false);
    } finally {
      logCommitterStatisticsAtDebug();
      updateCommonContextOnCommitterExit();
    }
  }

  /**
   * 获取或创建作业成功报告实例，如果为空则初始化新实例。
   * @param committerConfig 提交器配置
   * @return 作业成功报告实例，非空
   */
  private ManifestSuccessData getOrCreateSuccessData(
      ManifestCommitterConfig committerConfig) {
    if (successReport == null) {
      successReport = createManifestOutcome(
          committerConfig.createStageConfig(), activeStage);
    }
    return successReport;
  }

  /**
   * 执行整个作业的提交流程：加载所有任务清单、准备目标目录、移动文件到最终位置、清理临时目录。
   * @param jobContext 作业上下文
   * @throws IOException 任何提交阶段失败时抛出
   */
  @Override
  public void commitJob(final JobContext jobContext) throws IOException {

    ManifestCommitterConfig committerConfig = enterCommitter(false, jobContext);

    // 初始化成功报告，如果后续提交流程失败，该初始报告仍会保存到报告目录
    ManifestSuccessData marker = getOrCreateSuccessData(committerConfig);
    IOException failure = null;
    // 创建IO线程池和存储操作实例，自动关闭
    try (CloseableTaskPoolSubmitter ioProcs =
             committerConfig.createSubmitter();
         ManifestStoreOperations storeOperations = createManifestStoreOperations()) {
      // 创建所有阶段共享的阶段配置
      StageConfig stageConfig = committerConfig.createStageConfig()
          .withOperations(storeOperations)
          .withIOProcessors(ioProcs)
          .build();

      // 执行作业提交全流程，包括清理和验证
      final Configuration conf = jobContext.getConfiguration();
      CommitJobStage.Result result = new CommitJobStage(stageConfig).apply(
          new CommitJobStage.Arguments(
              committerConfig.getCreateJobMarker(),
              committerConfig.getValidateOutput(),
              conf.getTrimmed(OPT_DIAGNOSTICS_MANIFEST_DIR, ""),
              cleanupStageOptionsFromConfig(
                  OP_STAGE_JOB_CLEANUP, conf)
          ));
      marker = result.getJobSuccessData();
      // 更新缓存的成功报告
      setSuccessReport(marker);
      // 记录IO处理器线程数到诊断信息，方便问题排查
      marker.putDiagnostic(OPT_IO_PROCESSORS,
          conf.get(OPT_IO_PROCESSORS, Long.toString(OPT_IO_PROCESSORS_DEFAULT)));
    } catch (IOException e) {
      // 记录失败信息供摘要报告使用
      failure = e;
      // 重新抛出异常，通知框架作业提交失败
      throw e;
    } finally {
      // 即使失败也保存摘要报告
      maybeSaveSummary(activeStage,
          committerConfig,
          marker,
          failure,
          true,
          true);
      // 打印作业提交统计信息
      LOG.info("{}: Job Commit statistics {}",
          committerConfig.getName(),
          ioStatisticsToPrettyString(iostatistics));
      // 输出重命名恢复警告，如果存在恢复操作
      final Long recoveries = iostatistics.counters().get(OP_COMMIT_FILE_RENAME_RECOVERED);
      if (recoveries != null && recoveries > 0) {
        LOG.warn("{}: rename failures were recovered from. Number of recoveries: {}",
            committerConfig.getName(), recoveries);
      }
      updateCommonContextOnCommitterExit();
    }
  }

  /**
   * 中止作业，执行清理并保存作业报告（如果启用报告）。
   * @param jobContext 作业上下文
   * @param state 作业最终状态
   * @throws IOException 清理失败时抛出，报告保存失败会被吞噬
   */
  @Override
  public void abortJob(final JobContext jobContext,
      final JobStatus.State state)
      throws IOException {
    LOG.info("Aborting Job {} in state {}", jobContext.getJobID(), state);
    ManifestCommitterConfig committerConfig = enterCommitter(false,
        jobContext);
    ManifestSuccessData report = getOrCreateSuccessData(
        committerConfig);
    IOException failure = null;

    try {
      executeClean