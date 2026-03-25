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

import java.io.File;
import java.io.IOException;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.tuple.Triple;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.statistics.IOStatisticsSnapshot;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsStore;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.ManifestSuccessData;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.LoadedManifestData;

import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.COMMITTER_BYTES_COMMITTED_COUNT;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.COMMITTER_FILES_COMMITTED_COUNT;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.COMMITTER_TASK_DIRECTORY_COUNT_MEAN;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_STAGE_JOB_COMMIT;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_STAGE_JOB_CREATE_TARGET_DIRS;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_STAGE_JOB_LOAD_MANIFESTS;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterStatisticNames.OP_STAGE_JOB_RENAME_FILES;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.DiagnosticKeys.MANIFESTS;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.ManifestCommitterSupport.addHeapInformation;

/**
 * 文件级注释：Manifest提交器作业提交主阶段，协调多个子阶段完成整个MapReduce作业输出提交流程，
 * 核心流程包括：加载任务清单、创建目标目录、重命名任务输出到最终位置、保存成功标记、清理临时文件、输出校验。
 * 
 * 作业提交阶段，输入为提交参数（是否创建标记、是否校验输出等），输出为提交结果。
 */
public class CommitJobStage extends
    AbstractJobOrTaskStage<
            CommitJobStage.Arguments,
            CommitJobStage.Result> {

  private static final Logger LOG = LoggerFactory.getLogger(
      CommitJobStage.class);

  /**
   * 构造作业提交阶段实例。
   * @param stageConfig 阶段配置信息
   */
  public CommitJobStage(final StageConfig stageConfig) {
    super(false, stageConfig, OP_STAGE_JOB_COMMIT, true);
  }

  @Override
  /**
   * 执行作业提交全流程，协调各个子阶段按顺序完成输出提交。
   * @param arguments 作业提交参数
   * @return 作业提交结果
   * @throws IOException 执行过程中的IO异常
   */
  protected CommitJobStage.Result executeStage(
      final CommitJobStage.Arguments arguments) throws IOException {

    LOG.info("{}: Committing job \"{}\". resilient commit supported = {}",
        getName(),
        getJobId(),
        storeSupportsResilientCommit());

    // 跟踪已加载的清单数据，用于finally块清理临时文件
    LoadedManifestData loadedManifestData = null;

    try {
      boolean createMarker = arguments.isCreateMarker();
      IOStatisticsSnapshot heapInfo = new IOStatisticsSnapshot();
      addHeapInformation(heapInfo, "setup");
      // 加载所有任务输出清单
      final StageConfig stageConfig = getStageConfig();
      LoadManifestsStage.Result result = new LoadManifestsStage(stageConfig).apply(
          new LoadManifestsStage.Arguments(
              File.createTempFile("manifest", ".list"),
              /* do not cache manifests */
              stageConfig.getWriterQueueCapacity()));
      LoadManifestsStage.SummaryInfo loadedManifestSummary = result.getSummary();
      loadedManifestData = result.getLoadedManifestData();

      LOG.debug("{}: Job Summary {}", getName(), loadedManifestSummary);
      LOG.info("{}: Committing job with file count: {}; total size {} bytes",
          getName(),
          loadedManifestSummary.getFileCount(),
          String.format("%,d", loadedManifestSummary.getTotalFileSize()));
      addHeapInformation(heapInfo, OP_STAGE_JOB_LOAD_MANIFESTS);

      // 将加载清单阶段的IO统计信息聚合到全局统计中
      IOStatisticsStore iostats = getIOStatistics();
      iostats.aggregate(loadedManifestSummary.getIOStatistics());

      // 创建所有需要的目标输出目录
      final CreateOutputDirectoriesStage.Result dirStageResults =
          new CreateOutputDirectoriesStage(stageConfig)
              .apply(loadedManifestData.getDirectories());
      addHeapInformation(heapInfo, OP_STAGE_JOB_CREATE_TARGET_DIRS);

      // 批量重命名所有任务输出文件到最终输出路径
      ManifestSuccessData successData;
      successData = new RenameFilesStage(stageConfig).apply(
          Triple.of(loadedManifestData,
              dirStageResults.getCreatedDirectories(),
              stageConfig.getSuccessMarkerFileLimit()));
      if (LOG.isDebugEnabled()) {
        LOG.debug("{}: _SUCCESS file summary {}", getName(), successData.toJson());
      }
      addHeapInformation(heapInfo, OP_STAGE_JOB_RENAME_FILES);

      // 更新已提交文件总数和总字节数统计，覆盖任务聚合结果
      iostats.setCounter(
          COMMITTER_FILES_COMMITTED_COUNT,
          loadedManifestSummary.getFileCount());
      iostats.setCounter(
          COMMITTER_BYTES_COMMITTED_COUNT,
          loadedManifestSummary.getTotalFileSize());
      successData.snapshotIOStatistics(iostats);
      successData.getIOStatistics().aggregate(heapInfo);

      // 若配置了清单重命名目录，将所有任务清单移动到指定目录归档
      final String manifestRenameDir = arguments.getManifestRenameDir();
      if (isNotBlank(manifestRenameDir)) {
        Path manifestRenamePath = new Path(
            new Path(manifestRenameDir),
            getJobId());
        LOG.info("{}: Renaming manifests to {}", getName(), manifestRenamePath);
        try {
          renameDir(getTaskManifestDir(), manifestRenamePath);

          // 将归档路径保存到诊断信息中
          successData.getDiagnostics().put(MANIFESTS, manifestRenamePath.toUri().toString());
        } catch (IOException | IllegalArgumentException e) {
          // 清单重命名失败仅记录警告，不中断整个提交流程
          LOG.warn("{}: Failed to rename manifests to {}", getName(), manifestRenamePath, e);
        }
      }

      // 若开启了成功标记选项，保存_SUCCESS文件
      Path successPath = null;
      if (createMarker) {
        successPath = new SaveSuccessFileStage(stageConfig)
            .apply(successData);
        LOG.debug("{}: Saving _SUCCESS file to {}", getName(), successPath);
      }

      // 执行作业清理流程
      final CleanupJobStage.Arguments cleanupArguments = arguments.getCleanupArguments();
      // 设置任务目录数量统计用于清理
      cleanupArguments.setDirectoryCount(iostats.counters()
          .getOrDefault(COMMITTER_TASK_DIRECTORY_COUNT_MEAN, 0L));

      new CleanupJobStage(stageConfig).apply(cleanupArguments);

      // 若开启了输出校验，对所有已重命名文件进行校验
      if (arguments.isValidateOutput()) {
        LOG.info("{}: Validating output.", getName());
        new ValidateRenamedFilesStage(stageConfig)
            .apply(loadedManifestData.getEntrySequenceData());
      }

      // 恢复当前阶段为作业提交，确保统计信息归属正确
      stageConfig.enterStage(getStageName(arguments));

      // 返回提交结果
      return new Result(successPath, successData);
    } finally {
      // 清理临时文件，忽略清理失败
      if (loadedManifestData != null) {
        loadedManifestData.deleteEntrySequenceFile();
      }

    }
  }

  /**
   * 作业提交阶段参数类，封装整个作业提交流程的配置选项。
   */
  public static final class Arguments {

    /** 是否创建_SUCCESS成功标记文件 */
    private final boolean createMarker;

    /** 是否对输出文件执行完整性校验 */
    private final boolean validateOutput;

    /** 任务清单归档目录，可为空表示不归档 */
    private final String manifestRenameDir;

    /** 作业清理阶段参数 */
    private final CleanupJobStage.Arguments cleanupArguments;

    /**
     * 构造作业提交参数实例。
     * @param createMarker 是否创建_SUCCESS标记
     * @param validateOutput 是否执行输出校验
     * @param manifestRenameDir 清单归档目录，可为null
     * @param cleanupArguments 清理阶段参数
     */
    public Arguments(
        boolean createMarker,
        boolean validateOutput,
        @Nullable String manifestRenameDir,
        CleanupJobStage.Arguments cleanupArguments) {

      this.createMarker = createMarker;
      this.validateOutput = validateOutput;
      this.manifestRenameDir = manifestRenameDir;
      this.cleanupArguments = requireNonNull(cleanupArguments);
    }

    public boolean isCreateMarker() {
      return createMarker;
    }

    public boolean isValidateOutput() {
      return validateOutput;
    }

    public String getManifestRenameDir() {
      return manifestRenameDir;
    }

    public CleanupJobStage.Arguments getCleanupArguments() {
      return cleanupArguments;
    }
  }

  /**
   * 作业提交阶段结果类，封装提交结果信息。
   */
  public static final class Result {
    /** 作业提交成功统计数据 */
    private final ManifestSuccessData jobSuccessData;

    /** _SUCCESS文件路径，未保存则为null */
    private final Path successPath;

    /**
     * 构造提交结果实例。
     * @param successPath _SUCCESS文件路径
     * @param jobSuccessData 作业成功统计数据
     */
    public Result(final Path successPath,
        ManifestSuccessData jobSuccessData) {
      this.successPath = successPath;
      this.jobSuccessData = jobSuccessData;
    }

    public ManifestSuccessData getJobSuccessData() {
      return jobSuccessData;
    }

    public Path getSuccessPath() {
      return successPath;
    }
  }
}