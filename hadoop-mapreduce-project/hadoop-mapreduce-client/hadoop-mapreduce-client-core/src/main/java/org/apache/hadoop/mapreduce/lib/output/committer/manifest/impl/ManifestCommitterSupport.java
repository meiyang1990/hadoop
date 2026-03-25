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

package org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl;

import java.io.IOException;
import java.time.ZonedDateTime;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.EtagSource;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.statistics.IOStatisticsAggregator;
import org.apache.hadoop.fs.statistics.IOStatisticsSetters;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsStoreBuilder;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.ManifestSuccessData;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.TaskManifest;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.stages.StageConfig;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.iostatisticsStore;
import static org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter.PENDING_DIR_NAME;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.INITIAL_APP_ATTEMPT_ID;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.JOB_ATTEMPT_DIR_FORMAT_STR;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.JOB_DIR_FORMAT_STR;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.JOB_ID_SOURCE_MAPREDUCE;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.JOB_TASK_ATTEMPT_SUBDIR;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.JOB_TASK_MANIFEST_SUBDIR;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.MANIFEST_COMMITTER_CLASSNAME;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.MANIFEST_SUFFIX;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.OPT_STORE_OPERATIONS_CLASS;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.SPARK_WRITE_UUID;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.SUMMARY_FILENAME_FORMAT;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.TMP_SUFFIX;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.DiagnosticKeys.FREE_MEMORY;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.DiagnosticKeys.HEAP_MEMORY;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.DiagnosticKeys.PRINCIPAL;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.DiagnosticKeys.STAGE;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.DiagnosticKeys.TOTAL_MEMORY;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.InternalConstants.COUNTER_STATISTICS;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.InternalConstants.DURATION_STATISTICS;

/**
 * 文件级注释：Manifest提交器的通用工具支持类，提供IO统计、路径构建、对象创建等公共工具方法，
 * 被Manifest提交器各个阶段和组件调用，是整个Manifest提交器模块的底层工具支撑。
 * 
 * Class for manifest committer support util methods.
 */

@InterfaceAudience.Private
public final class ManifestCommitterSupport {

  private ManifestCommitterSupport() {
  }

  /**
   * 创建预配置了标准统计项的IO统计存储构建器，用于统一收集提交过程中的计数和耗时统计。
   * @return 预配置了标准统计项的存储构建器
   */
  public static IOStatisticsStoreBuilder createIOStatisticsStore() {

    final IOStatisticsStoreBuilder store
        = iostatisticsStore();

    store.withSampleTracking(COUNTER_STATISTICS);
    store.withDurationTracking(DURATION_STATISTICS);
    return store;
  }

  /**
   * 如果传入对象是IO统计源，则将其统计信息聚合到 aggregator 中，适配不同来源的统计数据。
   * @param ios IO统计聚合器
   * @param o 待检查的源对象
   */
  public static void maybeAddIOStatistics(IOStatisticsAggregator ios,
      Object o) {
    if (o instanceof IOStatisticsSource) {
      ios.aggregate(((IOStatisticsSource) o).getIOStatistics());
    }
  }

  /**
   * 从作业配置中构建Job UUID，优先从配置中获取Spark写入UUID，不存在则使用MapReduce JobID，用于唯一标识当前作业。
   * @param conf 作业/任务配置
   * @param jobId MapReduce作业ID
   * @return (Job UUID, 来源标识)对
   */
  public static Pair<String, String> buildJobUUID(Configuration conf,
      JobID jobId) {
    String jobUUID = conf.getTrimmed(SPARK_WRITE_UUID, "");
    if (jobUUID.isEmpty()) {
      jobUUID = jobId.toString();
      return Pair.of(jobUUID, JOB_ID_SOURCE_MAPREDUCE);
    } else {
      return Pair.of(jobUUID, SPARK_WRITE_UUID);
    }
  }

  /**
   * 获取作业待提交目录的根路径，所有暂未提交的作业尝试文件都存放在此路径下。
   * @param out 输出根目录
   * @return 待提交作业尝试根路径
   */
  public static Path getPendingJobAttemptsPath(Path out) {
    return new Path(out, PENDING_DIR_NAME);
  }

  /**
   * 从作业上下文中获取当前应用尝试ID。
   * @param context 作业上下文
   * @return 当前应用尝试ID
   */
  public static int getAppAttemptId(JobContext context) {
    return getAppAttemptId(context.getConfiguration());
  }

  /**
   * 从配置中获取当前应用尝试ID，未配置时返回初始值0，支持MapReduce和Spark两种场景：MapReduce会设置实际尝试ID，Spark始终为0。
   * @param conf 作业配置
   * @return 当前应用尝试ID
   */
  public static int getAppAttemptId(Configuration conf) {
    return conf.getInt(MRJobConfig.APPLICATION_ATTEMPT_ID,
        INITIAL_APP_ATTEMPT_ID);
  }

  /**
   * 构造任务完成后清单文件的最终路径。
   * @param manifestDir 清单文件根目录
   * @param taskId 任务ID
   * @return 任务清单最终路径
   */
  public static Path manifestPathForTask(Path manifestDir, String taskId) {

    return new Path(manifestDir, taskId + MANIFEST_SUFFIX);
  }

  /**
   * 构造任务尝试写入过程中临时清单文件的路径，避免重命名失败导致数据损坏，写入完成后再重命名到最终路径。
   * @param manifestDir 清单文件根目录
   * @param taskAttemptId 任务尝试ID
   * @return 临时清单文件路径
   */
  public static Path manifestTempPathForTaskAttempt(Path manifestDir,
      String taskAttemptId) {
    return new Path(manifestDir,
        taskAttemptId + MANIFEST_SUFFIX + TMP_SUFFIX);
  }

  /**
   * 根据任务尝试阶段配置，创建并初始化一个新的任务清单对象，填充任务和作业基础信息。
   * @param stageConfig 阶段配置，必须是任务尝试级配置
   * @return 初始化完成的任务清单对象
   */
  public static TaskManifest createTaskManifest(StageConfig stageConfig) {
    final TaskManifest manifest = new TaskManifest();
    manifest.setTaskAttemptID(stageConfig.getTaskAttemptId());
    manifest.setTaskID(stageConfig.getTaskId());
    manifest.setJobId(stageConfig.getJobId());
    manifest.setJobAttemptNumber(stageConfig.getJobAttemptNumber());
    manifest.setTaskAttemptDir(
        stageConfig.getTaskAttemptDir().toUri().toString());
    return manifest;
  }

  /**
   * 创建作业提交结果清单，填充基本诊断信息：时间戳、主机名、当前用户名、当前阶段等，用于作业成功后标记结果。
   * @param stageConfig 阶段配置
   * @param stage 当前执行阶段名称
   * @return 初始化完成的结果清单对象
   */
  public static ManifestSuccessData createManifestOutcome(
      StageConfig stageConfig, String stage) {
    final ManifestSuccessData outcome = new ManifestSuccessData();
    outcome.setJobId(stageConfig.getJobId());
    outcome.setJobIdSource(stageConfig.getJobIdSource());
    outcome.setCommitter(MANIFEST_COMMITTER_CLASSNAME);
    // 记录提交时间戳
    outcome.setTimestamp(System.currentTimeMillis());
    final ZonedDateTime now = ZonedDateTime.now();
    outcome.setDate(now.toString());
    outcome.setHostname(NetUtils.getLocalHostname());
    // 添加额外诊断信息，便于问题排查
    // 后续版本可以加入追踪Span信息
    try {
      outcome.putDiagnostic(PRINCIPAL,
          UserGroupInformation.getCurrentUser().getShortUserName());
    } catch (IOException ignored) {
      // 获取用户信息失败，跳过该诊断项
    }
    outcome.putDiagnostic(STAGE, stage);
    return outcome;
  }

  /**
   * 将当前JVM堆内存信息作为计量指标添加到IO统计中，便于监控提交过程中的内存使用情况。
   * @param ioStatisticsSetters 统计指标设置器
   * @param stage 当前阶段名称，作为指标前缀
   */
  public static void addHeapInformation(IOStatisticsSetters ioStatisticsSetters,
      String stage) {
    final long totalMemory = Runtime.getRuntime().totalMemory();
    final long freeMemory = Runtime.getRuntime().freeMemory();
    final String prefix = "stage.";
    ioStatisticsSetters.setGauge(prefix + stage + "." + TOTAL_MEMORY, totalMemory);
    ioStatisticsSetters.setGauge(prefix + stage + "." + FREE_MEMORY, freeMemory);
    ioStatisticsSetters.setGauge(prefix + stage + "." + HEAP_MEMORY, totalMemory - freeMemory);
  }

  /**
   * 根据作业ID生成作业汇总报告文件名。
   * @param jobId 作业ID
   * @return 汇总报告文件名
   */
  public static String createJobSummaryFilename(String jobId) {
    return String.format(SUMMARY_FILENAME_FORMAT, jobId);
  }

  /**
   * 从FileStatus中提取etag，只有当FileStatus实现EtagSource接口时才能提取，用于文件一致性校验。
   * @param status 文件状态对象
   * @return 提取到的etag，不支持则返回null
   */
  public static String getEtag(FileStatus status) {
    if (status instanceof EtagSource) {
      return ((EtagSource) status).getEtag();
    } else {
      return null;
    }
  }

  /**
   * 根据配置创建Manifest存储操作实例，支持通过配置自定义存储操作实现，适配不同文件系统的特殊需求。
   * @param conf 配置
   * @param filesystem 目标文件系统
   * @param path 操作根路径
   * @return 绑定到文件系统的存储操作实例
   * @throws IOException 创建实例失败时抛出
   */
  public static ManifestStoreOperations createManifestStoreOperations(
      final Configuration conf,
      final FileSystem filesystem,
      final Path path) throws IOException {
    try {
      // 从配置中获取自定义存储操作类，默认使用基于文件系统的实现
      final Class<? extends ManifestStoreOperations> storeClass = conf.getClass(
          OPT_STORE_OPERATIONS_CLASS,
          ManifestStoreOperationsThroughFileSystem.class,
          ManifestStoreOperations.class);
      final ManifestStoreOperations operations = storeClass.
          getDeclaredConstructor().newInstance();
      operations.bindToFileSystem(filesystem, path);
      return operations;
    } catch (Exception e) {
      throw new PathIOException(path.toString(),
          "Failed to create Store Operations from configuration option "
              + OPT_STORE_OPERATIONS_CLASS
              + ":" + e, e);
    }
  }

  /**
   * 作业尝试目录结构构建器，负责根据输出路径、作业ID、尝试编号构建整个Manifest提交流程所需的所有目录路径，
   * 统一管理目录结构，支持测试和生产环境复用。
   */
  public static class AttemptDirectories {

    /**
     * 作业输出根路径。
     */
    private final Path outputPath;

    /**
     * 当前作业尝试的根目录。
     */
    private final Path jobAttemptDir;

    /**
     * 当前作业的根目录（所有尝试共享）。
     */
    private final Path jobPath;

    /**
     * 当前作业尝试下，所有任务尝试目录的父目录。
     */
    private final Path jobAttemptTaskSubDir;

    /**
     * 输出根目录下的暂存目录。
     */
    private final Path outputTempSubDir;

    /**
     * 当前作业尝试下，存放所有任务清单的目录。
     */
    private final Path taskManifestDir;

    /**
     * 根据输出路径、作业唯一ID、尝试编号构建整个目录结构。
     * @param outputPath 输出根路径
     * @param jobUniqueId 作业唯一ID
     * @param jobAttemptNumber 作业尝试编号
     */
    public AttemptDirectories(
        Path outputPath,
        String jobUniqueId,
        int jobAttemptNumber) {
      this.outputPath = requireNonNull(outputPath, "Output path");

      this.outputTempSubDir = new Path(outputPath, PENDING_DIR_NAME);
      // 构建作业根路径
      this.jobPath = new Path(outputTempSubDir,
          String.format(JOB_DIR_FORMAT_STR, jobUniqueId));

      // 构建当前尝试专属根路径
      this.jobAttemptDir = new Path(jobPath,
          String.format(JOB_ATTEMPT_DIR_FORMAT_STR, jobAttemptNumber));

      // 构建任务尝试父目录
      this.jobAttemptTaskSubDir = new Path(jobAttemptDir, JOB_TASK_ATTEMPT_SUBDIR);

      // 构建任务清单存储目录
      this.taskManifestDir = new Path(jobAttemptDir, JOB_TASK_MANIFEST_SUBDIR);
    }

    public Path getOutputPath() {
      return outputPath;
    }

    public Path getJobAttemptDir() {
      return jobAttemptDir;
    }

    public Path getJobPath() {
      return jobPath;
    }

    public Path getJobAttemptTaskSubDir() {
      return jobAttemptTaskSubDir;
    }

    /**
     * 根据任务尝试ID获取其专属工作目录路径。
     * @param taskAttemptId 任务尝试ID
     * @return 任务尝试工作目录
     */
    public Path getTaskAttemptPath(String taskAttemptId) {
      return new Path(jobAttemptTaskSubDir, taskAttemptId);
    }

    public Path getOutputTempSubDir() {
      return outputTempSubDir;
    }

    public Path getTaskManifestDir() {
      return taskManifestDir;
    }
  }
}