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

import java.io.IOException;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsStore;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.InternalConstants;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.ManifestCommitterSupport;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.stages.StageConfig;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.stages.StageEventCallbacks;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.apache.hadoop.util.functional.CloseableTaskPoolSubmitter;

import static org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter.SUCCESSFUL_JOB_OUTPUT_DIR_MARKER;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.*;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.ManifestCommitterSupport.buildJobUUID;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.ManifestCommitterSupport.getAppAttemptId;

/**
 * 文件说明：Manifest提交器的配置类，从作业配置和提交器工厂传入数据中构建，
 * 隔离配置逻辑便于开发和测试，为整个提交流程提供统一配置入口。
 */
public final class ManifestCommitterConfig implements IOStatisticsSource {

  private static final Logger LOG = LoggerFactory.getLogger(
      ManifestCommitterConfig.class);

  /**
   * 作业最终输出目标目录，未做资格化处理。
   */
  private final Path destinationDir;

  /**
   * 角色标识，用于日志和文本消息中区分上下文。
   */
  private final String role;

  /**
   * 所有中间工作的目录，输出格式会将数据写入此处；如果从作业上下文构建则为null。
   */
  private final Path taskAttemptDir;

  /** 作业配置对象。 */
  private final Configuration conf;

  /** 作业上下文，如果是任务上下文则可以强转为TaskContext。 */
  private final JobContext jobContext;

  /** 是否需要创建作业成功标记文件。 */
  private final boolean createJobMarker;

  /**
   * 作业唯一ID，不带尝试编号后缀；要求全局唯一，解决Spark旧版本作业ID不唯一问题。
   */
  private final String jobUniqueId;

  /**
   * 作业唯一ID的来源标识。
   */
  private final String jobUniqueIdSource;

  /**
   * 作业尝试编号，从0开始计数。
   */
  private final int jobAttemptNumber;

  /**
   * 作业唯一ID + 尝试编号拼接成的作业尝试ID。
   */
  private final String jobAttemptId;

  /**
   * 任务ID，用作清单文件名；如果从作业上下文构建则为空字符串。
   */
  private final String taskId;

  /**
   * 任务尝试ID，决定任务尝试写入数据的工作目录，供提交器扫描；如果从作业上下文构建则为空字符串。
   */
  private final String taskAttemptId;

  /** 进度回调接口。 */
  private final Progressable progressable;

  /**
   * 用于统计更新的IO统计信息存储。
   */
  private final IOStatisticsStore iostatistics;


  /** 是否在提交完成后验证输出数据完整性。 */
  private final boolean validateOutput;

  /**
   * 尝试目录管理器，管理所有相关目录路径。
   */
  private final ManifestCommitterSupport.AttemptDirectories dirs;

  /**
   * 阶段进入事件的回调接口。
   */
  private final StageEventCallbacks stageEventCallbacks;

  /**
   * 日志打印使用的名称标识。
   */
  private final String name;

  /** 是否在提交时删除目标路径，规则更严格但IO成本更高。 */
  private final boolean deleteTargetPaths;

  /**
   * 条目写入队列的容量。
   */
  private final int writerQueueCapacity;

  /**
   * 保存任务清单时，保存并重命名操作的最大重试次数，失败后放弃。
   */
  private final int saveManifestAttempts;

  /**
   * 构造方法：从作业输出路径、上下文、统计信息和回调构建Manifest提交器配置。
   * @param outputPath 作业的目标输出路径
   * @param role 日志消息使用的角色标识
   * @param context 作业/任务上下文
   * @param iostatistics IO统计存储
   * @param stageEventCallbacks 阶段事件回调
   */

  ManifestCommitterConfig(
      final Path outputPath,
      final String role,
      final JobContext context,
      final IOStatisticsStore iostatistics,
      final StageEventCallbacks stageEventCallbacks) {
    this.role = role;
    this.jobContext = context;
    this.conf = context.getConfiguration();
    this.destinationDir = outputPath;
    this.iostatistics = iostatistics;
    this.stageEventCallbacks = stageEventCallbacks;

    // 生成作业唯一ID和来源
    Pair<String, String> pair = buildJobUUID(conf, context.getJobID());
    this.jobUniqueId = pair.getLeft();
    this.jobUniqueIdSource = pair.getRight();
    this.jobAttemptNumber = getAppAttemptId(context);
    this.jobAttemptId = this.jobUniqueId + "_" + jobAttemptNumber;

    // 构建各类目录路径
    this.dirs = new ManifestCommitterSupport.AttemptDirectories(outputPath,
        this.jobUniqueId, jobAttemptNumber);

    // 从配置读取各项参数
    this.createJobMarker = conf.getBoolean(
        SUCCESSFUL_JOB_OUTPUT_DIR_MARKER,
        DEFAULT_CREATE_SUCCESSFUL_JOB_DIR_MARKER);
    this.validateOutput = conf.getBoolean(
        OPT_VALIDATE_OUTPUT,
        OPT_VALIDATE_OUTPUT_DEFAULT);
    this.deleteTargetPaths = conf.getBoolean(
        OPT_DELETE_TARGET_FILES,
        OPT_DELETE_TARGET_FILES_DEFAULT);
    this.writerQueueCapacity = conf.getInt(
        OPT_WRITER_QUEUE_CAPACITY,
        DEFAULT_WRITER_QUEUE_CAPACITY);
    int attempts = conf.getInt(OPT_MANIFEST_SAVE_ATTEMPTS,
        OPT_MANIFEST_SAVE_ATTEMPTS_DEFAULT);
    // 校验重试次数合法性，非法值则重置为最小值1
    if (attempts < 1) {
      LOG.warn("Invalid value for {}: {}",
          OPT_MANIFEST_SAVE_ATTEMPTS, attempts);
      attempts = 1;
    }
    this.saveManifestAttempts = attempts;

    // 如果是任务尝试上下文，构建任务ID和任务尝试目录
    if (context instanceof TaskAttemptContext) {
      // 当前是任务级别上下文
      final TaskAttemptContext tac = (TaskAttemptContext) context;
      TaskAttemptID taskAttempt = Objects.requireNonNull(
          tac.getTaskAttemptID());
      taskAttemptId = taskAttempt.toString();
      taskId = taskAttempt.getTaskID().toString();
      // 生成任务尝试专属目录，保证不同实例目录隔离
      taskAttemptDir = dirs.getTaskAttemptPath(taskAttemptId);
      // 上下文本身就是进度回调实现
      progressable = tac;
      name = String.format(InternalConstants.NAME_FORMAT_TASK_ATTEMPT, taskAttemptId);

    } else {
      // 当前是作业级别上下文
      taskId = "";
      taskAttemptId = "";
      taskAttemptDir = null;
      progressable = null;
      name = String.format(InternalConstants.NAME_FORMAT_JOB_ATTEMPT, jobAttemptId);
    }
  }

  @Override
  public String toString() {
    return "ManifestCommitterConfig{" +
        "name=" + name +
        ", destinationDir=" + destinationDir +
        ", role='" + role + '\'' +
        ", taskAttemptDir=" + taskAttemptDir +
        ", createJobMarker=" + createJobMarker +
        ", jobUniqueId='" + jobUniqueId + '\'' +
        ", jobUniqueIdSource='" + jobUniqueIdSource + '\'' +
        ", jobAttemptNumber=" + jobAttemptNumber +
        ", jobAttemptId='" + jobAttemptId + '\'' +
        ", taskId='" + taskId + '\'' +
        ", taskAttemptId='" + taskAttemptId + '\'' +
        '}';
  }

  /**
   * 获取目标输出路径对应的文件系统实例。
   * @return 目标文件系统
   * @throws IOException 获取文件系统时发生IO异常
   */
  FileSystem getDestinationFileSystem() throws IOException {
    return FileSystem.get(destinationDir.toUri(), conf);
  }

  /**
   * 基于当前配置创建处理阶段配置对象，不绑定存储操作和处理器。
   * @return 填充好配置的阶段配置对象
   */
  StageConfig createStageConfig() {
    StageConfig stageConfig = new StageConfig();
    stageConfig
        .withConfiguration(conf)
        .withDeleteTargetPaths(deleteTargetPaths)
        .withIOStatistics(iostatistics)
        .withJobAttemptNumber(jobAttemptNumber)
        .withJobDirectories(dirs)
        .withJobId(jobUniqueId)
        .withJobIdSource(jobUniqueIdSource)
        .withName(name)
        .withProgressable(progressable)
        .withStageEventCallbacks(stageEventCallbacks)
        .withTaskAttemptDir(taskAttemptDir)
        .withTaskAttemptId(taskAttemptId)
        .withTaskId(taskId)
        .withWriterQueueCapacity(writerQueueCapacity);
    return stageConfig;
  }

  public Path getDestinationDir() {
    return destinationDir;
  }

  public String getRole() {
    return role;
  }

  public Path getTaskAttemptDir() {
    return taskAttemptDir;
  }

  public Path getJobAttemptDir() {
    return dirs.getJobAttemptDir();
  }

  public Path getTaskManifestDir() {
    return dirs.getTaskManifestDir();
  }

  public Configuration getConf() {
    return conf;
  }

  public JobContext getJobContext() {
    return jobContext;
  }

  public boolean getCreateJobMarker() {
    return createJobMarker;
  }

  public String getJobAttemptId() {
    return jobAttemptId;
  }

  public String getTaskAttemptId() {
    return taskAttemptId;
  }

  public String getTaskId() {
    return taskId;
  }

  public String getJobUniqueId() {
    return jobUniqueId;
  }

  public boolean getValidateOutput() {
    return validateOutput;
  }

  public String getName() {
    return name;
  }

  public int getSaveManifestAttempts() {
    return saveManifestAttempts;
  }

  /**
   * 获取写入队列容量。
   * @return 队列容量值
   */
  public int getWriterQueueCapacity() {
    return writerQueueCapacity;
  }

  @Override
  public IOStatisticsStore getIOStatistics() {
    return iostatistics;
  }

  /**
   * 根据配置参数{@link ManifestCommitterConstants#OPT_IO_PROCESSORS}创建异步任务线程池提交器。
   * @return 新建的线程池提交器
   */
  public CloseableTaskPoolSubmitter createSubmitter() {
    return createSubmitter(
        OPT_IO_PROCESSORS, OPT_IO_PROCESSORS_DEFAULT);
  }

  /**
   * 根据指定配置键和默认值创建异步任务线程池提交器。
   * @param key 线程池大小配置键
   * @param defVal 默认线程数
   * @return 新建的任务池提交器
   */
  public CloseableTaskPoolSubmitter createSubmitter(String key, int defVal) {
    int numThreads = conf.getInt(key, defVal);
    // 非法值则使用默认值
    if (numThreads <= 0) {
      // ignore the setting if it is too invalid.
      numThreads = defVal;
    }
    return createCloseableTaskSubmitter(numThreads, getJobAttemptId());
  }

  /**
   * 根据指定线程数和作业ID创建异步任务线程池提交器。
   *
   * @param numThreads 线程数量
   * @param jobAttemptId 作业尝试ID
   * @return 新建的任务池提交器
   */
  public static CloseableTaskPoolSubmitter createCloseableTaskSubmitter(
      final int numThreads,
      final String jobAttemptId) {
    return new CloseableTaskPoolSubmitter(
        HadoopExecutors.newFixedThreadPool(numThreads,
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("manifest-committer-" + jobAttemptId + "-%d")
                .build()));
  }

}