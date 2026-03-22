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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.statistics.impl.IOStatisticsStore;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.files.TaskManifest;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.ManifestCommitterSupport;
import org.apache.hadoop.mapreduce.lib.output.committer.manifest.impl.ManifestStoreOperations;
import org.apache.hadoop.util.JsonSerialization;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.functional.TaskPool;

import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.DEFAULT_WRITER_QUEUE_CAPACITY;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.SUCCESS_MARKER;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.SUCCESS_MARKER_FILE_LIMIT;
import static org.apache.hadoop.mapreduce.lib.output.committer.manifest.ManifestCommitterConstants.OPT_MANIFEST_SAVE_ATTEMPTS_DEFAULT;

/**
 * 清单提交器执行阶段配置类，保存所有执行阶段共享的通用配置信息。
 * 独立于MR具体数据类型（任务ID、尝试ID等），仅使用解析后的值。
 * 采用Builder构建API，调用{@link #build()}后配置变为只读，
 * 保证跨阶段共享时不会被意外修改。
 */
public class StageConfig {

  /**
   * 标记配置是否已冻结，冻结后禁止修改。
   */
  private boolean frozen;

  /**
   * 用于统计IO指标的存储对象。
   */
  private IOStatisticsStore iostatistics;

  /**
   * 作业ID，在多次作业尝试中保持不变。
   */
  private String jobId;

  /**
   * 作业唯一ID的来源描述。
   */
  private String jobIdSource = "";

  /**
   * 作业尝试编号，从0开始计数。
   */
  private int jobAttemptNumber;

  /**
   * 任务ID。
   */
  private String taskId;

  /**
   * 当前任务尝试的唯一ID。
   */
  private String taskAttemptId;

  /**
   * 作业输出目标目录。
   */
  private Path destinationDir;

  /**
   * 当前作业尝试的工作目录。
   */
  private Path jobAttemptDir;

  /**
   * 输出目录下的临时文件子目录。
   */
  private Path outputTempSubDir;

  /**
   * 当前任务尝试的工作目录。
   */
  private Path taskAttemptDir;

  /**
   * 存放任务清单文件的目录。
   */
  private Path taskManifestDir;

  /**
   * 作业尝试目录下存放所有任务尝试子目录的父目录。
   */
  private Path jobAttemptTaskSubDir;

  /**
   * 文件存储操作回调接口，封装对底层存储的操作。
   * 不会直接暴露给阶段，阶段需要通过父类调用以添加统计和日志。
   */
  private ManifestStoreOperations operations;

  /**
   * 用于处理除清单处理外其他IO操作的并行任务提交器。
   */
  private TaskPool.Submitter ioProcessors;

  /**
   * 可选的进度更新回调。
   */
  private Progressable progressable;

  /**
   * 进入执行阶段时的事件回调处理器。
   */
  private StageEventCallbacks enterStageEventHandler;

  /**
   * 线程本地的任务清单JSON序列化器，按需创建，可在多个阶段间共享。
   */
  private final ThreadLocal<JsonSerialization<TaskManifest>> threadLocalSerializer =
      ThreadLocal.withInitial(TaskManifest::serializer);

  /**
   * 提交时是否删除目标路径，更严格但会增加IO开销。
   */
  private boolean deleteTargetPaths;

  /**
   * 用于日志输出的名称。
   */
  private String name = "";

  /**
   * Hadoop配置对象，默认使用空配置，作业应覆盖为传入的实际配置。
   */
  private Configuration conf = new Configuration();

  /**
   * 入口写入队列的容量。
   */
  private int writerQueueCapacity = DEFAULT_WRITER_QUEUE_CAPACITY;

  /**
   * 成功标记文件中包含的标记文件数量上限。
   */
  private int successMarkerFileLimit = SUCCESS_MARKER_FILE_LIMIT;

  /**
   * 保存清单文件时保存并重命名的重试次数，默认值为{@value}。
   */
  private int manifestSaveAttempts = OPT_MANIFEST_SAVE_ATTEMPTS_DEFAULT;

  public StageConfig() {
  }

  /**
   * 检查配置是否仍可修改，若已冻结则抛出异常。
   */
  private void checkOpen() {
    Preconditions.checkState(!frozen,
        "StageConfig is now read-only");
  }

  /**
   * 完成配置构建，将配置设为只读。该操作幂等。
   * @return 已冻结的配置对象
   */
  public StageConfig build() {
    frozen = true;
    return this;
  }

  /**
   * 设置作业输出目标目录。
   * @param dir 目标目录
   * @return 当前配置对象
   */
  public StageConfig withDestinationDir(final Path dir) {
    destinationDir = dir;
    return this;
  }

  /**
   * 设置IO统计存储对象。
   * @param store IO统计存储对象
   * @return 当前配置对象
   */
  public StageConfig withIOStatistics(final IOStatisticsStore store) {
    checkOpen();
    iostatistics = store;
    return this;
  }

  /**
   * 设置IO处理任务提交器。
   * @param value 任务提交器
   * @return 当前配置对象
   */
  public StageConfig withIOProcessors(final TaskPool.Submitter value) {
    checkOpen();
    ioProcessors = value;
    return this;
  }

  /**
   * 设置作业尝试目录。
   * @param dir 作业尝试目录
   * @return 当前配置对象
   */
  public StageConfig withJobAttemptDir(final Path dir) {
    checkOpen();
    jobAttemptDir = dir;
    return this;
  }

  /**
   * 获取任务清单文件存放目录。
   * @return 作业尝试目录下的任务清单目录
   */
  public Path getTaskManifestDir() {
    return taskManifestDir;
  }

  /**
   * 设置任务清单文件存放目录。
   * @param value 任务清单目录路径
   * @return 当前配置对象
   */
  public StageConfig withTaskManifestDir(Path value) {
    checkOpen();
    taskManifestDir = value;
    return this;
  }

  /**
   * 设置作业尝试目录下任务尝试子目录的父目录。
   * @param value 目录路径
   * @return 当前配置对象
   */
  public StageConfig withJobAttemptTaskSubDir(Path value) {
    jobAttemptTaskSubDir = value;
    return this;
  }

  /**
   * 获取作业尝试目录下存放所有任务尝试的父目录，列出该目录可得到所有任务尝试目录。
   * @return 作业尝试目录下的任务父目录
   */
  public Path getJobAttemptTaskSubDir() {
    return jobAttemptTaskSubDir;
  }

  /**
   * 从尝试目录信息对象中批量设置所有作业相关目录，不会设置任务尝试相关字段。
   * @param dirs 尝试目录信息对象
   * @return 当前配置对象
   */
  public StageConfig withJobDirectories(
      final ManifestCommitterSupport.AttemptDirectories dirs) {

    checkOpen();
    withJobAttemptDir(dirs.getJobAttemptDir())
        .withJobAttemptTaskSubDir(dirs.getJobAttemptTaskSubDir())
        .withDestinationDir(dirs.getOutputPath())
        .withOutputTempSubDir(dirs.getOutputTempSubDir())
        .withTaskManifestDir(dirs.getTaskManifestDir());

    return this;
  }

  /**
   * 设置不包含尝试编号的作业ID。
   * @param value 作业ID
   * @return 当前配置对象
   */
  public StageConfig withJobId(final String value) {
    checkOpen();
    jobId = value;
    return this;
  }

  public Path getOutputTempSubDir() {
    return outputTempSubDir;
  }

  /**
   * 设置输出临时子目录。
   * @param value 临时目录路径
   * @return 当前配置对象
   */
  public StageConfig withOutputTempSubDir(final Path value) {
    checkOpen();
    outputTempSubDir = value;
    return this;
  }

  /**
   * 设置存储操作回调接口。
   * @param value 存储操作对象
   * @return 当前配置对象
   */
  public StageConfig withOperations(final ManifestStoreOperations value) {
    checkOpen();
    operations = value;
    return this;
  }

  /**
   * 设置任务尝试ID。
   * @param value 任务尝试ID
   * @return 当前配置对象
   */
  public StageConfig withTaskAttemptId(final String value) {
    checkOpen();
    taskAttemptId = value;
    return this;
  }

  /**
   * 设置任务ID。
   * @param value 任务ID
   * @return 当前配置对象
   */
  public StageConfig withTaskId(final String value) {
    checkOpen();
    taskId = value;
    return this;
  }

  /**
   * 设置阶段进入事件回调处理器。
   * @param value 回调处理器
   * @return 当前配置对象
   */
  public StageConfig withStageEventCallbacks(StageEventCallbacks value) {
    checkOpen();
    enterStageEventHandler = value;
    return this;
  }

  /**
   * 设置进度回调对象。
   * @param value 进度回调对象
   * @return 当前配置对象
   */
  public StageConfig withProgressable(final Progressable value) {
    checkOpen();
    progressable = value;
    return this;
  }

  /**
   * 设置任务尝试工作目录。
   * @param value 任务尝试目录路径
   * @return 当前配置对象
   */
  public StageConfig withTaskAttemptDir(final Path value) {
    checkOpen();
    taskAttemptDir = value;
    return this;
  }

  /**
   * 设置作业尝试编号。
   * @param value 作业尝试编号
   * @return 当前配置对象
   */
  public StageConfig withJobAttemptNumber(final int value) {
    checkOpen();
    jobAttemptNumber = value;
    return this;
  }

  /**
   * 设置作业ID来源描述。
   * @param value 来源描述
   * @return 当前配置对象
   */
  public StageConfig withJobIdSource(final String value) {
    checkOpen();
    jobIdSource = value;
    return this;
  }

  /**
   * 设置日志用名称。
   * @param value 名称
   * @return 当前配置对象
   */
  public StageConfig withName(String value) {
    name = value;
    return this;
  }

  /**
   * 获取日志用名称。
   * @return 日志名称
   */
  public String getName() {
    return name;
  }

  /**
   * 设置Hadoop配置对象。
   * @param value Hadoop配置
   * @return 当前配置对象
   */
  public StageConfig withConfiguration(Configuration value) {
    conf = value;
    return this;
  }

  /**
   * 获取Hadoop配置对象。
   * @return Hadoop配置
   */
  public Configuration getConf() {
    return conf;
  }

  /**
   * 获取写入队列容量。
   * @return 队列容量
   */
  public int getWriterQueueCapacity() {
    return writerQueueCapacity;
  }

  /**
   * 设置写入队列容量。
   * @param value 队列容量
   * @return 当前配置对象
   */
  public StageConfig withWriterQueueCapacity(final int value) {
    writerQueueCapacity = value;
    return this;
  }

  /**
   * 获取阶段进入事件回调处理器。
   * @return 回调处理器
   */
  public StageEventCallbacks getEnterStageEventHandler() {
    return enterStageEventHandler;
  }

  /**
   * 获取IO统计存储对象。
   * @return IO统计存储
   */
  public IOStatisticsStore getIOStatistics() {
    return iostatistics;
  }

  /**
   * 获取作业ID。
   * @return 作业ID
   */
  public String getJobId() {
    return jobId;
  }

  /**
   * 获取任务ID。
   * @return 任务ID
   */
  public String getTaskId() {
    return taskId;
  }

  /**
   * 获取任务尝试ID。
   * @return 任务尝试ID
   */
  public String getTaskAttemptId() {
    return taskAttemptId;
  }

  /**
   * 获取作业尝试目录。
   * @return 作业尝试目录
   */
  public Path getJobAttemptDir() {
    return jobAttemptDir;
  }

  /**
   * 获取作业输出目标目录。
   * @return 目标目录
   */
  public Path getDestinationDir() {
    return destinationDir;
  }

  /**
   * 获取作业成功标记文件的路径。
   * @return 目标目录下的成功标记文件路径
   */
  public Path getJobSuccessMarkerPath() {
    return new Path(destinationDir, SUCCESS_MARKER);
  }

  /**
   * 获取存储操作回调对象。
   * @return 存储操作对象
   */
  public ManifestStoreOperations getOperations() {
    return operations;
  }

  /**
   * 获取IO处理任务提交器。
   * @return 任务提交器
   */
  public TaskPool.Submitter getIoProcessors() {
    return ioProcessors;
  }

  /**
   * 获取进度回调对象。
   * @return 进度回调，可能为null
   */
  public Progressable getProgressable() {
    return progressable;
  }

  /**
   * 获取任务尝试工作目录。
   * @return 任务尝试目录
   */
  public Path getTaskAttemptDir() {
    return taskAttemptDir;
  }

  /**
   * 获取作业尝试编号。
   * @return 作业尝试编号
   */
  public int getJobAttemptNumber() {
    return jobAttemptNumber;
  }

  public String getJobIdSource() {
    return jobIdSource;
  }

  /**
   * 获取当前线程的任务清单序列化器。
   * @return 序列化器对象
   */
  public JsonSerialization<TaskManifest> currentManifestSerializer() {
    return threadLocalSerializer.get();
  }

  /**
   * 设置提交时是否删除目标路径。
   * @param value 是否删除
   * @return 当前配置对象
   */
  public StageConfig withDeleteTargetPaths(boolean value) {
    checkOpen();
    deleteTargetPaths = value;
    return this;
  }

  public boolean getDeleteTargetPaths() {
    return deleteTargetPaths;
  }

  /**
   * 设置成功标记文件中包含的标记文件数量上限。
   * @param value 数量上限
   * @return 当前配置对象
   */
  public StageConfig withSuccessMarkerFileLimit(final int value) {
    checkOpen();

    successMarkerFileLimit = value;
    return this;
  }

  public int getSuccessMarkerFileLimit() {
    return successMarkerFileLimit;
  }

  public int getManifestSaveAttempts() {
    return manifestSaveAttempts;
  }

  /**
   * 设置保存清单文件的重试次数。
   * @param value 重试次数
   * @return 当前配置对象
   */
  public StageConfig withManifestSaveAttempts(final int value) {
    checkOpen();
    manifestSaveAttempts = value;
    return this;
  }

  /**
   * 进入执行阶段，若已设置回调处理器则触发回调。
   * @param stage 阶段名称
   */
  public void enterStage(String stage) {
    if (enterStageEventHandler != null) {
      enterStageEventHandler.enterStage(stage);
    }
  }

  /**
   * 退出执行阶段，若已设置回调处理器则触发回调