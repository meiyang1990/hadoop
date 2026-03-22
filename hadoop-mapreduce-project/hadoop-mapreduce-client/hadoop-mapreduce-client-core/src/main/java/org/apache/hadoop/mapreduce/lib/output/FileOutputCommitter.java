// 这个文件已经全部加上中文注释
/**
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

package org.apache.hadoop.mapreduce.lib.output;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.util.DurationInfo;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 文件输出提交器，负责将MapReduce任务输出文件提交到作业输出目录
 * 即 ${mapreduce.output.fileoutputformat.outputdir} 指定的目录。
 * 核心功能是管理临时输出目录，在任务和作业成功完成后将输出移动到最终目录
 **/
@InterfaceAudience.Public
@InterfaceStability.Stable
public class FileOutputCommitter extends PathOutputCommitter {
  private static final Logger LOG =
      LoggerFactory.getLogger(FileOutputCommitter.class);

  /** 
   * 待提交数据的临时目录名称，存放尚未提交的作业输出数据
   */
  public static final String PENDING_DIR_NAME = "_temporary";
  /**
   * 临时目录名称，兼容MapReduce 1.x版本，已废弃
   */
  @Deprecated
  protected static final String TEMP_DIR_NAME = PENDING_DIR_NAME;
  public static final String SUCCEEDED_FILE_NAME = "_SUCCESS";
  /**
   * 配置项名称：是否在作业成功输出目录生成_SUCCESS标记文件
   */
  public static final String SUCCESSFUL_JOB_OUTPUT_DIR_MARKER =
      "mapreduce.fileoutputcommitter.marksuccessfuljobs";
  /**
   * 配置项名称：文件输出提交器算法版本
   */
  public static final String FILEOUTPUTCOMMITTER_ALGORITHM_VERSION =
      "mapreduce.fileoutputcommitter.algorithm.version";
  /**
   * 文件输出提交器算法版本默认值：版本2
   */
  public static final int FILEOUTPUTCOMMITTER_ALGORITHM_VERSION_DEFAULT = 2;
  /**
   * 配置项名称：是否跳过清理作业输出目录下的_temporary临时文件夹
   */
  // Skip cleanup _temporary folders under job's output directory
  public static final String FILEOUTPUTCOMMITTER_CLEANUP_SKIPPED =
      "mapreduce.fileoutputcommitter.cleanup.skipped";
  public static final boolean
      FILEOUTPUTCOMMITTER_CLEANUP_SKIPPED_DEFAULT = false;

  /**
   * 配置项名称：是否忽略清理作业输出目录下_temporary临时文件夹时的异常
   */
  // Ignore exceptions in cleanup _temporary folder under job's output directory
  public static final String FILEOUTPUTCOMMITTER_CLEANUP_FAILURES_IGNORED =
      "mapreduce.fileoutputcommitter.cleanup-failures.ignored";
  public static final boolean
      FILEOUTPUTCOMMITTER_CLEANUP_FAILURES_IGNORED_DEFAULT = false;

  /**
   * 配置项名称：作业提交失败后重试次数
   */
  // Number of attempts when failure happens in commit job
  public static final String FILEOUTPUTCOMMITTER_FAILURE_ATTEMPTS =
      "mapreduce.fileoutputcommitter.failures.attempts";

  /**
   * 作业提交失败重试次数默认值：1次，保持和原有行为一致
   */
  // default value to be 1 to keep consistent with previous behavior
  public static final int FILEOUTPUTCOMMITTER_FAILURE_ATTEMPTS_DEFAULT = 1;

  /**
   * 配置项名称：任务完成后是否删除任务临时目录
   * 仅当算法版本为2时生效，对于不支持O(1)递归删除的对象存储是一个优化，HDFS默认关闭
   */
  // Whether tasks should delete their task temporary directories. This is
  // purely an optimization for filesystems without O(1) recursive delete, as
  // commitJob will recursively delete the entire job temporary directory.
  // HDFS has O(1) recursive delete, so this parameter is left false by default.
  // Users of object stores, for example, may want to set this to true. Note:
  // this is only used if mapreduce.fileoutputcommitter.algorithm.version=2
  public static final String FILEOUTPUTCOMMITTER_TASK_CLEANUP_ENABLED =
      "mapreduce.fileoutputcommitter.task.cleanup.enabled";
  public static final boolean
      FILEOUTPUTCOMMITTER_TASK_CLEANUP_ENABLED_DEFAULT = false;

  private Path outputPath = null;
  private Path workPath = null;
  private final int algorithmVersion;
  private final boolean skipCleanup;
  private final boolean ignoreCleanupFailures;

  /**
   * 构造文件输出提交器实例
   * @param outputPath 作业最终输出路径，如果传入null则提交器不执行任何操作
   * @param context 任务尝试上下文
   * @throws IOException 初始化IO异常
   */
  public FileOutputCommitter(Path outputPath, 
                             TaskAttemptContext context) throws IOException {
    this(outputPath, (JobContext)context);
    if (getOutputPath() != null) {
      workPath = Preconditions.checkNotNull(
          getTaskAttemptPath(context, getOutputPath()),
          "Null task attempt path in %s and output path %s",
          context, outputPath);
    }
  }
  
  /**
   * 构造文件输出提交器实例，基于作业上下文
   * @param outputPath 作业最终输出路径，如果传入null则提交器不执行任何操作
   * @param context 作业上下文
   * @throws IOException 初始化IO异常
   */
  @Private
  public FileOutputCommitter(Path outputPath, 
                             JobContext context) throws IOException {
    super(outputPath, context);
    Configuration conf = context.getConfiguration();
    // 从配置读取算法版本，默认使用版本2
    algorithmVersion =
        conf.getInt(FILEOUTPUTCOMMITTER_ALGORITHM_VERSION,
                    FILEOUTPUTCOMMITTER_ALGORITHM_VERSION_DEFAULT);
    LOG.info("File Output Committer Algorithm version is " + algorithmVersion);
    // 仅支持版本1和版本2
    if (algorithmVersion != 1 && algorithmVersion != 2) {
      throw new IOException("Only 1 or 2 algorithm version is supported");
    }

    // 读取是否跳过清理配置
    skipCleanup = conf.getBoolean(
        FILEOUTPUTCOMMITTER_CLEANUP_SKIPPED,
        FILEOUTPUTCOMMITTER_CLEANUP_SKIPPED_DEFAULT);

    // 读取是否忽略清理失败配置
    ignoreCleanupFailures = conf.getBoolean(
        FILEOUTPUTCOMMITTER_CLEANUP_FAILURES_IGNORED,
        FILEOUTPUTCOMMITTER_CLEANUP_FAILURES_IGNORED_DEFAULT);

    LOG.info("FileOutputCommitter skip cleanup _temporary folders under " +
        "output directory:" + skipCleanup + ", ignore cleanup failures: " +
        ignoreCleanupFailures);

    // 版本1使用跳过清理会有风险，输出警告
    if (algorithmVersion == 1 && skipCleanup) {
        LOG.warn("Skip cleaning up when using FileOutputCommitter V1 can lead to unexpected behaviors. " +
                "For example, committing several times may be allowed falsely.");
    }

    // 输出路径不为空时，获取文件系统并标准化路径
    if (outputPath != null) {
      FileSystem fs = outputPath.getFileSystem(context.getConfiguration());
      this.outputPath = fs.makeQualified(outputPath);
    }
  }
  
  /**
   * 获取作业最终输出路径，即已提交作业尝试的输出根目录
   * @return 最终输出路径
   */
  @Override
  public Path getOutputPath() {
    return this.outputPath;
  }

  /**
   * 获取当前输出路径下，所有待提交作业尝试的根临时目录
   * @return 待提交作业尝试的根目录路径
   */
  private Path getPendingJobAttemptsPath() {
    return getPendingJobAttemptsPath(getOutputPath());
  }
  
  /**
   * 获取指定输出路径下，所有待提交作业尝试的根临时目录
   * @param out 输出基础目录
   * @return 待提交作业尝试的根目录路径
   */
  private static Path getPendingJobAttemptsPath(Path out) {
    return new Path(out, PENDING_DIR_NAME);
  }
  
  /**
   * 从作业上下文中获取当前应用尝试ID
   * @param context 作业上下文
   * @return 当前应用尝试ID
   */
  private static int getAppAttemptId(JobContext context) {
    return context.getConfiguration().getInt(
        MRJobConfig.APPLICATION_ATTEMPT_ID, 0);
  }
  
  /**
   * 计算当前作业尝试输出数据的存储路径
   * @param context 作业上下文，用于获取应用尝试ID
   * @return 当前作业尝试的输出存储路径
   */
  public Path getJobAttemptPath(JobContext context) {
    return getJobAttemptPath(context, getOutputPath());
  }
  
  /**
   * 在指定输出路径下，计算给定作业尝试输出数据的存储路径
   * @param context 作业上下文，用于获取应用尝试ID
   * @param out 输出基础目录
   * @return 当前作业尝试的输出存储路径
   */
  public static Path getJobAttemptPath(JobContext context, Path out) {
    return getJobAttemptPath(getAppAttemptId(context), out);
  }
  
  /**
   * 根据应用尝试ID计算作业尝试输出路径
   * @param appAttemptId 应用尝试ID
   * @return 当前作业尝试的输出存储路径
   */
  protected Path getJobAttemptPath(int appAttemptId) {
    return getJobAttemptPath(appAttemptId, getOutputPath());
  }
  
  /**
   * 在指定输出路径下，根据应用尝试ID计算作业尝试输出路径
   * @param appAttemptId 应用尝试ID
   * @param out 输出基础目录
   * @return 当前作业尝试的输出存储路径
   */
  private static Path getJobAttemptPath(int appAttemptId, Path out) {
    return new Path(getPendingJobAttemptsPath(out), String.valueOf(appAttemptId));
  }
  
  /**
   * 计算当前作业下所有待提交任务尝试输出存储根目录
   * @param context 当前作业上下文
   * @return 待提交任务尝试输出根目录路径
   */
  private Path getPendingTaskAttemptsPath(JobContext context) {
    return getPendingTaskAttemptsPath(context, getOutputPath());
  }
  
  /**
   * 在指定输出路径下，计算当前作业下所有待提交任务尝试输出存储根目录
   * @param context 当前作业上下文
   * @param out 输出基础目录
   * @return 待提交任务尝试输出根目录路径
   */
  private static Path getPendingTaskAttemptsPath(JobContext context, Path out) {
    return new Path(getJobAttemptPath(context, out), PENDING_DIR_NAME);
  }
  
  /**
   * 计算当前任务尝试输出的临时存储路径，任务提交前输出写入这里
   * 
   * @param context 任务尝试上下文
   * @return 任务尝试临时输出路径
   */
  public Path getTaskAttemptPath(TaskAttemptContext context) {
    return new Path(getPendingTaskAttemptsPath(context), 
        String.valueOf(context.getTaskAttemptID()));
  }
  
  /**
   * 在指定输出路径下，计算当前任务尝试输出的临时存储路径
   * 
   * @param context 任务尝试上下文
   * @param out 输出基础目录
   * @return 任务尝试临时输出路径
   */
  public static Path getTaskAttemptPath(TaskAttemptContext context, Path out) {
    return new Path(getPendingTaskAttemptsPath(context, out), 
        String.valueOf(context.getTaskAttemptID()));
  }
  
  /**
   * 计算任务提交后输出的暂存路径，作业整体提交前放在这里
   * @param context 任务尝试上下文
   * @return 已提交任务输出暂存路径
   */
  public Path getCommittedTaskPath(TaskAttemptContext context) {
    return getCommittedTaskPath(getAppAttemptId(context), context);
  }
  
  public static Path getCommittedTaskPath(TaskAttemptContext context, Path out) {
    return getCommittedTaskPath(getAppAttemptId(context), context, out);
  }
  
  /**
   * 对于指定应用尝试，计算已提交任务输出的暂存路径
   * @param appAttemptId 应用尝试ID
   * @param context 任务上下文
   * @return 已提交任务输出暂存路径
   */
  protected Path getCommittedTaskPath(int appAttemptId, TaskAttemptContext context) {
    return new Path(getJobAttemptPath(appAttemptId),
        String.valueOf(context.getTaskAttemptID().getTaskID()));
  }
  
  private static Path getCommittedTaskPath(int appAttemptId, TaskAttemptContext context, Path out) {
    return new Path(getJobAttemptPath(appAttemptId, out),
        String.valueOf(context.getTaskAttemptID().getTaskID()));
  }

  /**
   * 已提交任务路径过滤器，过滤掉临时目录
   */
  private static class CommittedTaskFilter implements PathFilter {
    @Override
    public boolean accept(Path path) {
      return !PENDING_DIR_NAME.equals(path.getName());
    }
  }

  /**
   * 获取当前作业所有已提交任务的输出路径列表
   * @param context 当前作业上下文
   * @return 所有已提交任务的FileStatus数组
   * @throws IOException 列出文件状态时IO异常
   */
  private FileStatus[] getAllCommittedTaskPaths(JobContext context) 
    throws IOException {
    Path jobAttemptPath = getJobAttemptPath(context);
    FileSystem fs = jobAttemptPath.getFileSystem(context.getConfiguration());
    return fs.listStatus(jobAttemptPath, new CommittedTaskFilter());
  }

  /**
   * 获取当前任务尝试的工作目录，任务直接输出写入这里
   * @return 当前任务工作目录路径
   * @throws IOException 获取路径时IO异常
   */
  public Path getWorkPath() throws IOException {
    return workPath;
  }

  /**
   * 作业初始化方法，创建所有任务工作目录的根临时目录
   * @param context 作业上下文
   */
  public void setupJob(JobContext context) throws IOException {
    if (hasOutputPath()) {
      Path jobAttemptPath = getJobAttemptPath(context);
      FileSystem fs = jobAttemptPath.getFileSystem(
          context.getConfiguration());
      if (!fs.mkdirs(jobAttemptPath)) {
        LOG.error("Mkdirs failed to create " + jobAttemptPath);
      }
    } else {
      LOG.warn("Output Path is null in setupJob()");
    }
  }

  /**
   * 作业完成提交入口方法，支持失败重试，调用commitJobInternal完成实际提交
   * @param context 作业上下文
   */
  public void commitJob(JobContext context) throws IOException {
    // 可重复提交时读取配置重试次数，否则只尝试一次
    int maxAttemptsOnFailure = isCommitJobRepeatable(context) ?
        context.getConfiguration().getInt(FILEOUTPUTCOMMITTER_FAILURE_ATTEMPTS,
            FILEOUTPUTCOMMITTER_FAILURE_ATTEMPTS_DEFAULT) : 1;
    int attempt = 0;
    boolean jobCommitNotFinished = true;
    // 循环重试直到成功或达到最大重试次数
    while (jobCommitNotFinished) {
      try {
        commitJobInternal(context);
        jobCommitNotFinished = false;
      } catch (Exception e) {
        if (++attempt >= maxAttemptsOnFailure) {
          throw e;
        } else {
          LOG.warn("Exception get thrown in job commit, retry (" + attempt +
              ") time.", e);
        }
      }
    }
  }

  /**
   * 实际执行作业提交的核心方法，完成以下操作：
   * 版本1：将所有已提交任务输出移动到最终输出目录；删除整个临时目录；根据配置生成_SUCCESS标记文件
   * 版本2：任务提交已经直接输出到最终目录，仅删除临时目录并生成_SUCCESS标记文件
   * @param context 作业上下文
   */
  @VisibleForTesting
  protected void commitJobInternal(JobContext context) throws IOException {
    if (hasOutputPath()) {