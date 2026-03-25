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

package org.apache.hadoop.mapred;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 文件输出提交器，负责将MapReduce作业输出提交到指定输出目录
 * 输出目录由配置参数${mapreduce.output.fileoutputformat.outputdir}指定
 * 本类是旧API(org.apache.hadoop.mapred)实现，内部包装了新API(org.apache.hadoop.mapreduce)的实现
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class FileOutputCommitter extends OutputCommitter {

  public static final Logger LOG = LoggerFactory.getLogger(
      "org.apache.hadoop.mapred.FileOutputCommitter");
  
  /**
   * 临时任务输出目录名称
   */
  public static final String TEMP_DIR_NAME = 
    org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter.PENDING_DIR_NAME;
  public static final String SUCCEEDED_FILE_NAME = 
    org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter.SUCCEEDED_FILE_NAME;
  static final String SUCCESSFUL_JOB_OUTPUT_DIR_MARKER =
    org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter.SUCCESSFUL_JOB_OUTPUT_DIR_MARKER;
  
  /**
   * 从作业上下文中获取作业输出根路径
   * @param context 作业上下文
   * @return 作业输出根路径
   */
  private static Path getOutputPath(JobContext context) {
    JobConf conf = context.getJobConf();
    return FileOutputFormat.getOutputPath(conf);
  }
  
  /**
   * 从任务尝试上下文中获取作业输出根路径
   * @param context 任务尝试上下文
   * @return 作业输出根路径
   */
  private static Path getOutputPath(TaskAttemptContext context) {
    JobConf conf = context.getJobConf();
    return FileOutputFormat.getOutputPath(conf);
  }
  
  /** 被包装的新API实现实例 */
  private org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter wrapped = null;
  
  /**
   * 获取被包装的新API FileOutputCommitter实例，延迟初始化
   * @param context 作业上下文
   * @return 新API实现实例
   * @throws IOException 初始化失败时抛出IO异常
   */
  private org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter 
  getWrapped(JobContext context) throws IOException {
    if(wrapped == null) {
      wrapped = new org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter(
          getOutputPath(context), context);
    }
    return wrapped;
  }
  
  /**
   * 获取被包装的新API FileOutputCommitter实例，延迟初始化
   * @param context 任务尝试上下文
   * @return 新API实现实例
   * @throws IOException 初始化失败时抛出IO异常
   */
  private org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter 
  getWrapped(TaskAttemptContext context) throws IOException {
    if(wrapped == null) {
      wrapped = new org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter(
          getOutputPath(context), context);
    }
    return wrapped;
  }
  
  /**
   * 计算作业尝试输出的存放路径
   * @param context 作业上下文，用于获取应用尝试ID
   * @return 作业尝试数据存储路径
   */
  @Private
  Path getJobAttemptPath(JobContext context) {
    Path out = getOutputPath(context);
    return out == null ? null : 
      org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
        .getJobAttemptPath(context, out);
  }

  /**
   * 计算任务尝试输出的存放路径
   * @param context 任务尝试上下文
   * @return 任务尝试输出路径
   * @throws IOException IO异常
   */
  @Private
  public Path getTaskAttemptPath(TaskAttemptContext context) throws IOException {
    Path out = getOutputPath(context);
    return out == null ? null : getTaskAttemptPath(context, out);
  }

  private Path getTaskAttemptPath(TaskAttemptContext context, Path out) throws IOException {
    Path workPath = FileOutputFormat.getWorkOutputPath(context.getJobConf());
    if(workPath == null && out != null) {
      return org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
      .getTaskAttemptPath(context, out);
    }
    return workPath;
  }
  
  /**
   * 计算已提交任务输出的暂存路径，等待整个作业提交完成
   * @param context 任务尝试上下文
   * @return 已提交任务暂存输出路径
   */
  @Private
  Path getCommittedTaskPath(TaskAttemptContext context) {
    Path out = getOutputPath(context);
    return out == null ? null : 
      org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
        .getCommittedTaskPath(context, out);
  }

  /**
   * 获取任务工作路径
   * @param context 任务尝试上下文
   * @param outputPath 输出根路径
   * @return 任务工作路径
   * @throws IOException IO异常
   */
  public Path getWorkPath(TaskAttemptContext context, Path outputPath) 
  throws IOException {
    return outputPath == null ? null : getTaskAttemptPath(context, outputPath);
  }
  
  /**
   * 作业级初始化，创建输出所需目录结构
   * @param context 作业上下文
   * @throws IOException IO异常
   */
  @Override
  public void setupJob(JobContext context) throws IOException {
    getWrapped(context).setupJob(context);
  }
  
  /**
   * 提交作业，将所有任务输出从暂存目录移动到最终输出目录
   * @param context 作业上下文
   * @throws IOException IO异常
   */
  @Override
  public void commitJob(JobContext context) throws IOException {
    getWrapped(context).commitJob(context);
  }
  
  /**
   * 作业清理，已废弃，由新API实现
   * @param context 作业上下文
   * @throws IOException IO异常
   */
  @Override
  @Deprecated
  public void cleanupJob(JobContext context) throws IOException {
    getWrapped(context).cleanupJob(context);
  }

  /**
   * 中止作业，清理作业输出临时文件
   * @param context 作业上下文
   * @param runState 作业运行状态编码
   * @throws IOException IO异常
   */
  @Override
  public void abortJob(JobContext context, int runState) 
  throws IOException {
    JobStatus.State state;
    // 将整型状态码转换为状态枚举
    if(runState == JobStatus.State.RUNNING.getValue()) {
      state = JobStatus.State.RUNNING;
    } else if(runState == JobStatus.State.SUCCEEDED.getValue()) {
      state = JobStatus.State.SUCCEEDED;
    } else if(runState == JobStatus.State.FAILED.getValue()) {
      state = JobStatus.State.FAILED;
    } else if(runState == JobStatus.State.PREP.getValue()) {
      state = JobStatus.State.PREP;
    } else if(runState == JobStatus.State.KILLED.getValue()) {
      state = JobStatus.State.KILLED;
    } else {
      throw new IllegalArgumentException(runState+" is not a valid runState.");
    }
    getWrapped(context).abortJob(context, state);
  }
  
  /**
   * 任务级初始化，准备任务输出目录
   * @param context 任务尝试上下文
   * @throws IOException IO异常
   */
  @Override
  public void setupTask(TaskAttemptContext context) throws IOException {
    getWrapped(context).setupTask(context);
  }
  
  /**
   * 提交任务，将任务输出从尝试目录移动到暂存目录
   * @param context 任务尝试上下文
   * @throws IOException IO异常
   */
  @Override
  public void commitTask(TaskAttemptContext context) throws IOException {
    getWrapped(context).commitTask(context, getTaskAttemptPath(context));
  }

  /**
   * 中止任务，清理任务临时输出
   * @param context 任务尝试上下文
   * @throws IOException IO异常
   */
  @Override
  public void abortTask(TaskAttemptContext context) throws IOException {
    getWrapped(context).abortTask(context, getTaskAttemptPath(context));
  }

  /**
   * 检查任务是否需要提交
   * @param context 任务尝试上下文
   * @return 如果需要任务提交返回true，否则false
   * @throws IOException IO异常
   */
  @Override
  public boolean needsTaskCommit(TaskAttemptContext context) 
  throws IOException {
    return getWrapped(context).needsTaskCommit(context, getTaskAttemptPath(context));
  }

  @Override
  @Deprecated
  public boolean isRecoverySupported() {
    return true;
  }

  /**
   * 检查作业提交是否支持幂等重入
   * @param context 作业上下文
   * @return 支持重入返回true，否则false
   * @throws IOException IO异常
   */
  @Override
  public boolean isCommitJobRepeatable(JobContext context) throws IOException {
    return getWrapped(context).isCommitJobRepeatable(context);
  }

  /**
   * 检查是否支持任务恢复
   * @param context 作业上下文
   * @return 支持恢复返回true，否则false
   * @throws IOException IO异常
   */
  @Override
  public boolean isRecoverySupported(JobContext context) throws IOException {
    return getWrapped(context).isRecoverySupported(context);
  }

  /**
   * 恢复失败/被终止的任务
   * @param context 任务尝试上下文
   * @throws IOException IO异常
   */
  @Override
  public void recoverTask(TaskAttemptContext context)
      throws IOException {
    getWrapped(context).recoverTask(context);
  }
}