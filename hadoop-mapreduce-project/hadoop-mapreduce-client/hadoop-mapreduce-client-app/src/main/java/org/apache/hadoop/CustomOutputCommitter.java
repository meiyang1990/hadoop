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

package org.apache.hadoop;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.OutputCommitter;
import org.apache.hadoop.mapred.TaskAttemptContext;

/**
 * 自定义MapReduce输出提交器，用于验证输出提交流程，会在作业和任务各生命周期生成对应标记文件
 * 继承自Hadoop MapReduce原生OutputCommitter，在各个生命周期阶段生成标记文件方便调试流程。
 */
public class CustomOutputCommitter extends OutputCommitter {

  // 作业setup阶段标记文件名
  public static final String JOB_SETUP_FILE_NAME = "_job_setup";
  // 作业提交成功阶段标记文件名
  public static final String JOB_COMMIT_FILE_NAME = "_job_commit";
  // 作业终止阶段标记文件名
  public static final String JOB_ABORT_FILE_NAME = "_job_abort";
  // 任务setup阶段标记文件名
  public static final String TASK_SETUP_FILE_NAME = "_task_setup";
  // 任务终止阶段标记文件名
  public static final String TASK_ABORT_FILE_NAME = "_task_abort";
  // 任务提交成功阶段标记文件名
  public static final String TASK_COMMIT_FILE_NAME = "_task_commit";

  /**
   * 作业初始化阶段回调，生成作业setup标记文件
   * @param jobContext 作业上下文对象
   * @throws IOException 文件创建IO异常
   */
  @Override
  public void setupJob(JobContext jobContext) throws IOException {
    writeFile(jobContext.getJobConf(), JOB_SETUP_FILE_NAME);
  }

  /**
   * 作业提交成功阶段回调，生成作业提交标记文件
   * @param jobContext 作业上下文对象
   * @throws IOException 文件创建IO异常
   */
  @Override
  public void commitJob(JobContext jobContext) throws IOException {
    super.commitJob(jobContext);
    writeFile(jobContext.getJobConf(), JOB_COMMIT_FILE_NAME);
  }

  /**
   * 作业终止阶段回调，生成作业终止标记文件
   * @param jobContext 作业上下文对象
   * @param status 作业终止状态码
   * @throws IOException 文件创建IO异常
   */
  @Override
  public void abortJob(JobContext jobContext, int status) 
  throws IOException {
    super.abortJob(jobContext, status);
    writeFile(jobContext.getJobConf(), JOB_ABORT_FILE_NAME);
  }
  
  /**
   * 任务初始化阶段回调，生成任务setup标记文件
   * @param taskContext 任务尝试上下文对象
   * @throws IOException 文件创建IO异常
   */
  @Override
  public void setupTask(TaskAttemptContext taskContext) throws IOException {
    writeFile(taskContext.getJobConf(), TASK_SETUP_FILE_NAME);
  }

  /**
   * 判断当前任务是否需要提交，此处固定返回true表示所有任务都需要提交
   * @param taskContext 任务尝试上下文对象
   * @return 固定返回true，表示需要执行任务提交
   * @throws IOException IO异常
   */
  @Override
  public boolean needsTaskCommit(TaskAttemptContext taskContext)
      throws IOException {
    return true;
  }

  /**
   * 任务提交成功阶段回调，生成任务提交标记文件
   * @param taskContext 任务尝试上下文对象
   * @throws IOException 文件创建IO异常
   */
  @Override
  public void commitTask(TaskAttemptContext taskContext) throws IOException {
    writeFile(taskContext.getJobConf(), TASK_COMMIT_FILE_NAME);
  }

  /**
   * 任务终止阶段回调，生成任务终止标记文件
   * @param taskContext 任务尝试上下文对象
   * @throws IOException 文件创建IO异常
   */
  @Override
  public void abortTask(TaskAttemptContext taskContext) throws IOException {
    writeFile(taskContext.getJobConf(), TASK_ABORT_FILE_NAME);
  }

  /**
   * 在作业输出目录下创建指定名称的空标记文件
   * @param conf 作业配置对象，用于获取输出路径和文件系统信息
   * @param filename 要创建的标记文件名
   * @throws IOException 文件创建或关闭IO异常
   */
  private void writeFile(JobConf conf , String filename) throws IOException {
    System.out.println("writing file ----" + filename);
    // 获取作业输出根路径
    Path outputPath = FileOutputFormat.getOutputPath(conf);
    // 获取对应文件系统实例
    FileSystem fs = outputPath.getFileSystem(conf);
    // 创建空文件后直接关闭
    fs.create(new Path(outputPath, filename)).close();
  }
}