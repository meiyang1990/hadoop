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

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.task.annotation.Checkpointable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * 文件输出提交器的部分提交实现，支持可抢占任务的增量输出提交。
 * 工作在${mapreduce.output.fileoutputformat.outputdir}作业输出目录下，
 * 允许任务在执行过程中逐步提交中间输出，并在任务重启后清理旧的部分输出，
 * 适用于可重启、支持检查点的长任务场景。
 */
@Checkpointable
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class PartialFileOutputCommitter
    extends FileOutputCommitter implements PartialOutputCommitter {

  private static final Logger LOG =
      LoggerFactory.getLogger(PartialFileOutputCommitter.class);


  /**
   * 构造函数，指定输出路径和任务尝试上下文，初始化部分输出提交器
   * @param outputPath 作业输出根路径
   * @param context 任务尝试上下文
   * @throws IOException 初始化时IO异常
   */
  public PartialFileOutputCommitter(Path outputPath,
                             TaskAttemptContext context) throws IOException {
    super(outputPath, context);
  }

  /**
   * 构造函数，指定输出路径和作业上下文，初始化部分输出提交器
   * @param outputPath 作业输出根路径
   * @param context 作业上下文
   * @throws IOException 初始化时IO异常
   */
  public PartialFileOutputCommitter(Path outputPath,
                             JobContext context) throws IOException {
    super(outputPath, context);
  }

  @Override
  public Path getCommittedTaskPath(int appAttemptId, TaskAttemptContext context) {
    return new Path(getJobAttemptPath(appAttemptId),
        String.valueOf(context.getTaskAttemptID()));
  }

  /**
   * 获取指定路径对应的文件系统实例，仅用于测试
   * @param p 目标路径
   * @param conf 配置对象
   * @return 对应路径的文件系统实例
   * @throws IOException 获取文件系统时IO异常
   */
  @VisibleForTesting
  FileSystem fsFor(Path p, Configuration conf) throws IOException {
    return p.getFileSystem(conf);
  }

  @Override
  /**
   * 清理当前任务已提交的旧部分输出，仅保留当前尝试之前的输出
   * 用于任务被抢占重启后，清理本次任务之前尝试产生的部分输出，避免输出冗余
   * @param context 当前任务尝试上下文
   * @throws IOException 清理过程中IO异常
   */
  public void cleanUpPartialOutputForTask(TaskAttemptContext context)
      throws IOException {

    // we double check this is never invoked from a non-preemptable subclass.
    // This should never happen, since the invoking codes is checking it too,
    // but it is safer to double check. Errors handling this would produce
    // inconsistent output.
    // 双重校验：确保仅从支持Checkpointable的类调用
    if (!this.getClass().isAnnotationPresent(Checkpointable.class)) {
      throw new IllegalStateException("Invoking cleanUpPartialOutputForTask() " +
          "from non @Preemptable class");
    }
    // 获取文件系统实例
    FileSystem fs =
      fsFor(getTaskAttemptPath(context), context.getConfiguration());

    LOG.info("cleanUpPartialOutputForTask: removing everything belonging to " +
        context.getTaskAttemptID().getTaskID() + " in: " +
        getCommittedTaskPath(context).getParent());

    // 获取当前任务和任务尝试ID
    final TaskAttemptID taid = context.getTaskAttemptID();
    final TaskID tid = taid.getTaskID();
    // 获取任务已提交输出的父目录
    Path pCommit = getCommittedTaskPath(context).getParent();
    // 删除当前尝试ID之前所有尝试的已提交输出
    for (int i = 0; i < taid.getId(); ++i) {
      TaskAttemptID oldId = new TaskAttemptID(tid, i);
      Path pTask = new Path(pCommit, oldId.toString());
      if (!fs.delete(pTask, true) && fs.exists(pTask)) {
        throw new IOException("Failed to delete " + pTask);
      }
    }
  }

}