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

package org.apache.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * OutputCommitter 定义了MapReduce作业任务输出的提交协议，负责管理作业和任务输出的生命周期。
 * 
 * <p>MapReduce框架依赖OutputCommitter完成以下核心职责：<p>
 * <ol>
 *   <li>
 *   在作业初始化阶段完成作业输出准备工作，例如创建作业临时输出目录。
 *   </li>
 *   <li>
 *   在作业完成后清理作业相关临时资源，例如删除作业临时输出目录。
 *   </li>
 *   <li>
 *   为单个任务准备临时输出空间。
 *   </li> 
 *   <li>
 *   检查任务是否需要执行提交操作，避免不必要的提交流程。
 *   </li>
 *   <li>
 *   提交任务输出到最终位置。
 *   </li>  
 *   <li>
 *   丢弃任务未提交的输出，清理临时资源。
 *   </li>
 * </ol>
 * 该类中的方法可能会从多个进程、多个上下文中被调用，需要实现者注意调用场景。
 * 并非所有方法都保证只会被调用一次，OutputCommitter需要能够处理重复调用的场景。
 * 同一任务被多次调用仅会在异常场景下发生。
 * 
 * @see org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter 
 * @see JobContext
 * @see TaskAttemptContext 
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class OutputCommitter {
  /**
   * 在作业初始化阶段设置作业输出，由应用主进程为整个作业调用。
   * 每次作业尝试都会调用一次，因此可能会被调用多次。
   * 
   * @param jobContext 当前作业的上下文对象
   * @throws IOException 如果创建临时输出失败则抛出异常
   */
  public abstract void setupJob(JobContext jobContext) throws IOException;

  /**
   * 作业完成后清理作业输出，由应用主进程为整个作业调用，可能会被调用多次。
   * 
   * @param jobContext 当前作业的上下文对象
   * @throws IOException
   * @deprecated 请使用 {@link #commitJob(JobContext)} 和 {@link #abortJob(JobContext, JobStatus.State)} 替代
   */
  @Deprecated
  public void cleanupJob(JobContext jobContext) throws IOException { }

  /**
   * 在作业成功完成后提交作业最终输出，仅当作业最终状态为SUCCESSFUL时被调用。
   * 由应用主进程为整个作业调用，保证只会被调用一次，如果抛出异常整个作业会失败。
   * 
   * @param jobContext 当前作业的上下文对象
   * @throws IOException
   */
  public void commitJob(JobContext jobContext) throws IOException {
    cleanupJob(jobContext);
  }

  
  /**
   * 中止未成功完成作业的输出清理，当作业最终状态为FAILED或KILLED时被调用。
   * 由应用主进程为整个作业调用，可能会被调用多次。
   *
   * @param jobContext 当前作业的上下文对象
   * @param state 作业最终运行状态
   * @throws IOException
   */
  public void abortJob(JobContext jobContext, JobStatus.State state) 
  throws IOException {
    cleanupJob(jobContext);
  }
  
  /**
   * 为单个任务准备输出环境，由每个任务进程在任务执行前调用。
   * 同一任务的不同尝试可能会被多次调用。
   * 
   * @param taskContext 当前任务尝试的上下文对象
   * @throws IOException
   */
  public abstract void setupTask(TaskAttemptContext taskContext)
  throws IOException;
  
  /**
   * 检查当前任务是否需要执行输出提交操作，由任务进程在任务完成后调用。
   * 
   * @param taskContext 当前任务尝试的上下文对象
   * @return true表示需要提交，false表示不需要提交
   * @throws IOException
   */
  public abstract boolean needsTaskCommit(TaskAttemptContext taskContext)
  throws IOException;

  /**
   * 将任务的临时输出移动到最终输出位置，当needsTaskCommit返回true且任务成功时由任务进程调用。
   * 该方法用于标记单个任务输出完成，整个作业成功后还会调用commitJob提交作业输出。
   * 同一任务的不同尝试可能会被多次调用，仅在异常网络故障场景下会发生重复调用。
   * 
   * @param taskContext 当前任务尝试的上下文对象
   * @throws IOException 如果提交失败则抛出异常
   */
  public abstract void commitTask(TaskAttemptContext taskContext)
  throws IOException;
  
  /**
   * 丢弃任务未提交的输出，清理任务临时资源，由任务进程调用。
   * 同一任务的不同尝试可能会被多次调用。
   * 
   * @param taskContext 当前任务尝试的上下文对象
   * @throws IOException
   */
  public abstract void abortTask(TaskAttemptContext taskContext)
  throws IOException;

  /**
   * 检查是否支持重启作业时恢复任务输出。
   * 如果支持恢复，作业重启可以更高效完成。
   * 
   * @return true表示支持任务输出恢复，false表示不支持
   * @see #recoverTask(TaskAttemptContext)
   * @deprecated 请使用 {@link #isRecoverySupported(JobContext)} 替代
   */
  @Deprecated
  public boolean isRecoverySupported() {
    return false;
  }

  /**
   * 检查是否支持重复执行作业提交操作。如果返回true，当应用主进程重启后可以重试之前未完成的提交操作。
   * 注意在异常场景下，之前的应用主进程可能仍在运行，因此如果返回true，重试提交需要能够和之前的提交并发运行。
   * 
   * 如果支持可重复作业提交，作业重启可以容忍应用主进程在提交阶段失败。
   * 默认不支持该特性，具体实现类（如FileOutputCommitter）需要主动覆盖该方法开启支持。
   *
   * @param jobContext 当前作业的上下文对象
   * @return true表示支持可重复作业提交，false表示不支持
   * @throws IOException
   */
  public boolean isCommitJobRepeatable(JobContext jobContext)
      throws IOException {
    return false;
  }

  /**
   * 检查是否支持重启作业时恢复任务输出。
   * 如果支持恢复，作业重启可以更高效完成。
   * 
   * @param jobContext 当前作业的上下文对象
   * @return true表示支持任务输出恢复，false表示不支持
   * @throws IOException
   * @see #recoverTask(TaskAttemptContext)
   */
  public boolean isRecoverySupported(JobContext jobContext) throws IOException {
    return isRecoverySupported();
  }

  /**
   * 恢复已完成任务的输出，用于作业重启场景。
   * 重试次数可以通过任务上下文配置中的APPLICATION_ATTEMPT_ID获取。
   * 由应用主进程调用，每个需要恢复的任务单独调用一次。
   * 如果抛出异常，该任务会重新尝试执行。同一任务的不同应用尝试可能会被多次调用。
   * 
   * @param taskContext 需要恢复的任务尝试上下文对象
   * @throws IOException
   */
  public void recoverTask(TaskAttemptContext taskContext)
  throws IOException
  {}
}