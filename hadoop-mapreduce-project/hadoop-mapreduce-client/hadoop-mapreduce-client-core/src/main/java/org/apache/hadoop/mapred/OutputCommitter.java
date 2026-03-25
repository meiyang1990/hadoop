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
import org.apache.hadoop.classification.InterfaceStability;

/**
 * 文件级注释：
 * 该文件是MapReduce旧版API中的输出提交器抽象基类，负责管理MapReduce作业和任务的输出提交流程
 * 核心职责是提供作业初始化输出准备、任务临时输出管理、最终输出提交/回滚的标准化接口，
 * 让MapReduce框架可以统一处理不同输出格式的输出提交逻辑，同时支持作业失败重试和输出恢复
 *
 * <code>OutputCommitter</code> describes the commit of task output for a 
 * Map-Reduce job.
 *
 * <p>The Map-Reduce framework relies on the <code>OutputCommitter</code> of 
 * the job to:<p>
 * <ol>
 *   <li>
 *   Setup the job during initialization. For example, create the temporary 
 *   output directory for the job during the initialization of the job.
 *   </li>
 *   <li>
 *   Cleanup the job after the job completion. For example, remove the
 *   temporary output directory after the job completion. 
 *   </li>
 *   <li>
 *   Setup the task temporary output.
 *   </li> 
 *   <li>
 *   Check whether a task needs a commit. This is to avoid the commit
 *   procedure if a task does not need commit.
 *   </li>
 *   <li>
 *   Commit of the task output.
 *   </li>  
 *   <li>
 *   Discard the task commit.
 *   </li>
 * </ol>
 * The methods in this class can be called from several different processes and
 * from several different contexts.  It is important to know which process and
 * which context each is called from.  Each method should be marked accordingly
 * in its documentation.  It is also important to note that not all methods are
 * guaranteed to be called once and only once.  If a method is not guaranteed to
 * have this property the output committer needs to handle this appropriately. 
 * Also note it will only be in rare situations where they may be called 
 * multiple times for the same task.
 * 
 * @see FileOutputCommitter 
 * @see JobContext
 * @see TaskAttemptContext 
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class OutputCommitter 
                extends org.apache.hadoop.mapreduce.OutputCommitter {
  /**
   * 作业级输出初始化方法，在作业初始化阶段由应用主进程调用
   * 负责创建作业所需的临时输出目录等初始化工作，每个作业尝试都会调用一次
   * 
   * @param jobContext 当前作业上下文，包含作业配置和信息
   * @throws IOException 初始化失败时抛出IO异常
   */
  public abstract void setupJob(JobContext jobContext) throws IOException;

  /**
   * 作业完成后清理临时输出方法，已废弃
   * 原负责在作业完成后清理临时输出目录，由应用主进程调用，可能被调用多次
   * 
   * @param jobContext 当前作业上下文
   * @throws IOException
   * @deprecated Use {@link #commitJob(JobContext)} or 
   *                 {@link #abortJob(JobContext, int)} instead.
   */
  @Deprecated
  public void cleanupJob(JobContext jobContext) throws IOException { }

  /**
   * 作业成功完成后提交最终输出方法，由应用主进程调用，仅调用一次
   * 仅当作业最终状态为SUCCESSFUL时才会调用，如果抛出异常整个作业会标记为失败
   * 
   * @param jobContext 当前作业上下文
   * @throws IOException 
   */
  public void commitJob(JobContext jobContext) throws IOException {
    cleanupJob(jobContext);
  }
  
  /**
   * 作业失败/被杀死后终止作业输出，由应用主进程调用，可能被调用多次
   * 仅当作业最终状态为FAILED或KILLED时才会调用，负责清理作业输出
   * 
   * @param jobContext 当前作业上下文
   * @param status 作业最终运行状态（FAILED/KILLED）
   * @throws IOException
   */
  public void abortJob(JobContext jobContext, int status) 
  throws IOException {
    cleanupJob(jobContext);
  }
  
  /**
   * 任务级输出初始化方法，在每个任务进程中调用，为任务准备临时输出
   * 同一个任务的不同尝试都会调用一次，可能被调用多次
   * 
   * @param taskContext 当前任务尝试上下文
   * @throws IOException
   */
  public abstract void setupTask(TaskAttemptContext taskContext)
  throws IOException;
  
  /**
   * 检查当前任务是否需要提交输出，在每个任务进程中调用
   * 用于跳过不需要提交输出的任务，减少不必要的提交操作
   * 
   * @param taskContext 当前任务尝试上下文
   * @return true表示需要提交，false表示不需要
   * @throws IOException
   */
  public abstract boolean needsTaskCommit(TaskAttemptContext taskContext)
  throws IOException;

  /**
   * 提交当前任务的输出，将临时输出移动到最终输出位置，在任务进程中调用
   * 仅当needsTaskCommit返回true，且该任务尝试被判定为成功时才会调用
   * 同一个任务的不同尝试都会调用一次，作业整体成功后还会调用作业级提交
   * 
   * @param taskContext 当前任务尝试上下文
   * @throws IOException if commit is not 
   */
  public abstract void commitTask(TaskAttemptContext taskContext)
  throws IOException;
  
  /**
   * 丢弃当前任务尝试的输出，清理任务临时文件，在任务进程中调用
   * 当任务尝试失败不需要提交时调用，同一个任务的不同尝试都会调用一次
   * 
   * @param taskContext 当前任务尝试上下文
   * @throws IOException
   */
  public abstract void abortTask(TaskAttemptContext taskContext)
  throws IOException;

  /**
   * 检查是否支持任务输出恢复，已废弃，使用isRecoverySupported(JobContext)替代
   * 该方法是新旧API的兼容桥接方法，默认返回false不支持恢复
   * 
   * @deprecated Use {@link #isRecoverySupported(JobContext)} instead.
   */
  @Deprecated
  @Override
  public boolean isRecoverySupported() {
    return false;
  }

  /**
   * 检查当前作业是否支持任务输出恢复，用于作业重启场景
   * 如果支持恢复，作业重启可以复用已完成任务的输出，提升重启效率
   *
   * @param jobContext 当前作业上下文
   * @return <code>true</code>支持恢复，<code>false</code>不支持
   * @throws IOException
   * @see #recoverTask(TaskAttemptContext)
   */
  public boolean isRecoverySupported(JobContext jobContext) throws IOException {
    return isRecoverySupported();
  }

  /**
   * 检查作业提交是否支持重试，用于应用主进程(AM)失败重启场景
   * 如果支持重试，AM重启后可以重试未完成的作业提交，提升作业成功率
   * 默认不支持，具体实现类（如FileOutputCommitter）可覆盖开启支持
   * 若返回true，要求重试提交可以和之前未完成的提交并发执行
   *
   * @param jobContext 当前作业上下文
   * @return <code>true</code>支持重试提交，<code>false</code>不支持
   * @throws IOException
   */
  public boolean isCommitJobRepeatable(JobContext jobContext) throws
      IOException {
    return false;
  }

  @Override
  public boolean isCommitJobRepeatable(org.apache.hadoop.mapreduce.JobContext
      jobContext) throws IOException {
    return isCommitJobRepeatable((JobContext) jobContext);
  }

  /**
   * 恢复已完成任务的输出，在作业重启时由应用主进程逐个任务调用
   * 用于作业重启后恢复之前已经完成的任务输出，避免重新执行任务
   * 重试次数可从任务上下文中的APPLICATION_ATTEMPT_ID获取，如果抛出异常任务会重新执行
   * 
   * @param taskContext 需要恢复的任务上下文
   * @throws IOException
   */
  public void recoverTask(TaskAttemptContext taskContext) 
  throws IOException {
  }
  
  /**
   * 新版API接口适配方法，桥接调用旧版API的setupJob方法
   * 用于兼容新旧MapReduce API，参数类型转换后调用旧版实现
   */
  @Override
  public final void setupJob(org.apache.hadoop.mapreduce.JobContext jobContext
                             ) throws IOException {
    setupJob((JobContext) jobContext);
  }

  /**
   * 新版API接口适配方法，桥接调用旧版API的cleanupJob方法，已废弃
   * 用于兼容新旧MapReduce API，参数类型转换后调用旧版实现
   * @deprecated Use {@link #commitJob(org.apache.hadoop.mapreduce.JobContext)}
   *             or {@link #abortJob(org.apache.hadoop.mapreduce.JobContext, org.apache.hadoop.mapreduce.JobStatus.State)}
   *             instead.
   */
  @Override
  @Deprecated
  public final void cleanupJob(org.apache.hadoop.mapreduce.JobContext context
                               ) throws IOException {
    cleanupJob((JobContext) context);
  }

  /**
   * 新版API接口适配方法，桥接调用旧版API的commitJob方法
   * 用于兼容新旧MapReduce API，参数类型转换后调用旧版实现
   */
  @Override
  public final void commitJob(org.apache.hadoop.mapreduce.JobContext context
                             ) throws IOException {
    commitJob((JobContext) context);
  }
  
  /**
   * 新版API接口适配方法，桥接调用旧版API的abortJob方法
   * 用于兼容新旧MapReduce API，完成状态类型转换后调用旧版实现
   */
  @Override
  public final void abortJob(org.apache.hadoop.mapreduce.JobContext context, 
		                   org.apache.hadoop.mapreduce.JobStatus.State runState) 
  throws IOException {
    // 将新版API的作业状态转换为旧版API的整型状态码
    int state = JobStatus.getOldNewJobRunState(runState);
    if (state != JobStatus.FAILED && state != JobStatus.KILLED) {
      throw new IOException ("Invalid job run state : " + runState.name());
    }
    abortJob((JobContext) context, state);
  }
  
  /**
   * 新版API接口适配方法，桥接调用旧版API的setupTask方法
   * 用于兼容新旧MapReduce API，参数类型转换后调用旧版实现
   */
  @Override
  public final 
  void setupTask(org.apache.hadoop.mapreduce.TaskAttemptContext taskContext
                 ) throws IOException {
    setupTask((TaskAttemptContext) taskContext);
  }
  
  /**
   * 新版API接口适配方法，桥接调用旧版API的needsTaskCommit方法
   * 用于兼容新旧MapReduce API，参数类型转换后调用旧版实现
   */
  @Override
  public final boolean 
    needsTaskCommit(org.apache.hadoop.mapreduce.TaskAttemptContext taskContext
                    ) throws IOException {
    return needsTaskCommit((TaskAttemptContext) taskContext);
  }

  /**
   * 新版API接口适配方法，桥接调用旧版API的commitTask方法
   * 用于兼容新旧MapReduce API，参数类型转换后调用旧版实现
   */
  @Override
  public final 
  void commitTask(org.apache.hadoop.mapreduce.TaskAttemptContext taskContext
                  ) throws IOException {
    commitTask((TaskAttemptContext) taskContext);
  }
  
  /**
   * 新版API接口适配方法，桥接调用旧版API的abortTask方法
   * 用于兼容新旧MapReduce API，参数类型转换后调用旧版实现
   */
  @Override
  public final 
  void abortTask(org.apache.hadoop.mapreduce.TaskAttemptContext taskContext
                 ) throws IOException {
    abortTask((TaskAttemptContext) taskContext);
  }
  
  /**
   * 新版API接口适配方法，桥接调用旧版API的recoverTask方法
   * 用于兼容新旧MapReduce API，参数类型转换后调用旧版实现
   */
  @Override
  public final 
  void recoverTask(org.apache.hadoop.mapreduce.TaskAttemptContext taskContext
      ) throws IOException {
    recoverTask((TaskAttemptContext) taskContext);
  }

  /**
   * 新版API接口适配方法，桥接调用旧版API的isRecoverySupported方法
   * 用于兼容新旧MapReduce API，参数类型转换后调用旧版实现
   */
  @Override
  public final boolean isRecoverySupported(
      org.apache.hadoop.mapreduce.JobContext context) throws IOException {
    return isRecoverySupported((JobContext) context);
  }

}