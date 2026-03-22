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

package org.apache.hadoop.mapreduce.lib.output;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.fs.statistics.IOStatistics;
import org.apache.hadoop.fs.statistics.IOStatisticsSource;
import org.apache.hadoop.fs.statistics.IOStatisticsSupport;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * 绑定动态输出提交器的代理实现，支持通过类名实例化并动态加载实际输出提交器。
 * 核心作用是兼容现有通过类名配置提交器的代码，同时保留根据输出文件系统动态选择提交器的能力。
 * 所有输出提交生命周期操作都会委托给内部绑定的实际提交器执行，自身不实现核心逻辑。
 * 该类本身不需要对应的工厂类，避免循环依赖。
 * 
 * 使用方式：
 * <ol>
 *   <li>
 *     在配置项中需要填写提交器类名的地方，填写此类的规范名称（见{@link #NAME}）。
 *     此类实例化时，会通过工厂机制根据输出路径自动定位对应文件系统的配置提交器。
 *   </li>
 *   <li>
 *     在代码中显式通过构造函数创建实例，然后调用生命周期方法。
 *     动态配置的提交器会在构造阶段被创建，所有操作都会转发给它执行。
 *   </li>
 * </ol>
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class BindingPathOutputCommitter extends PathOutputCommitter
    implements IOStatisticsSource, StreamCapabilities {

  /**
   * 用于配置的类全限定名常量。
   */
  public static final String NAME
      = BindingPathOutputCommitter.class.getCanonicalName();

  /**
   * 被代理的实际输出提交器实例。
   */
  private final PathOutputCommitter committer;

  /**
   * 构造绑定输出提交器，动态创建实际的输出提交器实例。
   * @param outputPath 输出路径，可为null
   * @param context 任务尝试上下文
   * @throws IOException 创建失败时抛出异常
   */
  public BindingPathOutputCommitter(Path outputPath,
      TaskAttemptContext context) throws IOException {
    super(outputPath, context);
    // 根据输出路径获取对应工厂，创建实际输出提交器
    committer = PathOutputCommitterFactory.getCommitterFactory(outputPath,
        context.getConfiguration())
        .createOutputCommitter(outputPath, context);
  }

  @Override
  public Path getOutputPath() {
    return committer.getOutputPath();
  }

  @Override
  public Path getWorkPath() throws IOException {
    return committer.getWorkPath();
  }

  @Override
  public void setupJob(JobContext jobContext) throws IOException {
    committer.setupJob(jobContext);
  }

  @Override
  public void setupTask(TaskAttemptContext taskContext) throws IOException {
    committer.setupTask(taskContext);
  }

  @Override
  public boolean needsTaskCommit(TaskAttemptContext taskContext)
      throws IOException {
    return committer.needsTaskCommit(taskContext);
  }

  @Override
  public void commitTask(TaskAttemptContext taskContext) throws IOException {
    committer.commitTask(taskContext);
  }

  @Override
  public void abortTask(TaskAttemptContext taskContext) throws IOException {
    committer.abortTask(taskContext);
  }

  @Override
  @SuppressWarnings("deprecation")
  public void cleanupJob(JobContext jobContext) throws IOException {
    super.cleanupJob(jobContext);
  }

  @Override
  public void commitJob(JobContext jobContext) throws IOException {
    committer.commitJob(jobContext);
  }

  @Override
  public void abortJob(JobContext jobContext, JobStatus.State state)
      throws IOException {
    committer.abortJob(jobContext, state);
  }

  @SuppressWarnings("deprecation")
  @Override
  public boolean isRecoverySupported() {
    return committer.isRecoverySupported();
  }

  @Override
  public boolean isCommitJobRepeatable(JobContext jobContext)
      throws IOException {
    return committer.isCommitJobRepeatable(jobContext);
  }

  @Override
  public boolean isRecoverySupported(JobContext jobContext) throws IOException {
    return committer.isRecoverySupported(jobContext);
  }

  @Override
  public void recoverTask(TaskAttemptContext taskContext) throws IOException {
    committer.recoverTask(taskContext);
  }

  @Override
  public boolean hasOutputPath() {
    return committer.hasOutputPath();
  }

  @Override
  public String toString() {
    return "BindingPathOutputCommitter{"
        + "committer=" + committer +
        '}';
  }

  /**
   * 获取内部绑定的实际输出提交器。
   * @return 被代理的实际输出提交器实例
   */
  public PathOutputCommitter getCommitter() {
    return committer;
  }

  /**
   * 委托内部提交器检查是否支持指定能力。
   * {@inheritDoc}
   */
  @Override
  public boolean hasCapability(final String capability) {
    // 如果内部提交器实现了StreamCapabilities接口，委托调用
    if (committer instanceof StreamCapabilities) {
      return ((StreamCapabilities) committer).hasCapability(capability);
    } else {
      return false;
    }
  }

  @Override
  public IOStatistics getIOStatistics() {
    // 从内部提交器提取IO统计信息
    return IOStatisticsSupport.retrieveIOStatistics(committer);
  }
}