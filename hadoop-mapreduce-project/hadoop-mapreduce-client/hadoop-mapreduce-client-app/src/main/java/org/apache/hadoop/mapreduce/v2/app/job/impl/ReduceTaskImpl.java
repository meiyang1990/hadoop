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

package org.apache.hadoop.mapreduce.v2.app.job.impl;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.ReduceTaskAttemptImpl;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.security.token.JobTokenIdentifier;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.TaskAttemptListener;
import org.apache.hadoop.mapreduce.v2.app.metrics.MRAppMetrics;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.util.Clock;

/**
 * Reduce任务实现类，继承TaskImpl抽象基类，负责管理MapReduce作业中单个Reduce任务的全生命周期
 * 核心职责：维护Reduce任务的元数据、创建Reduce任务尝试、获取任务配置参数，提供任务类型标识
 */
@SuppressWarnings({ "rawtypes" })
public class ReduceTaskImpl extends TaskImpl {
  
  // 当前Reduce任务需要等待处理的Map任务总数
  private final int numMapTasks;

  /**
   * 构造ReduceTaskImpl实例，初始化Reduce任务核心参数
   * @param jobId 所属作业ID
   * @param partition Reduce分区编号
   * @param eventHandler 事件处理器，用于处理任务相关事件
   * @param jobFile 作业文件路径
   * @param conf 作业配置对象
   * @param numMapTasks 作业总Map任务数
   * @param taskAttemptListener 任务尝试状态监听器
   * @param jobToken 作业认证令牌
   * @param credentials 作业凭据信息
   * @param clock 时钟工具，用于计时
   * @param appAttemptId 应用尝试ID
   * @param metrics MR应用 metrics指标收集器
   * @param appContext 应用上下文，保存整个MR应用运行时信息
   */
  public ReduceTaskImpl(JobId jobId, int partition,
      EventHandler eventHandler, Path jobFile, JobConf conf,
      int numMapTasks, TaskAttemptListener taskAttemptListener,
      Token<JobTokenIdentifier> jobToken,
      Credentials credentials, Clock clock,
      int appAttemptId, MRAppMetrics metrics, AppContext appContext) {
    super(jobId, TaskType.REDUCE, partition, eventHandler, jobFile, conf,
        taskAttemptListener, jobToken, credentials, clock,
        appAttemptId, metrics, appContext);
    this.numMapTasks = numMapTasks;
  }

  /**
   * 获取Reduce任务允许的最大尝试次数，从配置读取
   * @return 最大尝试次数，默认4次
   */
  @Override
  protected int getMaxAttempts() {
    return conf.getInt(MRJobConfig.REDUCE_MAX_ATTEMPTS, 4);
  }

  /**
   * 创建一个新的Reduce任务尝试实例
   * @return 新建的Reduce任务尝试对象
   */
  @Override
  protected TaskAttemptImpl createAttempt() {
    return new ReduceTaskAttemptImpl(getID(), nextAttemptNumber,
        eventHandler, jobFile,
        partition, numMapTasks, conf, taskAttemptListener,
        jobToken, credentials, clock, appContext);
  }

  /**
   * 获取当前任务的类型
   * @return 返回REDUCE类型标识
   */
  @Override
  public TaskType getType() {
    return TaskType.REDUCE;
  }

}