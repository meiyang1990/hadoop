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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.security.token.JobTokenIdentifier;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.TaskAttemptListener;
import org.apache.hadoop.mapreduce.v2.app.job.impl.TaskAttemptImpl;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.util.Clock;

/**
 * Reduce任务尝试实现类，负责在YARN框架下管理单个Reduce任务尝试的生命周期
 * 继承通用TaskAttemptImpl，扩展Reduce任务特有的逻辑，负责构造可远程执行的Reduce任务实例
 */
@SuppressWarnings("rawtypes")
public class ReduceTaskAttemptImpl extends TaskAttemptImpl {

  // 当前Reduce任务需要处理的Map任务总数
  private final int numMapTasks;

  /**
   * 构造Reduce任务尝试实例
   * @param id 任务尝试编号
   * @param attempt 尝试次数
   * @param eventHandler 事件处理器，用于处理任务状态变更事件
   * @param jobFile 作业文件路径
   * @param partition 当前Reduce处理的数据分区编号
   * @param numMapTasks 作业的Map任务总数
   * @param conf 作业配置对象
   * @param taskAttemptListener 任务尝试状态监听器
   * @param jobToken 作业认证令牌
   * @param credentials 作业凭证信息
   * @param clock 时钟工具，用于时间计算
   * @param appContext 应用上下文，保存作业运行全局信息
   */
  public ReduceTaskAttemptImpl(TaskId id, int attempt,
      EventHandler eventHandler, Path jobFile, int partition,
      int numMapTasks, JobConf conf,
      TaskAttemptListener taskAttemptListener,
      Token<JobTokenIdentifier> jobToken,
      Credentials credentials, Clock clock,
      AppContext appContext) {
    super(id, attempt, eventHandler, taskAttemptListener, jobFile, partition,
        conf, new String[] {}, jobToken, credentials, clock,
        appContext);
    this.numMapTasks = numMapTasks;
  }

  /**
   * 创建可在NM节点远程执行的Reduce任务实例
   * @return 构造完成的可执行Reduce任务对象
   */
  @Override
  public Task createRemoteTask() {
  //job file name is set in TaskAttempt, setting it null here
    ReduceTask reduceTask =
      new ReduceTask("", TypeConverter.fromYarn(getID()), partition,
          numMapTasks, 1); // YARN doesn't have the concept of slots per task, set it as 1.
  reduceTask.setUser(conf.get(MRJobConfig.USER_NAME));
  reduceTask.setConf(conf);
    return reduceTask;
  }

}