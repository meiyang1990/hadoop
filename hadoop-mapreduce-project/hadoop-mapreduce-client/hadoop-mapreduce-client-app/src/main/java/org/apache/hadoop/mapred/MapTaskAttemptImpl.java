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
import org.apache.hadoop.mapreduce.split.JobSplit.TaskSplitMetaInfo;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.TaskAttemptListener;
import org.apache.hadoop.mapreduce.v2.app.job.impl.TaskAttemptImpl;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.util.Clock;

/**
 * Map任务尝试实现类，兼容旧版MapReduce API，负责维护Map任务尝试的状态和数据
 * 继承TaskAttemptImpl，实现Map任务远端执行对象的构造
 */
@SuppressWarnings("rawtypes")
public class MapTaskAttemptImpl extends TaskAttemptImpl {

  // 存储当前Map任务处理的数据分片元信息
  private final TaskSplitMetaInfo splitInfo;

  /**
   * 构造Map任务尝试实例，初始化分片信息并调用父类构造
   * @param taskId YARN格式的任务ID
   * @param attempt 尝试编号
   * @param eventHandler 事件处理器，用于处理任务状态变更事件
   * @param jobFile 作业描述文件路径
   * @param partition 分区编号
   * @param splitInfo 数据分片元信息
   * @param conf 作业配置
   * @param taskAttemptListener 任务尝试监听器
   * @param jobToken 作业认证令牌
   * @param credentials 作业凭证信息
   * @param clock 时钟工具
   * @param appContext MR应用上下文
   */
  public MapTaskAttemptImpl(TaskId taskId, int attempt, 
      EventHandler eventHandler, Path jobFile, 
      int partition, TaskSplitMetaInfo splitInfo, JobConf conf,
      TaskAttemptListener taskAttemptListener, 
      Token<JobTokenIdentifier> jobToken,
      Credentials credentials, Clock clock,
      AppContext appContext) {
    super(taskId, attempt, eventHandler, 
        taskAttemptListener, jobFile, partition, conf, splitInfo.getLocations(),
        jobToken, credentials, clock, appContext);
    this.splitInfo = splitInfo;
  }

  /**
   * 创建用于NodeManager上远程执行的MapTask实例
   * @return 可远程执行的旧版MapTask对象
   */
  @Override
  public Task createRemoteTask() {
    //job file name is set in TaskAttempt, setting it null here
    MapTask mapTask =
      new MapTask("", TypeConverter.fromYarn(getID()), partition,
          splitInfo.getSplitIndex(), 1); // YARN doesn't have the concept of slots per task, set it as 1.
    mapTask.setUser(conf.get(MRJobConfig.USER_NAME));
    mapTask.setConf(conf);
    return mapTask;
  }

}