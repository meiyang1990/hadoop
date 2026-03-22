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
import org.apache.hadoop.mapred.MapTaskAttemptImpl;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.security.token.JobTokenIdentifier;
import org.apache.hadoop.mapreduce.split.JobSplit.TaskSplitMetaInfo;
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
 * MapReduce Map任务实现类，继承TaskImpl，负责管理Map任务的生命周期和核心属性
 * 承担Map任务实例的构建、尝试创建、配置读取等核心逻辑，是Map任务在ApplicationMaster中的表示
 */
@SuppressWarnings({ "rawtypes" })
public class MapTaskImpl extends TaskImpl {

  // 当前Map任务对应的数据分片元信息，包含输入数据位置和大小等信息
  private final TaskSplitMetaInfo taskSplitMetaInfo;

  /**
   * 构造MapTaskImpl实例，初始化Map任务核心属性
   * @param jobId 所属作业ID
   * @param partition 任务分区编号
   * @param eventHandler 事件处理器，用于处理任务相关事件
   * @param remoteJobConfFile 远程作业配置文件路径
   * @param conf 作业配置对象
   * @param taskSplitMetaInfo Map任务对应的数据分片元信息
   * @param taskAttemptListener 任务尝试监听器，用于监听尝试状态变化
   * @param jobToken 作业认证令牌
   * @param credentials 作业凭证信息
   * @param clock 时钟工具，用于时间计算
   * @param appAttemptId 应用尝试ID
   * @param metrics MR应用 metrics 收集器
   * @param appContext 应用上下文，保存全局应用信息
   */
  public MapTaskImpl(JobId jobId, int partition, EventHandler eventHandler,
      Path remoteJobConfFile, JobConf conf,
      TaskSplitMetaInfo taskSplitMetaInfo,
      TaskAttemptListener taskAttemptListener,
      Token<JobTokenIdentifier> jobToken,
      Credentials credentials, Clock clock,
      int appAttemptId, MRAppMetrics metrics, AppContext appContext) {
    super(jobId, TaskType.MAP, partition, eventHandler, remoteJobConfConfFile,
        conf, taskAttemptListener, jobToken, credentials, clock,
        appAttemptId, metrics, appContext);
    this.taskSplitMetaInfo = taskSplitMetaInfo;
  }

  @Override
  /**
   * 获取Map任务最大重试次数，从作业配置中读取，默认值为4
   * @return Map任务最大重试次数
   */
  protected int getMaxAttempts() {
    return conf.getInt(MRJobConfig.MAP_MAX_ATTEMPTS, 4);
  }

  @Override
  /**
   * 创建新的Map任务尝试实例，用于任务重试
   * @return 新建的MapTaskAttemptImpl实例
   */
  protected TaskAttemptImpl createAttempt() {
    return new MapTaskAttemptImpl(getID(), nextAttemptNumber,
        eventHandler, jobFile,
        partition, taskSplitMetaInfo, conf, taskAttemptListener,
        jobToken, credentials, clock, appContext);
  }

  @Override
  /**
   * 获取当前任务类型，固定返回MAP类型
   * @return 任务类型MAP
   */
  public TaskType getType() {
    return TaskType.MAP;
  }

  /**
   * 获取当前Map任务对应的数据分片元信息
   * @return 数据分片元信息对象
   */
  protected TaskSplitMetaInfo getTaskSplitMetaInfo() {
    return this.taskSplitMetaInfo;
  }

  /**
   * @return a String formatted as a comma-separated list of splits.
   */
  @Override
  /**
   * 将当前Map任务的数据块位置拼接为逗号分隔的字符串，用于日志和监控展示
   * @return 逗号分隔的数据位置字符串
   */
  protected String getSplitsAsString() {
    // 获取当前分片所在的节点位置列表
    String[] splits = getTaskSplitMetaInfo().getLocations();
    if (splits == null || splits.length == 0)
    return "";
    StringBuilder sb = new StringBuilder();
    // 遍历拼接位置节点，用逗号分隔
    for (int i = 0; i < splits.length; i++) {
      if (i != 0) sb.append(",");
      sb.append(splits[i]);
    }
    return sb.toString();
  }
}