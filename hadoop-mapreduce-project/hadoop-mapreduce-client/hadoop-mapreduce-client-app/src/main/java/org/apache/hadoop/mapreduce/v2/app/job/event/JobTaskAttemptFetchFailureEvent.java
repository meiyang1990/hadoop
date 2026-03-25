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

package org.apache.hadoop.mapreduce.v2.app.job.event;

import java.util.List;

import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;

/**
 * 作业任务尝试读取Map输出失败事件
 * 当Reduce任务尝试从Map任务拉取shuffle数据失败时，向Job发出该事件
 * 用于触发任务推测执行或重新调度失败的Map输出获取
 */
public class JobTaskAttemptFetchFailureEvent extends JobEvent {

  private final TaskAttemptId reduce;
  private final List<TaskAttemptId> maps;
  private final String hostname;

  /**
   * 构造任务尝试拉取Map输出失败事件
   * @param reduce 发生拉取失败的Reduce任务尝试ID
   * @param maps 拉取失败的Map任务尝试列表
   * @param host Reduce任务运行所在的主机名
   */
  public JobTaskAttemptFetchFailureEvent(TaskAttemptId reduce, 
      List<TaskAttemptId> maps, String host) {
    super(reduce.getTaskId().getJobId(),
        JobEventType.JOB_TASK_ATTEMPT_FETCH_FAILURE);
    this.reduce = reduce;
    this.maps = maps;
    this.hostname = host;
  }

  /**
   * 获取拉取失败的Map任务尝试列表
   * @return 拉取失败的Map任务尝试ID列表
   */
  public List<TaskAttemptId> getMaps() {
    return maps;
  }

  /**
   * 获取发生拉取失败的Reduce任务尝试ID
   * @return Reduce任务尝试ID
   */
  public TaskAttemptId getReduce() {
    return reduce;
  }

  /**
   * 获取发生拉取失败的Reduce所在主机名
   * @return 主机名
   */
  public String getHost() {
    return hostname;
  }
}