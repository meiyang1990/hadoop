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

import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;

/**
 * 任务尝试过多获取失败事件，当Reduce任务多次拉取某个Map任务输出失败时触发
 * 用于通知作业调度器标记对应的Map任务尝试失败并重试
 */
public class TaskAttemptTooManyFetchFailureEvent extends TaskAttemptEvent {
  private TaskAttemptId reduceID;
  private String  reduceHostname;

  /**
   * 构造任务尝试过多获取失败事件
   * @param attemptId 发生获取失败的Map任务尝试ID
   * @param reduceId 报告失败的Reduce任务尝试ID
   * @param reduceHost 报告失败的Reduce任务所在主机名
   */
  public TaskAttemptTooManyFetchFailureEvent(TaskAttemptId attemptId,
      TaskAttemptId reduceId, String reduceHost) {
      super(attemptId, TaskAttemptEventType.TA_TOO_MANY_FETCH_FAILURE);
    this.reduceID = reduceId;
    this.reduceHostname = reduceHost;
  }

  /**
   * 获取报告失败的Reduce任务尝试ID
   * @return 报告失败的Reduce任务尝试ID
   */
  public TaskAttemptId getReduceId() {
    return reduceID;
  }

  /**
   * 获取报告失败的Reduce任务所在主机名
   * @return 报告失败的Reduce任务主机名
   */
  public String getReduceHost() {
    return reduceHostname;
  }  
}