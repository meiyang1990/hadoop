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

import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptCompletionEvent;

/**
 * 任务尝试完成事件，用于通知Job任务存在某个任务尝试已执行完成
 * 承载任务尝试完成的相关信息，供Job状态机处理后续逻辑
 */
public class JobTaskAttemptCompletedEvent extends JobEvent {

  private TaskAttemptCompletionEvent completionEvent;

  /**
   * 构造任务尝试完成事件对象
   * @param completionEvent 原始任务尝试完成事件记录
   */
  public JobTaskAttemptCompletedEvent(TaskAttemptCompletionEvent completionEvent) {
    super(completionEvent.getAttemptId().getTaskId().getJobId(), 
        JobEventType.JOB_TASK_ATTEMPT_COMPLETED);
    this.completionEvent = completionEvent;
  }

  /**
   * 获取原始任务尝试完成事件信息
   * @return 任务尝试完成事件记录
   */
  public TaskAttemptCompletionEvent getCompletionEvent() {
    return completionEvent;
  }
}