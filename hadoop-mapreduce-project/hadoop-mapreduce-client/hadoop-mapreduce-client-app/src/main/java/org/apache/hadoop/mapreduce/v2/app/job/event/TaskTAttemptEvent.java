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
 * 任务尝试相关事件，封装任务尝试ID信息，用于MapReduce应用中任务尝试维度的事件通知
 * 继承TaskEvent，在任务事件基础上增加了任务尝试标识
 */
public class TaskTAttemptEvent extends TaskEvent {

  // 当前事件关联的任务尝试ID
  private TaskAttemptId attemptID;

  /**
   * 构造任务尝试事件对象
   * @param id 任务尝试ID
   * @param type 事件类型
   */
  public TaskTAttemptEvent(TaskAttemptId id, TaskEventType type) {
    super(id.getTaskId(), type);
    this.attemptID = id;
  }

  /**
   * 获取当前事件关联的任务尝试ID
   * @return 任务尝试ID对象
   */
  public TaskAttemptId getTaskAttemptID() {
    return attemptID;
  }

}