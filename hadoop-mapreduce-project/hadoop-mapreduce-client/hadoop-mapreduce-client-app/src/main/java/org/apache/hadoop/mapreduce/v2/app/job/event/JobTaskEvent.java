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

import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskState;

/**
 * 作业任务状态变更事件，用于通知Job任务完成状态变更
 * 当MapReduce作业中的某个任务完成后，会生成该事件通知作业主逻辑处理
 */
public class JobTaskEvent extends JobEvent {

  private TaskId taskID;
  private TaskState taskState;

  /**
   * 构造任务状态变更事件
   * @param taskID 发生状态变更的任务ID
   * @param taskState 任务当前状态
   */
  public JobTaskEvent(TaskId taskID, TaskState taskState) {
    // 调用父类构造，关联任务所属作业ID，设置事件类型为任务完成
    super(taskID.getJobId(), JobEventType.JOB_TASK_COMPLETED);
    this.taskID = taskID;
    this.taskState = taskState;
  }

  /**
   * 获取发生状态变更的任务ID
   * @return 任务ID实例
   */
  public TaskId getTaskID() {
    return taskID;
  }

  /**
   * 获取任务当前状态
   * @return 任务状态枚举实例
   */
  public TaskState getState() {
    return taskState;
  }
}