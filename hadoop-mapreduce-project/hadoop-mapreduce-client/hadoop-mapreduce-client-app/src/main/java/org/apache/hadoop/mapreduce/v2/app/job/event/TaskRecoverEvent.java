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

import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.TaskInfo;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;

/**
 * 任务恢复事件，用于应用重启后从作业历史中恢复已存在任务的状态信息
 * 继承自TaskEvent，在MR AppMaster重启时触发任务恢复流程
 */
public class TaskRecoverEvent extends TaskEvent {

  // 从作业历史中解析出的任务信息，包含任务执行历史数据
  private TaskInfo taskInfo;
  // 任务输出提交器，用于恢复任务输出的提交状态
  private OutputCommitter committer;
  // 标记是否需要恢复任务的输出数据
  private boolean recoverTaskOutput;

  /**
   * 构造任务恢复事件
   * @param taskID 要恢复的任务ID
   * @param taskInfo 作业历史中解析出的任务信息
   * @param committer 任务输出提交器
   * @param recoverTaskOutput 是否需要恢复任务输出
   */
  public TaskRecoverEvent(TaskId taskID, TaskInfo taskInfo,
      OutputCommitter committer, boolean recoverTaskOutput) {
    super(taskID, TaskEventType.T_RECOVER);
    this.taskInfo = taskInfo;
    this.committer = committer;
    this.recoverTaskOutput = recoverTaskOutput;
  }

  /**
   * 获取从作业历史中解析的任务信息
   * @return 任务历史信息对象
   */
  public TaskInfo getTaskInfo() {
    return taskInfo;
  }

  /**
   * 获取任务输出提交器
   * @return 输出提交器实例
   */
  public OutputCommitter getOutputCommitter() {
    return committer;
  }

  /**
   * 获取是否需要恢复任务输出的标记
   * @return true表示需要恢复，false表示不需要
   */
  public boolean getRecoverTaskOutput() {
    return recoverTaskOutput;
  }
}