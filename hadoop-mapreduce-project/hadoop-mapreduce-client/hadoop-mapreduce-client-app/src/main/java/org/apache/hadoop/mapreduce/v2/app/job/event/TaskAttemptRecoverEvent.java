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
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.TaskAttemptInfo;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;

/**
 * 任务尝试恢复事件类，承载从作业历史恢复已完成任务尝试所需的所有信息
 * 用于作业容错恢复场景，从历史日志中恢复未完成作业的任务尝试状态
 */
public class TaskAttemptRecoverEvent extends TaskAttemptEvent {

  private TaskAttemptInfo taInfo;
  private OutputCommitter committer;
  private boolean recoverAttemptOutput;

  /**
   * 构造任务尝试恢复事件
   * @param id 任务尝试ID
   * @param taInfo 从作业历史解析得到的任务尝试信息
   * @param committer 输出提交器，用于恢复输出状态
   * @param recoverOutput 是否需要恢复任务尝试的输出
   */
  public TaskAttemptRecoverEvent(TaskAttemptId id, TaskAttemptInfo taInfo,
      OutputCommitter committer, boolean recoverOutput) {
    super(id, TaskAttemptEventType.TA_RECOVER);
    this.taInfo = taInfo;
    this.committer = committer;
    this.recoverAttemptOutput = recoverOutput;
  }

  /**
   * 获取从作业历史解析得到的任务尝试信息
   * @return 任务尝试历史信息对象
   */
  public TaskAttemptInfo getTaskAttemptInfo() {
    return taInfo;
  }

  /**
   * 获取任务尝试对应的输出提交器
   * @return 输出提交器实例
   */
  public OutputCommitter getCommitter() {
    return committer;
  }

  /**
   * 获取是否需要恢复任务尝试输出的标志
   * @return true表示需要恢复输出，false表示不需要
   */
  public boolean getRecoverOutput() {
    return recoverAttemptOutput;
  }
}