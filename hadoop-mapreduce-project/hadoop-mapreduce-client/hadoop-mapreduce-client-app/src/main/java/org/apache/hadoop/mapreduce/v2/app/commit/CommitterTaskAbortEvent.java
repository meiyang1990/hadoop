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

package org.apache.hadoop.mapreduce.v2.app.commit;

import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;

/**
 * 任务提交器的任务终止事件，封装任务尝试终止相关信息
 * 用于MapReduce输出提交器处理任务异常终止流程
 */
public class CommitterTaskAbortEvent extends CommitterEvent {

  private final TaskAttemptId attemptID;
  private final TaskAttemptContext attemptContext;

  /**
   * 构造任务终止事件对象
   * @param attemptID 任务尝试ID
   * @param attemptContext 任务尝试上下文
   */
  public CommitterTaskAbortEvent(TaskAttemptId attemptID,
      TaskAttemptContext attemptContext) {
    super(CommitterEventType.TASK_ABORT);
    this.attemptID = attemptID;
    this.attemptContext = attemptContext;
  }

  /**
   * 获取当前终止任务的尝试ID
   * @return 任务尝试ID
   */
  public TaskAttemptId getAttemptID() {
    return attemptID;
  }

  /**
   * 获取当前终止任务的上下文对象
   * @return 任务尝试上下文
   */
  public TaskAttemptContext getAttemptContext() {
    return attemptContext;
  }
}