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
 * Task尝试杀死事件，用于通知任务尝试组件需要终止指定任务尝试
 * 携带杀死原因和是否需要重新调度尝试的标识，是MapReduce应用内部事件驱动模型的核心事件之一
 */
public class TaskAttemptKillEvent extends TaskAttemptEvent {

  private final String message;
  // Next map attempt will be rescheduled(i.e. updated in ask with higher
  // priority equivalent to that of a fast fail map)
  // 是否需要重新调度该任务的新尝试
  private final boolean rescheduleAttempt;

  /**
   * 构造任务尝试杀死事件
   * @param attemptID 目标任务尝试ID
   * @param message 杀死原因描述信息
   * @param rescheduleAttempt 是否需要重新调度新的任务尝试
   */
  public TaskAttemptKillEvent(TaskAttemptId attemptID,
      String message, boolean rescheduleAttempt) {
    super(attemptID, TaskAttemptEventType.TA_KILL);
    this.message = message;
    this.rescheduleAttempt = rescheduleAttempt;
  }

  /**
   * 构造不需要重新调度的任务尝试杀死事件
   * @param attemptID 目标任务尝试ID
   * @param message 杀死原因描述信息
   */
  public TaskAttemptKillEvent(TaskAttemptId attemptID,
      String message) {
    this(attemptID, message, false);
  }

  /**
   * 获取杀死原因描述信息
   * @return 杀死原因文本
   */
  public String getMessage() {
    return message;
  }

  /**
   * 获取是否需要重新调度新的任务尝试标识
   * @return true表示需要重新调度，false表示不需要
   */
  public boolean getRescheduleAttempt() {
    return rescheduleAttempt;
  }
}