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
 * 任务尝试被杀死事件，承载任务尝试被杀后的事件信息与调度标记
 */
public class TaskTAttemptKilledEvent extends TaskTAttemptEvent {

  // 标记是否需要重新调度下一个任务尝试（快速失败的Map任务会提升优先级重新调度）
  private final boolean rescheduleAttempt;

  /**
   * 构造任务尝试被杀死事件
   * @param id 被杀死的任务尝试ID
   * @param rescheduleAttempt 是否需要重新调度新的任务尝试
   */
  public TaskTAttemptKilledEvent(TaskAttemptId id, boolean rescheduleAttempt) {
    super(id, TaskEventType.T_ATTEMPT_KILLED);
    this.rescheduleAttempt = rescheduleAttempt;
  }

  /**
   * 获取是否需要重新调度任务尝试的标记
   * @return true表示需要重新调度，false表示不需要
   */
  public boolean getRescheduleAttempt() {
    return rescheduleAttempt;
  }
}