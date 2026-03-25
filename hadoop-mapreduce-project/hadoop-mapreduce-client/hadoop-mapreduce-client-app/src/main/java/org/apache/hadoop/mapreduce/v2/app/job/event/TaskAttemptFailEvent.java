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
 * 任务尝试失败事件，用于通知作业任务尝试执行失败，并携带是否快速失败的标记
 */
public class TaskAttemptFailEvent extends TaskAttemptEvent {
  // 是否快速失败，快速失败表示任务失败后不重试，直接标记任务失败
  private boolean fastFail;

  /**
   * 创建任务尝试失败事件，默认不开启快速失败
   * @param id 失败任务尝试的ID
   */
  public TaskAttemptFailEvent(TaskAttemptId id) {
    this(id, false);
  }

  /**
   * 创建任务尝试失败事件，可指定是否开启快速失败
   * @param id 失败任务尝试的ID
   * @param fastFail 是否开启快速失败，true表示失败后不重试
   */
  public TaskAttemptFailEvent(TaskAttemptId id, boolean fastFail) {
    super(id, TaskAttemptEventType.TA_FAILMSG);
    this.fastFail = fastFail;
  }

  /**
   * 获取是否开启快速失败标记
   * @return true表示任务失败后不重试，false表示允许重试
   */
  public boolean isFastFail() {
    return fastFail;
  }
}