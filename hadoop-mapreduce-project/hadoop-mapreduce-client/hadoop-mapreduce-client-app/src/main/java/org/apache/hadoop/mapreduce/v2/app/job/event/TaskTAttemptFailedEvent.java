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
 * 任务尝试失败事件，封装任务尝试执行失败的相关信息
 * 用于MapReduce ApplicationMaster内部状态机处理任务尝试失败场景
 */
public class TaskTAttemptFailedEvent extends TaskTAttemptEvent {

  // 是否快速失败，标记是否需要立即终止整个作业
  private boolean fastFail;

  /**
   * 构造任务尝试失败事件，默认不开启快速失败
   * @param id 失败的任务尝试ID
   */
  public TaskTAttemptFailedEvent(TaskAttemptId id) {
    this(id, false);
  }

  /**
   * 构造任务尝试失败事件，可指定是否快速失败
   * @param id 失败的任务尝试ID
   * @param fastFail 是否快速失败，true表示需要立即终止整个作业
   */
  public TaskTAttemptFailedEvent(TaskAttemptId id, boolean fastFail) {
    super(id, TaskEventType.T_ATTEMPT_FAILED);
    this.fastFail = fastFail;
  }

  /**
   * 获取是否快速失败标识
   * @return true表示需要快速失败，立即终止作业；false表示可重新尝试执行任务
   */
  public boolean isFastFail() {
    return fastFail;
  }
}