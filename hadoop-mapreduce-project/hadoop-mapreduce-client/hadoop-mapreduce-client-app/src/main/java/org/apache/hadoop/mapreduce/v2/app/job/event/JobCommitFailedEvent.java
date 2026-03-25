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

import org.apache.hadoop.mapreduce.v2.api.records.JobId;

/**
 * 作业提交失败事件，当MapReduce作业提交阶段发生错误时触发
 * 封装了作业ID和失败原因信息，用于驱动作业状态机处理失败逻辑
 */
public class JobCommitFailedEvent extends JobEvent {
  // 作业提交失败的错误信息
  private String message;

  /**
   * 构造作业提交失败事件
   * @param jobID 失败作业的ID
   * @param message 失败原因描述信息
   */
  public JobCommitFailedEvent(JobId jobID, String message) {
    super(jobID, JobEventType.JOB_COMMIT_FAILED);
    this.message = message;
  }

  /**
   * 获取作业提交失败的错误信息
   * @return 失败原因描述
   */
  public String getMessage() {
    return this.message;
  }
}