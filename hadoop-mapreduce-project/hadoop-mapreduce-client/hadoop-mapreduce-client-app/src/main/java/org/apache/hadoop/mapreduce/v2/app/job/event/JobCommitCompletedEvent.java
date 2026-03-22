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
 * 作业提交完成事件，用于通知作业提交阶段已完成
 * 是MapReduce应用Master处理作业生命周期的核心事件之一
 */
public class JobCommitCompletedEvent extends JobEvent {

  /**
   * 构造作业提交完成事件
   * @param jobID 完成提交阶段的作业ID
   */
  public JobCommitCompletedEvent(JobId jobID) {
    super(jobID, JobEventType.JOB_COMMIT_COMPLETED);
  }
}