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

import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;

/**
 * 作业终止完成事件，用于通知作业中止操作已完成并携带作业最终状态
 * 是MapReduce应用 Master 事件驱动模型中处理作业终止流程的核心事件
 */
public class JobAbortCompletedEvent extends JobEvent {

  private JobStatus.State finalState;

  /**
   * 构造作业终止完成事件
   * @param jobID 目标作业ID
   * @param finalState 作业中止后的最终状态
   */
  public JobAbortCompletedEvent(JobId jobID, JobStatus.State finalState) {
    super(jobID, JobEventType.JOB_ABORT_COMPLETED);
    this.finalState = finalState;
  }

  /**
   * 获取作业中止后的最终状态
   * @return 作业最终状态
   */
  public JobStatus.State getFinalState() {
    return finalState;
  }
}