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

import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;

/**
 * 作业提交器的作业终止事件，封装作业终止时需要传递给提交器处理的相关信息。
 * 用于在MapReduce应用作业中止流程中，触发输出提交器执行作业终止清理操作。
 */
public class CommitterJobAbortEvent extends CommitterEvent {

  private JobId jobID;
  private JobContext jobContext;
  private JobStatus.State finalState;

  /**
   * 构造作业终止事件，封装终止相关信息。
   * @param jobID 终止作业的ID
   * @param jobContext 作业上下文对象，包含作业运行时配置和信息
   * @param finalState 作业终止后的最终状态
   */
  public CommitterJobAbortEvent(JobId jobID, JobContext jobContext,
      JobStatus.State finalState) {
    super(CommitterEventType.JOB_ABORT);
    this.jobID = jobID;
    this.jobContext = jobContext;
    this.finalState = finalState;
  }

  /**
   * 获取终止作业的ID。
   * @return 作业ID
   */
  public JobId getJobID() {
    return jobID;
  }

  /**
   * 获取终止作业的上下文对象。
   * @return 作业上下文
   */
  public JobContext getJobContext() {
    return jobContext;
  }

  /**
   * 获取作业终止后的最终状态。
   * @return 作业最终状态
   */
  public JobStatus.State getFinalState() {
    return finalState;
  }
}