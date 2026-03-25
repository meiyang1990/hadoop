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
 * 作业初始化完成事件，标识MapReduce作业的初始化阶段已完成
 * 用于通知作业状态机从初始化阶段进入任务调度执行阶段
 */
public class JobSetupCompletedEvent extends JobEvent {

  /**
   * 构造作业初始化完成事件
   * @param jobID 完成初始化的作业ID
   */
  public JobSetupCompletedEvent(JobId jobID) {
    super(jobID, JobEventType.JOB_SETUP_COMPLETED);
  }
}