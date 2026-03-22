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
 * 作业启动事件，用于通知作业开始执行，支持故障恢复场景恢复原作业启动时间
 */
public class JobStartEvent extends JobEvent {

  // 故障恢复场景中，原作业的启动时间
  long recoveredJobStartTime;

  /**
   * 构造非恢复场景的作业启动事件
   * @param jobID 作业ID
   */
  public JobStartEvent(JobId jobID) {
    this(jobID, -1L);
  }

  /**
   * 构造作业启动事件，支持指定恢复后的原作业启动时间
   * @param jobID 作业ID
   * @param recoveredJobStartTime 恢复场景中原作业的启动时间，非恢复场景传入-1
   */
  public JobStartEvent(JobId jobID, long recoveredJobStartTime) {
    super(jobID, JobEventType.JOB_START);
    this.recoveredJobStartTime = recoveredJobStartTime;
  }

  /**
   * 获取故障恢复场景中原作业的启动时间
   * @return 原作业启动时间，非恢复场景返回-1
   */
  public long getRecoveredJobStartTime() {
    return recoveredJobStartTime;
  }
}