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
 * 作业诊断信息更新事件，用于承载MapReduce作业运行过程中新增的诊断日志信息
 * 在作业运行异常或需要输出运行状态时触发，将诊断信息传递给事件处理系统
 */
public class JobDiagnosticsUpdateEvent extends JobEvent {

  private String diagnosticUpdate;

  /**
   * 构造作业诊断信息更新事件
   * @param jobID 目标作业ID
   * @param diagnostic 新增的诊断信息文本
   */
  public JobDiagnosticsUpdateEvent(JobId jobID, String diagnostic) {
    super(jobID, JobEventType.JOB_DIAGNOSTIC_UPDATE);
    this.diagnosticUpdate = diagnostic;
  }

  /**
   * 获取本次更新的新增诊断信息
   * @return 诊断信息文本
   */
  public String getDiagnosticUpdate() {
    return this.diagnosticUpdate;
  }
}