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
 * 任务尝试诊断信息更新事件，用于在MapReduce应用中通知任务尝试有新的诊断日志信息更新
 * 继承自TaskAttemptEvent，携带任务尝试标识和新增的诊断信息
 */
public class TaskAttemptDiagnosticsUpdateEvent extends TaskAttemptEvent {

  // 新增的诊断日志信息
  private String diagnosticInfo;

  /**
   * 构造任务尝试诊断信息更新事件
   * @param attemptID 目标任务尝试的ID
   * @param diagnosticInfo 需要更新的新增诊断信息
   */
  public TaskAttemptDiagnosticsUpdateEvent(TaskAttemptId attemptID,
      String diagnosticInfo) {
    super(attemptID, TaskAttemptEventType.TA_DIAGNOSTICS_UPDATE);
    this.diagnosticInfo = diagnosticInfo;
  }

  /**
   * 获取本次更新的新增诊断信息
   * @return 诊断信息字符串
   */
  public String getDiagnosticInfo() {
    return diagnosticInfo;
  }
}