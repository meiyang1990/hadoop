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

package org.apache.hadoop.mapreduce.v2.api.protocolrecords;

import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptReport;

/**
 * 获取任务尝试报告响应协议接口
 * 定义了MapReduce客户端向ApplicationMaster获取任务尝试运行报告后
 * 返回结果的结构规范，属于MR RPC协议层的数据模型
 */
public interface GetTaskAttemptReportResponse {
  /**
   * 获取查询到的任务尝试运行报告
   * @return 任务尝试运行报告，包含当前尝试的运行状态、进度、诊断信息等
   */
  public abstract TaskAttemptReport getTaskAttemptReport();
  
  /**
   * 设置任务尝试运行报告
   * @param taskAttemptReport 需要返回给客户端的任务尝试运行报告
   */
  public abstract void setTaskAttemptReport(TaskAttemptReport taskAttemptReport);
}