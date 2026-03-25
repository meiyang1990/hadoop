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

import org.apache.hadoop.mapreduce.v2.api.records.TaskReport;

/**
 * 获取任务报告响应接口，定义MapReduce应用master获取任务报告响应的结构
 * 用于MR客户端向MR ApplicationMaster请求单个任务运行报告后的响应封装
 */
public interface GetTaskReportResponse {
  /**
   * 获取请求返回的任务报告对象
   * @return 包含任务运行状态、进度、指标等信息的任务报告
   */
  public abstract TaskReport getTaskReport();
  
  /**
   * 设置响应中的任务报告对象
   * @param taskReport 要返回给请求端的任务报告
   */
  public abstract void setTaskReport(TaskReport taskReport);
}