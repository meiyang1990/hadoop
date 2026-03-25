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

import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;

/**
 * 获取任务尝试诊断信息请求接口
 * 定义了客户端向MR应用Master查询指定任务尝试诊断日志信息的请求结构
 */
public interface GetDiagnosticsRequest {
  /**
   * 获取请求对应的任务尝试ID
   * @return 需要查询诊断信息的任务尝试ID
   */
  public abstract TaskAttemptId getTaskAttemptId();
  
  /**
   * 设置需要查询诊断信息的任务尝试ID
   * @param taskAttemptId 需要查询的任务尝试ID
   */
  public abstract void setTaskAttemptId(TaskAttemptId taskAttemptId);
}