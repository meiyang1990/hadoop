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
 * 获取任务尝试报告的请求接口，定义了MR ApplicationMaster与MR客户端之间获取任务尝试报告请求的结构。
 * 用于客户端向服务端请求指定任务尝试的运行状态报告。
 */
public interface GetTaskAttemptReportRequest {
  /**
   * 获取请求查询的任务尝试ID。
   * @return 要查询报告的任务尝试唯一标识
   */
  public abstract TaskAttemptId getTaskAttemptId();
  
  /**
   * 设置需要查询报告的任务尝试ID。
   * @param taskAttemptId 要查询报告的任务尝试唯一标识
   */
  public abstract void setTaskAttemptId(TaskAttemptId taskAttemptId);
}