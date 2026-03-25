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

import org.apache.hadoop.mapreduce.v2.api.records.JobReport;

/**
 * 获取作业报告响应接口，定义了MapReduce客户端向服务端获取作业报告后返回结果的结构。
 * 属于MapReduce应用协议层的响应实体，封装了服务端返回的作业运行状态报告。
 */
public interface GetJobReportResponse {
  /**
   * 获取响应中携带的作业报告信息。
   * @return 作业报告实例，包含作业的运行状态、进度、统计信息等
   */
  public abstract JobReport getJobReport();
  
  /**
   * 设置响应中的作业报告信息，由服务端在构造响应时填充。
   * @param jobReport 需要返回给客户端的作业报告实例
   */
  public abstract void setJobReport(JobReport jobReport);
}