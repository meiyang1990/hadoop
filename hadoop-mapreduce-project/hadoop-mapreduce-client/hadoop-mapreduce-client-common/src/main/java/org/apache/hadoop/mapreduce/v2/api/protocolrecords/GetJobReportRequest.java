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

import org.apache.hadoop.mapreduce.v2.api.records.JobId;

/**
 * 获取作业报告请求接口，定义了客户端向MR ApplicationMaster查询作业报告所需参数的存取方法
 * 用于MapReduce客户端与服务端之间RPC通信的请求参数封装
 */
public interface GetJobReportRequest {
  /**
   * 获取请求查询的作业ID
   * @return 待查询作业的JobId对象
   */
  public abstract JobId getJobId();
  
  /**
   * 设置需要查询报告的作业ID
   * @param jobId 待查询作业的JobId对象
   */
  public abstract void setJobId(JobId jobId);
}