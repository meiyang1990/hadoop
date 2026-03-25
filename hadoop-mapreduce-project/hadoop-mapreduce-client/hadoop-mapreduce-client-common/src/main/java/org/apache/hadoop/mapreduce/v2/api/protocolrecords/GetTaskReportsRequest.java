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
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;

/**
 * 获取任务报告请求接口，定义客户端向ApplicationMaster请求指定作业指定类型任务报告的请求参数结构
 * 用于MapReduce客户端获取作业中特定类型任务的运行报告信息
 */
public interface GetTaskReportsRequest {
  
  /**
   * 获取请求对应的作业ID
   * @return 目标作业的唯一标识ID
   */
  public abstract JobId getJobId();

  /**
   * 获取请求的任务类型
   * @return 需要获取报告的任务类型（MAP/REDUCE）
   */
  public abstract TaskType getTaskType();
  
  /**
   * 设置请求对应的作业ID
   * @param jobId 目标作业的唯一标识ID
   */
  public abstract void setJobId(JobId jobId);

  /**
   * 设置需要获取报告的任务类型
   * @param taskType 需要获取报告的任务类型（MAP/REDUCE）
   */
  public abstract void setTaskType(TaskType taskType);
}