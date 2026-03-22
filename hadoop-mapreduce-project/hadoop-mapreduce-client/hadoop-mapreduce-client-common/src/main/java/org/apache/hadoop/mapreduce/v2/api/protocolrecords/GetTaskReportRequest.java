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

import org.apache.hadoop.mapreduce.v2.api.records.TaskId;

/**
 * 获取任务报告请求协议接口
 * 用于MapReduce客户端向ApplicationMaster请求获取指定任务的运行报告
 */
public interface GetTaskReportRequest {
  /**
   * 获取请求查询的任务ID
   * @return 目标任务的TaskId对象
   */
  public abstract TaskId getTaskId();
  
  /**
   * 设置请求查询的任务ID
   * @param taskId 需要获取报告的目标任务ID
   */
  public abstract void setTaskId(TaskId taskId); 
}