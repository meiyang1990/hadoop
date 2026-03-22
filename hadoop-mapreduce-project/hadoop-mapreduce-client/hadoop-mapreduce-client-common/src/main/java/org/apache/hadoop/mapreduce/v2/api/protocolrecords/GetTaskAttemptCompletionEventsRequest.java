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
 * 获取任务尝试完成事件请求接口
 * 用于MapReduce客户端向ApplicationMaster请求分页获取已完成任务尝试的事件信息
 * 用于作业执行进度监控和任务完成状态追踪
 */
public interface GetTaskAttemptCompletionEventsRequest {
  /**
   * 获取请求对应的作业ID
   * @return 目标作业ID
   */
  public abstract JobId getJobId();
  
  /**
   * 获取起始事件ID偏移量
   * @return 分页查询的起始事件ID
   */
  public abstract int getFromEventId();
  
  /**
   * 获取本次请求最多返回的事件数量
   * @return 最大返回事件数
   */
  public abstract int getMaxEvents();
  
  /**
   * 设置请求对应的作业ID
   * @param jobId 目标作业ID
   */
  public abstract void setJobId(JobId jobId);
  
  /**
   * 设置分页查询的起始事件ID
   * @param id 起始事件ID
   */
  public abstract void setFromEventId(int id);
  
  /**
   * 设置本次请求最多返回的事件数量
   * @param maxEvents 最大返回事件数
   */
  public abstract void setMaxEvents(int maxEvents);
}