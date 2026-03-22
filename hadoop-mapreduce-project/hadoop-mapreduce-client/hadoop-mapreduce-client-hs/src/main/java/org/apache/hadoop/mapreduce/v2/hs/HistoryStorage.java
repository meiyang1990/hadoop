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

package org.apache.hadoop.mapreduce.v2.hs;

import java.util.Map;

import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.hs.webapp.dao.JobsInfo;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * 文件说明：MapReduce历史服务器的作业历史存储服务接口，定义查询已完成MapReduce作业的标准API
 * 
 * 实现注意事项：当HDFS上的历史文件被删除时，本接口不会收到回调通知。
 * 如果实现方没有完整备份HDFS上存储的历史数据，可以依赖HistoryFileManager来感知文件删除事件。
 * 核心职责：为历史作业查询提供统一抽象，支持不同实现的存储后端接入历史服务器。
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public interface HistoryStorage {
  
  /**
   * 注入历史文件管理器，用于和HDFS上的历史文件交互
   * @param hsManager 历史文件管理器实例，用于操作历史文件
   */
  void setHistoryFileManager(HistoryFileManager hsManager);
  
  /**
   * 根据过滤条件分页查询部分作业信息（仅返回摘要信息）
   * @param offset 分页起始偏移量
   * @param count 单次返回最大作业数量
   * @param user 按用户名过滤，为null不过滤
   * @param queue 按队列名过滤，为null不过滤
   * @param sBegin 只返回开始时间大于等于该值的作业，为null不过滤
   * @param sEnd 只返回开始时间小于等于该值的作业，为null不过滤
   * @param fBegin 只返回结束时间大于等于该值的作业，为null不过滤
   * @param fEnd 只返回结束时间小于等于该值的作业，为null不过滤
   * @param jobState 只返回该状态的作业，为null不过滤
   * @return 过滤后的作业摘要列表
   */
  JobsInfo getPartialJobs(Long offset, Long count, String user, 
      String queue, Long sBegin, Long sEnd, Long fBegin, Long fEnd, 
      JobState jobState);
  
  /**
   * 获取所有缓存的作业摘要信息，仅为兼容旧版保留接口
   * @return 所有缓存作业Id与作业摘要的映射表
   */
  Map<JobId, Job> getAllPartialJobs();
  
  /**
   * 获取完整解析后的作业全量信息
   * @param jobId 目标作业Id
   * @return 全量作业信息，找不到返回null
   */
  Job getFullJob(JobId jobId);
}