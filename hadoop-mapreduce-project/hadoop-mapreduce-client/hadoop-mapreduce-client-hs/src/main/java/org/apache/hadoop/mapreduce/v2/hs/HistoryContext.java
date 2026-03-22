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

import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.hs.webapp.dao.JobsInfo;

/**
 * 历史作业上下文接口，继承自应用上下文，定义了MapReduce历史服务器查询已完成作业信息的核心方法。
 * 为历史作业数据查询、分页过滤提供统一抽象接口，供不同历史存储实现接入。
 */
public interface HistoryContext extends AppContext {

  /**
   * 获取指定应用下所有已完成的作业列表。
   * @param appID 应用ID，对应YARN应用标识
   * @return 该应用下所有作业ID与作业对象的映射集合
   */
  Map<JobId, Job> getAllJobs(ApplicationId appID);

  /**
   * 根据多条件分页查询历史作业列表，用于Web UI展示作业信息。
   * @param offset 分页起始偏移量
   * @param count 单页返回作业数量
   * @param user 提交作业的用户名过滤条件
   * @param queue 作业运行队列过滤条件
   * @param sBegin 作业提交时间起始过滤条件
   * @param sEnd 作业提交时间结束过滤条件
   * @param fBegin 作业完成时间起始过滤条件
   * @param fEnd 作业完成时间结束过滤条件
   * @param jobState 作业状态过滤条件
   * @return 封装了符合条件的作业信息集合，用于Web DAO层返回
   */
  JobsInfo getPartialJobs(Long offset, Long count, String user,
      String queue, Long sBegin, Long sEnd, Long fBegin, Long fEnd, JobState jobState);
}