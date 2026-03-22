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

import org.apache.hadoop.mapreduce.v2.api.records.Counters;

/**
 * 获取作业/任务计数器响应接口
 * 定义了MapReduce任务计数器查询响应的数据结构，用于客户端从ApplicationMaster获取任务运行统计信息
 */
public interface GetCountersResponse {
  /**
   * 获取响应中的计数器对象
   * @return 包含任务/作业所有统计指标的计数器对象
   */
  public abstract Counters getCounters();
  
  /**
   * 设置响应中的计数器对象
   * @param counters 包含任务/作业所有统计指标的计数器对象
   */
  public abstract void setCounters(Counters counters);
}