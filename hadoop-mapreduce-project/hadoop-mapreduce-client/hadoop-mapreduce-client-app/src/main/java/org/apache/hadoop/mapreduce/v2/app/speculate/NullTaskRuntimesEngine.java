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

package org.apache.hadoop.mapreduce.v2.app.speculate;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptStatusUpdateEvent.TaskAttemptStatus;


/**
 * 空任务运行时间估算引擎，不进行实际的运行时间计算，用于禁用推测执行功能的场景
 * 该类当前仅作为示例存在，未在实际流程中使用
 */
/*
 * This class is provided solely as an exemplae of the values that mean
 *  that nothing needs to be computed.  It's not currently used.
 */
public class NullTaskRuntimesEngine implements TaskRuntimeEstimator {
  
  /**
   * 注册新的任务尝试，空实现不做任何处理
   */
  @Override
  public void enrollAttempt(TaskAttemptStatus status, long timestamp) {
    // no code
  }

  /**
   * 获取任务尝试注册时间，返回最大值表示该任务无需推测
   */
  @Override
  public long attemptEnrolledTime(TaskAttemptId attemptID) {
    return Long.MAX_VALUE;
  }

  /**
   * 更新任务尝试状态，空实现不做任何处理
   */
  @Override
  public void updateAttempt(TaskAttemptStatus status, long timestamp) {
    // no code
  }

  /**
   * 根据配置和上下文初始化引擎，空实现不做任何处理
   */
  @Override
  public void contextualize(Configuration conf, AppContext context) {
    // no code
  }

  /**
   * 获取任务可被推测执行的时间阈值，返回最大值表示不会触发推测
   */
  @Override
  public long thresholdRuntime(TaskId id) {
    return Long.MAX_VALUE;
  }

  /**
   * 估算指定任务尝试的运行时间，返回-1表示无有效估算结果
   */
  @Override
  public long estimatedRuntime(TaskAttemptId id) {
    return -1L;
  }
  
  /**
   * 估算任务新启动尝试的运行时间，返回-1表示无有效估算结果
   */
  @Override
  public long estimatedNewAttemptRuntime(TaskId id) {
    return -1L;
  }

  /**
   * 获取运行时间估算的方差，返回-1表示无有效方差结果
   */
  @Override
  public long runtimeEstimateVariance(TaskAttemptId id) {
    return -1L;
  }

}