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

package org.apache.hadoop.mapreduce.v2.api.records;

import java.util.List;

/**
 * MapReduce任务运行报告接口，定义了单个任务运行状态和统计信息的存取方法
 * 用于在客户端和服务端之间传递任务执行情况，供监控、查询和作业状态跟踪使用
 */
public interface TaskReport {
  /**
   * 获取任务ID
   * @return 对应任务的唯一标识ID
   */
  public abstract TaskId getTaskId();

  /**
   * 获取任务当前状态
   * @return 任务的状态枚举值
   */
  public abstract TaskState getTaskState();

  /**
   * 获取任务执行进度
   * @return 任务进度，范围0~1
   */
  public abstract float getProgress();

  /**
   * 获取任务状态描述信息
   * @return 状态描述字符串
   */
  public abstract String getStatus();

  /**
   * 获取任务启动时间戳
   * @return 任务启动时间（毫秒）
   */
  public abstract long getStartTime();

  /**
   * 获取任务结束时间戳
   * @return 任务结束时间（毫秒），未完成时返回0
   */
  public abstract long getFinishTime();

  /**
   * 获取任务的V2版本计数器统计信息
   * @return 任务运行过程中的统计计数器集合
   */
  public abstract Counters getCounters();

  /**
   * 获取任务的原生V1版本计数器统计信息
   * @return 兼容旧API的计数器集合
   */
  public abstract org.apache.hadoop.mapreduce.Counters getRawCounters();

  /**
   * 获取当前正在运行的任务尝试列表
   * @return 正在运行的任务尝试ID列表
   */
  public abstract List<TaskAttemptId> getRunningAttemptsList();

  /**
   * 获取指定索引位置的正在运行的任务尝试ID
   * @param index 列表索引
   * @return 对应索引的任务尝试ID
   */
  public abstract TaskAttemptId getRunningAttempt(int index);

  /**
   * 获取正在运行的任务尝试数量
   * @return 正在运行的任务尝试总数
   */
  public abstract int getRunningAttemptsCount();
  
  /**
   * 获取执行成功的任务尝试ID
   * @return 成功完成的任务尝试ID，任务未成功时返回null
   */
  public abstract TaskAttemptId getSuccessfulAttempt();
  
  /**
   * 获取任务诊断信息列表
   * @return 诊断信息字符串列表
   */
  public abstract List<String> getDiagnosticsList();

  /**
   * 获取指定索引位置的诊断信息
   * @param index 列表索引
   * @return 对应索引的诊断信息字符串
   */
  public abstract String getDiagnostics(int index);

  /**
   * 获取诊断信息总数
   * @return 诊断信息条目数量
   */
  public abstract int getDiagnosticsCount();
  
  
  /**
   * 设置任务ID
   * @param taskId 任务唯一标识ID
   */
  public abstract void setTaskId(TaskId taskId);

  /**
   * 设置任务当前状态
   * @param taskState 任务状态枚举值
   */
  public abstract void setTaskState(TaskState taskState);

  /**
   * 设置任务执行进度
   * @param progress 任务进度，范围0~1
   */
  public abstract void setProgress(float progress);

  /**
   * 设置任务状态描述信息
   * @param status 状态描述字符串
   */
  public abstract void setStatus(String status);

  /**
   * 设置任务启动时间戳
   * @param startTime 任务启动时间（毫秒）
   */
  public abstract void setStartTime(long startTime);

  /**
   * 设置任务结束时间戳
   * @param finishTime 任务结束时间（毫秒）
   */
  public abstract void setFinishTime(long finishTime);

  /**
   * 设置任务的V2版本计数器统计信息
   * @param counters 统计计数器集合
   */
  public abstract void setCounters(Counters counters);

  /**
   * 设置任务的原生V1版本计数器统计信息
   * @param rCounters 兼容旧API的计数器集合
   */
  public abstract void
      setRawCounters(org.apache.hadoop.mapreduce.Counters rCounters);

  /**
   * 批量添加正在运行的任务尝试
   * @param taskAttempts 待添加的任务尝试ID列表
   */
  public abstract void addAllRunningAttempts(List<TaskAttemptId> taskAttempts);

  /**
   * 添加单个正在运行的任务尝试
   * @param taskAttempt 待添加的任务尝试ID
   */
  public abstract void addRunningAttempt(TaskAttemptId taskAttempt);

  /**
   * 移除指定索引位置的正在运行的任务尝试
   * @param index 待移除条目的索引
   */
  public abstract void removeRunningAttempt(int index);

  /**
   * 清空所有正在运行的任务尝试列表
   */
  public abstract void clearRunningAttempts();
  
  /**
   * 设置执行成功的任务尝试ID
   * @param taskAttempt 成功完成的任务尝试ID
   */
  public abstract void setSuccessfulAttempt(TaskAttemptId taskAttempt)
;
  /**
   * 批量添加诊断信息
   * @param diagnostics 待添加的诊断信息列表
   */
  public abstract void addAllDiagnostics(List<String> diagnostics);

  /**
   * 添加单条诊断信息
   * @param diagnostics 待添加的诊断信息字符串
   */
  public abstract void addDiagnostics(String diagnostics);

  /**
   * 移除指定索引位置的诊断信息
   * @param index 待移除条目的索引
   */
  public abstract void removeDiagnostics(int index);

  /**
   * 清空所有诊断信息
   */
  public abstract void clearDiagnostics();
}