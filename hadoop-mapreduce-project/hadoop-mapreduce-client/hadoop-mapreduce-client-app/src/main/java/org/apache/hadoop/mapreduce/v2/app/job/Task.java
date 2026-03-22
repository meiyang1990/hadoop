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

package org.apache.hadoop.mapreduce.v2.app.job;

import java.util.Map;

import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskReport;
import org.apache.hadoop.mapreduce.v2.api.records.TaskState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;

/**
 * MapReduce任务的只读视图接口，定义了任务对外暴露的查询能力。
 * 用于在ApplicationMaster中提供任务状态信息，不支持修改任务内部状态。
 */
public interface Task {
  /**
   * 获取当前任务的唯一标识ID
   * @return 任务ID对象
   */
  TaskId getID();

  /**
   * 获取当前任务的运行报告，包含任务状态、启动时间、完成时间等信息
   * @return 任务报告对象
   */
  TaskReport getReport();

  /**
   * 获取当前任务的状态
   * @return 任务状态枚举
   */
  TaskState getState();

  /**
   * 获取当前任务的所有计数器统计信息
   * @return 任务计数器集合
   */
  Counters getCounters();

  /**
   * 获取当前任务的执行进度
   * @return 进度值，范围[0.0, 1.0]
   */
  float getProgress();

  /**
   * 获取当前任务的类型（Map任务或Reduce任务）
   * @return 任务类型枚举
   */
  TaskType getType();

  /**
   * 获取当前任务的所有尝试实例
   * @return 任务尝试ID到尝试实例的映射
   */
  Map<TaskAttemptId, TaskAttempt> getAttempts();

  /**
   * 根据尝试ID获取对应的任务尝试实例
   * @param attemptID 任务尝试ID
   * @return 对应任务尝试实例
   */
  TaskAttempt getAttempt(TaskAttemptId attemptID);

  /** 
   * 判断任务是否已经进入最终状态（成功/失败/杀死）
   * @return true表示任务已结束，false表示任务仍在运行
   */
  boolean isFinished();

  /**
   * 判断指定任务尝试是否可以提交输出结果。
   * 当一个尝试通过该检查后，后续其他尝试的请求都会返回false，保证仅一个尝试提交结果。
   * @param taskAttemptID 请求提交的任务尝试ID
   * @return true表示该尝试可以提交输出，false表示不允许提交
   */
  boolean canCommit(TaskAttemptId taskAttemptID);

  
}