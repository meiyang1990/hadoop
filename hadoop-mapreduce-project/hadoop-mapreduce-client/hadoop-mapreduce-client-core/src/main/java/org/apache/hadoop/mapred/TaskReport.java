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
package org.apache.hadoop.mapred;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/** 
 * MapReduce v1 API 任务运行状态报告类，封装单个任务的运行进度、状态、诊断信息等运行数据。
 * 继承自新API的org.apache.hadoop.mapreduce.TaskReport，提供向下兼容的旧API封装。
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class TaskReport extends org.apache.hadoop.mapreduce.TaskReport {
  
  /**
   * 构造空的任务状态报告对象
   */
  public TaskReport() {
    super();
  }
  
  /**
   * 构造包含完整任务状态信息的任务报告（已废弃）
   * @param taskid 任务ID
   * @param progress 任务执行进度
   * @param state 任务状态字符串
   * @param diagnostics 诊断信息数组
   * @param startTime 任务开始时间
   * @param finishTime 任务结束时间
   * @param counters 任务计数器
   * @deprecated
   */
  @Deprecated
  TaskReport(TaskID taskid, float progress, String state,
      String[] diagnostics, long startTime, long finishTime,
      Counters counters) {
    this(taskid, progress, state, diagnostics, null, startTime, finishTime, 
        counters);
  }
  
  /**
   * 构造包含完整任务状态信息的任务报告
   * @param taskid 任务ID
   * @param progress 任务执行进度
   * @param state 任务状态字符串
   * @param diagnostics 诊断信息数组
   * @param currentStatus 当前任务尝试状态
   * @param startTime 任务开始时间
   * @param finishTime 任务结束时间
   * @param counters 任务计数器
   */
  TaskReport(TaskID taskid, float progress, String state,
             String[] diagnostics, TIPStatus currentStatus, 
             long startTime, long finishTime,
             Counters counters) {
    super(taskid, progress, state, diagnostics, currentStatus, startTime,
      finishTime, new org.apache.hadoop.mapreduce.Counters(counters));
  }
  
  /**
   * 将新API的TaskReport对象降级转换为旧API的TaskReport对象，保持兼容
   * @param report 新API版本的任务状态报告
   * @return 旧API版本的任务状态报告
   */
  static TaskReport downgrade(
      org.apache.hadoop.mapreduce.TaskReport report) {
    return new TaskReport(TaskID.downgrade(report.getTaskID()),
      report.getProgress(), report.getState(), report.getDiagnostics(),
      report.getCurrentStatus(), report.getStartTime(), report.getFinishTime(),
      Counters.downgrade(report.getTaskCounters()));
  }
  
  /**
   * 将新API的TaskReport数组批量降级转换为旧API的TaskReport数组
   * @param reports 新API版本的任务状态报告数组
   * @return 旧API版本的任务状态报告数组
   */
  static TaskReport[] downgradeArray(org.apache.hadoop.
      mapreduce.TaskReport[] reports) {
    List<TaskReport> ret = new ArrayList<TaskReport>();
    // 遍历逐个转换每个报告对象
    for (org.apache.hadoop.mapreduce.TaskReport report : reports) {
      ret.add(downgrade(report));
    }
    return ret.toArray(new TaskReport[0]);
  }
  
  /**
   * 获取任务ID的字符串形式
   * @return 任务ID字符串
   */
  public String getTaskId() {
    return TaskID.downgrade(super.getTaskID()).toString();
  }

  /**
   * 获取旧API格式的任务ID对象
   * @return 旧API任务ID
   */
  public TaskID getTaskID() {
    return TaskID.downgrade(super.getTaskID());
  }

  /**
   * 获取旧API格式的任务计数器
   * @return 任务计数器
   */
  public Counters getCounters() { 
    return Counters.downgrade(super.getTaskCounters()); 
  }
  
  /** 
   * 设置任务成功运行的尝试ID
   * @param t 成功的尝试ID
   */ 
  public void setSuccessfulAttempt(TaskAttemptID t) {
    super.setSuccessfulAttemptId(t);
  }

  /**
   * 获取完成该任务的成功尝试ID
   * @return 成功尝试ID
   */
  public TaskAttemptID getSuccessfulTaskAttempt() {
    return TaskAttemptID.downgrade(super.getSuccessfulTaskAttemptId());
  }

  /** 
   * 设置当前正在运行的任务尝试ID集合
   * @param runningAttempts 正在运行的尝试ID集合
   */ 
  public void setRunningTaskAttempts(
      Collection<TaskAttemptID> runningAttempts) {
    // 将旧API尝试ID转换为新API格式
    Collection<org.apache.hadoop.mapreduce.TaskAttemptID> attempts = 
      new ArrayList<org.apache.hadoop.mapreduce.TaskAttemptID>();
    for (TaskAttemptID id : runningAttempts) {
      attempts.add(id);
    }
    super.setRunningTaskAttemptIds(attempts);
  }

  /**
   * 获取当前正在运行的任务尝试ID集合
   * @return 正在运行的尝试ID集合
   */
  public Collection<TaskAttemptID> getRunningTaskAttempts() {
    // 将新API尝试ID转换为旧API格式
    Collection<TaskAttemptID> attempts = new ArrayList<TaskAttemptID>();
    for (org.apache.hadoop.mapreduce.TaskAttemptID id : 
         super.getRunningTaskAttemptIds()) {
      attempts.add(TaskAttemptID.downgrade(id));
    }
    return attempts;
  }
  
  /** 
   * 设置任务结束时间
   * @param finishTime 任务结束时间
   */
  protected void setFinishTime(long finishTime) {
    super.setFinishTime(finishTime);
  }

  /** 
   * 设置任务开始时间
   * @param startTime 任务开始时间
   */ 
  protected void setStartTime(long startTime) {
    super.setStartTime(startTime);
  }

}