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

package org.apache.hadoop.mapreduce.v2.app.job.event;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.v2.api.records.Phase;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptState;

/**
 * 任务尝试状态更新事件，用于将任务尝试的最新状态从执行线程传递给AppMaster的事件处理线程
 * 承载了任务尝试的最新运行状态信息，支持原子更新获取
 */
public class TaskAttemptStatusUpdateEvent extends TaskAttemptEvent {
  // 原子引用，持有任务尝试的最新状态对象，支持并发安全读取更新
  private AtomicReference<TaskAttemptStatus> taskAttemptStatusRef;

  /**
   * 构造任务尝试状态更新事件
   * @param id 目标任务尝试的ID
   * @param taskAttemptStatusRef 包含最新状态的原子引用
   */
  public TaskAttemptStatusUpdateEvent(TaskAttemptId id,
      AtomicReference<TaskAttemptStatus> taskAttemptStatusRef) {
    super(id, TaskAttemptEventType.TA_UPDATE);
    this.taskAttemptStatusRef = taskAttemptStatusRef;
  }

  /**
   * 获取任务尝试状态的原子引用
   * @return 包含最新状态的原子引用
   */
  public AtomicReference<TaskAttemptStatus> getTaskAttemptStatusRef() {
    return taskAttemptStatusRef;
  }

  /**
   * The internal TaskAttemptStatus object corresponding to remote Task status.
   * 
   * 内部任务尝试状态结构体，存储任务尝试运行过程中的所有状态信息
   */
  public static class TaskAttemptStatus {
    // 任务尝试ID
    public TaskAttemptId id;
    // 任务执行进度(0.0~1.0)
    public float progress;
    // 任务当前计数器，统计各项运行指标
    public Counters counters;
    // 状态描述字符串
    public String stateString;
    // 当前所处计算阶段(Map/Shuffle/Sort/Reduce等)
    public Phase phase;
    // 获取失败的Map任务尝试ID列表，用于Shuffle阶段错误处理
    public List<TaskAttemptId> fetchFailedMaps;
    // Map阶段完成时间戳
    public long mapFinishTime;
    // Shuffle阶段完成时间戳
    public long shuffleFinishTime;
    // Sort阶段完成时间戳
    public long sortFinishTime;
    // 任务尝试当前状态
    public TaskAttemptState taskState;
  }
}