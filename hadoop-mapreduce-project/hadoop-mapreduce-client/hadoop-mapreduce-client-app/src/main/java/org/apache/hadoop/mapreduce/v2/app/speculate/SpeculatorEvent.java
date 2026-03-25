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

import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptStatusUpdateEvent.TaskAttemptStatus;
import org.apache.hadoop.yarn.event.AbstractEvent;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;

/**
 * 推测执行事件，封装MapReduce推测执行过程中产生的各类事件，传递给推测执行器处理
 * 继承YARN AbstractEvent，支持事件类型和时间戳管理，承载不同事件对应的业务数据
 */
public class SpeculatorEvent extends AbstractEvent<Speculator.EventType> {

  // 仅对ATTEMPT_STATUS_UPDATE事件有效：任务尝试更新后的状态报告
  private TaskAttemptStatus reportedStatus;

  // 仅对TASK_CONTAINER_NEED_UPDATE事件有效：需要容器的任务ID
  private TaskId taskID;
  // 仅对TASK_CONTAINER_NEED_UPDATE事件有效：容器需求数量变化（+1表示新增需求，-1表示取消需求）
  private int containersNeededChange;
  
  // 仅对CREATE_JOB事件有效：当前作业ID
  private JobId jobID;

  /**
   * 构造作业创建事件，通知推测执行器初始化对应作业的推测执行逻辑
   * @param jobID 作业ID
   * @param timestamp 事件时间戳
   */
  public SpeculatorEvent(JobId jobID, long timestamp) {
    super(Speculator.EventType.JOB_CREATE, timestamp);
    this.jobID = jobID;
  }

  /**
   * 构造任务尝试状态更新事件，通知推测执行器任务尝试的最新运行状态
   * @param reportedStatus 任务尝试更新后的状态
   * @param timestamp 事件时间戳
   */
  public SpeculatorEvent(TaskAttemptStatus reportedStatus, long timestamp) {
    super(Speculator.EventType.ATTEMPT_STATUS_UPDATE, timestamp);
    this.reportedStatus = reportedStatus;
  }

  /**
   * 构造任务尝试启动事件，通知推测执行器有新的任务尝试开始运行
   * @param attemptID 新启动的任务尝试ID
   * @param flag 标志位（未使用）
   * @param timestamp 事件时间戳
   */
  public SpeculatorEvent(TaskAttemptId attemptID, boolean flag, long timestamp) {
    super(Speculator.EventType.ATTEMPT_START, timestamp);
    this.reportedStatus = new TaskAttemptStatus();
    this.reportedStatus.id = attemptID;
    this.taskID = attemptID.getTaskId();
  }

  /*
   * This c'tor creates a TASK_CONTAINER_NEED_UPDATE event .
   * We send a +1 event when a task enters a state where it wants a container,
   *  and a -1 event when it either gets one or withdraws the request.
   * The per job sum of all these events is the number of containers requested
   *  but not granted.  The intent is that we only do speculations when the
   *  speculation wouldn't compete for containers with tasks which need
   *  to be run.
   */
  /**
   * 构造任务容器需求更新事件，通知推测执行器任务对容器需求的数量变化
   * 用于统计当前作业未分配的容器需求，确保推测执行不会抢占必须运行任务的容器资源
   * @param taskID 需求变化对应的任务ID
   * @param containersNeededChange 需求变化量：+1表示新增容器需求，-1表示取消容器需求
   */
  public SpeculatorEvent(TaskId taskID, int containersNeededChange) {
    super(Speculator.EventType.TASK_CONTAINER_NEED_UPDATE);
    this.taskID = taskID;
    this.containersNeededChange = containersNeededChange;
  }

  /**
   * 获取任务尝试状态报告，仅对状态更新和启动事件有效
   * @return 任务尝试状态对象
   */
  public TaskAttemptStatus getReportedStatus() {
    return reportedStatus;
  }

  /**
   * 获取容器需求变化量，仅对容器需求更新事件有效
   * @return 容器需求变化量（+1/-1）
   */
  public int containersNeededChange() {
    return containersNeededChange;
  }

  /**
   * 获取关联任务ID，对容器需求更新和尝试启动事件有效
   * @return 任务ID
   */
  public TaskId getTaskID() {
    return taskID;
  }
  
  /**
   * 获取关联作业ID，仅对作业创建事件有效
   * @return 作业ID
   */
  public JobId getJobID() {
    return jobID;
  }
}