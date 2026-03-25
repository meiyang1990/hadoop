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

package org.apache.hadoop.mapreduce.jobhistory;

import java.util.Set;

import org.apache.avro.util.Utf8;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.mapred.TaskStatus;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.util.JobHistoryEventUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric;
import org.apache.hadoop.yarn.util.SystemClock;

/**
 * 任务失败事件，用于记录MapReduce任务执行失败的历史信息，用于作业历史审计和日志
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class TaskFailedEvent implements HistoryEvent {
  private TaskFailed datum = null;

  private TaskAttemptID failedDueToAttempt;
  private TaskID id;
  private TaskType taskType;
  private long finishTime;
  private String status;
  private String error;
  private Counters counters;
  private long startTime;

  private static final Counters EMPTY_COUNTERS = new Counters();

  /**
   * 构造任务失败事件，记录任务失败的完整信息
   * @param id 失败任务的ID
   * @param finishTime 任务完成时间
   * @param taskType 任务类型（Map/Reduce）
   * @param error 错误信息描述
   * @param status 任务状态
   * @param failedDueToAttempt 导致任务失败的尝试ID
   * @param counters 任务的计量统计信息
   * @param startTs 任务开始时间
   */
  public TaskFailedEvent(TaskID id, long finishTime, 
      TaskType taskType, String error, String status,
      TaskAttemptID failedDueToAttempt, Counters counters, long startTs) {
    this.id = id;
    this.finishTime = finishTime;
    this.taskType = taskType;
    this.error = error;
    this.status = status;
    this.failedDueToAttempt = failedDueToAttempt;
    this.counters = counters;
    this.startTime = startTs;
  }

  /**
   * 构造任务失败事件，自动获取当前系统时间作为任务开始时间
   */
  public TaskFailedEvent(TaskID id, long finishTime, TaskType taskType,
      String error, String status, TaskAttemptID failedDueToAttempt,
      Counters counters) {
    this(id, finishTime, taskType, error, status, failedDueToAttempt, counters,
        SystemClock.getInstance().getTime());
  }

  /**
   * 构造任务失败事件，使用空计数器对象
   */
  public TaskFailedEvent(TaskID id, long finishTime, 
      TaskType taskType, String error, String status,
      TaskAttemptID failedDueToAttempt) {
    this(id, finishTime, taskType, error, status, failedDueToAttempt,
        EMPTY_COUNTERS);
  }

  TaskFailedEvent() {}

  /**
   * 获取Avro序列化后的事件数据对象，延迟初始化构建Avro结构
   * @return 序列化后的Avro数据对象
   */
  public Object getDatum() {
    if(datum == null) {
      datum = new TaskFailed();
      // 设置任务ID字符串
      datum.setTaskid(new Utf8(id.toString()));
      // 设置错误信息
      datum.setError(new Utf8(error));
      // 设置完成时间
      datum.setFinishTime(finishTime);
      // 设置任务类型
      datum.setTaskType(new Utf8(taskType.name()));
      // 设置导致失败的尝试ID，空值处理
      datum.setFailedDueToAttempt(
          failedDueToAttempt == null
          ? null
          : new Utf8(failedDueToAttempt.toString()));
      // 设置任务状态
      datum.setStatus(new Utf8(status));
      // 将Hadoop Counters转换为Avro格式存储
      datum.setCounters(EventWriter.toAvro(counters));
    }
    return datum;
  }
  
  /**
   * 从Avro数据对象中解析出事件信息，反序列化
   * @param odatum Avro格式的事件数据对象
   */
  public void setDatum(Object odatum) {
    this.datum = (TaskFailed)odatum;
    // 解析任务ID
    this.id =
        TaskID.forName(datum.getTaskid().toString());
    // 解析任务类型
    this.taskType =
        TaskType.valueOf(datum.getTaskType().toString());
    // 解析完成时间
    this.finishTime = datum.getFinishTime();
    // 解析错误信息
    this.error = datum.getError().toString();
    // 解析导致失败的尝试ID，空值处理
    this.failedDueToAttempt =
        datum.getFailedDueToAttempt() == null
        ? null
        : TaskAttemptID.forName(
            datum.getFailedDueToAttempt().toString());
    // 解析任务状态
    this.status = datum.getStatus().toString();
    // 从Avro格式转换回Hadoop Counters对象
    this.counters =
        EventReader.fromAvro(datum.getCounters());
  }

  /** Gets the task id. */
  public TaskID getTaskId() { return id; }
  /** Gets the error string. */
  public String getError() { return error; }
  /** Gets the finish time of the attempt. */
  public long getFinishTime() {
    return finishTime;
  }
  /**
   * 获取任务开始时间，用于上报给YARN应用时间线服务ATSv2
   * @return 任务开始时间
   */
  public long getStartTime() {
    return startTime;
  }
  /** Gets the task type. */
  public TaskType getTaskType() {
    return taskType;
  }
  /** Gets the attempt id due to which the task failed. */
  public TaskAttemptID getFailedAttemptID() {
    return failedDueToAttempt;
  }
  /**
   * 获取任务失败状态
   * @return 任务状态字符串
   */
  public String getTaskStatus() { return status; }
  /** Gets task counters. */
  public Counters getCounters() { return counters; }
  /** Gets the event type. */
  public EventType getEventType() {
    return EventType.TASK_FAILED;
  }

  /**
   * 将当前任务失败事件转换为YARN时间线服务可识别的TimelineEvent对象
   * @return 转换后的TimelineEvent
   */
  @Override
  public TimelineEvent toTimelineEvent() {
    TimelineEvent tEvent = new TimelineEvent();
    // 设置事件ID为大写事件类型名
    tEvent.setId(StringUtils.toUpperCase(getEventType().name()));
    // 添加任务类型信息
    tEvent.addInfo("TASK_TYPE", getTaskType().toString());
    // 添加失败状态信息
    tEvent.addInfo("STATUS", TaskStatus.State.FAILED.toString());
    // 添加完成时间信息
    tEvent.addInfo("FINISH_TIME", getFinishTime());
    // 添加错误信息
    tEvent.addInfo("ERROR", getError());
    // 添加导致失败的尝试ID信息
    tEvent.addInfo("FAILED_ATTEMPT_ID",
        getFailedAttemptID() == null ? "" : getFailedAttemptID().toString());
    return tEvent;
  }

  /**
   * 将任务计量统计信息转换为YARN时间线服务可识别的TimelineMetric集合
   * @return 转换后的度量指标集合
   */
  @Override
  public Set<TimelineMetric> getTimelineMetrics() {
    Set<TimelineMetric> metrics = JobHistoryEventUtils
        .countersToTimelineMetric(getCounters(), finishTime);
    return metrics;
  }
}