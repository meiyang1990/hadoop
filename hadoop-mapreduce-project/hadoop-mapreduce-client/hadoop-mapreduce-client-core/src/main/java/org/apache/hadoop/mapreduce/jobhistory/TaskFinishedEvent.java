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
 * 任务成功完成事件，用于在作业历史中记录任务完成信息
 * 属于MapReduce作业历史日志系统，记录任务完成时的核心状态数据
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class TaskFinishedEvent implements HistoryEvent {

  private TaskFinished datum = null;

  private TaskID taskid;
  private TaskAttemptID successfulAttemptId;
  private long finishTime;
  private TaskType taskType;
  private String status;
  private Counters counters;
  private long startTime;

  /**
   * 构造任务完成事件，记录任务成功完成的相关信息
   * @param id 任务ID
   * @param attemptId 当前任务成功运行的尝试ID
   * @param finishTime 任务完成时间戳
   * @param taskType 任务类型（Map/Reduce等）
   * @param status 任务状态字符串
   * @param counters 任务运行计数器
   * @param startTs 任务开始时间戳
   */
  public TaskFinishedEvent(TaskID id, TaskAttemptID attemptId, long finishTime,
                           TaskType taskType,
                           String status, Counters counters, long startTs) {
    this.taskid = id;
    this.successfulAttemptId = attemptId;
    this.finishTime = finishTime;
    this.taskType = taskType;
    this.status = status;
    this.counters = counters;
    this.startTime = startTs;
  }

  /**
   * 构造任务完成事件，自动获取当前时间作为任务开始时间
   * @param id 任务ID
   * @param attemptId 当前任务成功运行的尝试ID
   * @param finishTime 任务完成时间戳
   * @param taskType 任务类型（Map/Reduce等）
   * @param status 任务状态字符串
   * @param counters 任务运行计数器
   */
  public TaskFinishedEvent(TaskID id, TaskAttemptID attemptId, long finishTime,
          TaskType taskType, String status, Counters counters) {
    this(id, attemptId, finishTime, taskType, status, counters,
        SystemClock.getInstance().getTime());
  }

  /**
   * 无参构造器，用于反序列化场景
   */
  TaskFinishedEvent() {}

  /**
   * 获取Avro序列化后的事件数据对象
   * 将事件属性转换为Avro格式用于持久化存储
   * @return Avro格式的事件数据对象
   */
  public Object getDatum() {
    if (datum == null) {
      datum = new TaskFinished();
      datum.setTaskid(new Utf8(taskid.toString()));
      if(successfulAttemptId != null)
      {
        datum.setSuccessfulAttemptId(new Utf8(successfulAttemptId.toString()));
      }
      datum.setFinishTime(finishTime);
      datum.setCounters(EventWriter.toAvro(counters));
      datum.setTaskType(new Utf8(taskType.name()));
      datum.setStatus(new Utf8(status));
    }
    return datum;
  }

  /**
   * 从Avro数据对象中反序列化恢复事件属性
   * @param oDatum Avro格式的事件数据对象
   */
  public void setDatum(Object oDatum) {
    this.datum = (TaskFinished)oDatum;
    this.taskid = TaskID.forName(datum.getTaskid().toString());
    if (datum.getSuccessfulAttemptId() != null) {
      this.successfulAttemptId = TaskAttemptID
          .forName(datum.getSuccessfulAttemptId().toString());
    }
    this.finishTime = datum.getFinishTime();
    this.taskType = TaskType.valueOf(datum.getTaskType().toString());
    this.status = datum.getStatus().toString();
    this.counters = EventReader.fromAvro(datum.getCounters());
  }

  /** 获取任务ID */
  public TaskID getTaskId() { return taskid; }
  /** 获取成功完成的任务尝试ID */
  public TaskAttemptID getSuccessfulTaskAttemptId() {
    return successfulAttemptId;
  }
  /** 获取任务完成时间戳 */
  public long getFinishTime() { return finishTime; }
  /**
   * 获取任务开始时间戳，用于上报到ATSv2时间线服务
   * @return 任务开始时间戳
   */
  public long getStartTime() {
    return startTime;
  }
  /** 获取任务运行计数器 */
  public Counters getCounters() { return counters; }
  /** 获取任务类型 */
  public TaskType getTaskType() {
    return taskType;
  }
  /**
   * 获取任务状态
   * @return 任务状态字符串
   */
  public String getTaskStatus() { return status.toString(); }
  /** 获取事件类型 */
  public EventType getEventType() {
    return EventType.TASK_FINISHED;
  }

  @Override
  /**
   * 将当前事件转换为YARN时间线服务可识别的事件对象
   * 用于将作业历史数据上报到YARN时间线服务进行可视化展示
   * @return YARN时间线事件对象
   */
  public TimelineEvent toTimelineEvent() {
    TimelineEvent tEvent = new TimelineEvent();
    tEvent.setId(StringUtils.toUpperCase(getEventType().name()));
    tEvent.addInfo("TASK_TYPE", getTaskType().toString());
    tEvent.addInfo("FINISH_TIME", getFinishTime());
    tEvent.addInfo("STATUS", TaskStatus.State.SUCCEEDED.toString());
    tEvent.addInfo("SUCCESSFUL_TASK_ATTEMPT_ID",
        getSuccessfulTaskAttemptId() == null ? "" :
            getSuccessfulTaskAttemptId().toString());
    return tEvent;
  }

  @Override
  /**
   * 将任务计数器转换为YARN时间线服务可识别的指标集合
   * 用于将任务运行指标上报到YARN时间线服务
   * @return 时间线指标集合
   */
  public Set<TimelineMetric> getTimelineMetrics() {
    Set<TimelineMetric> jobMetrics = JobHistoryEventUtils
        .countersToTimelineMetric(getCounters(), finishTime);
    return jobMetrics;
  }
}