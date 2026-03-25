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
 * 任务尝试完成事件，用于记录任务尝试执行完成的相关信息到作业历史
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class TaskAttemptFinishedEvent  implements HistoryEvent {

  // Avro序列化对象
  private TaskAttemptFinished datum = null;

  // 任务尝试ID
  private TaskAttemptID attemptId;
  // 任务类型（Map/Reduce等
  private TaskType taskType;
  // 任务状态
  private String taskStatus;
  // 任务尝试完成时间戳
  private long finishTime;
  // 任务运行所在机架名
  private String rackName;
  // 任务运行所在主机名
  private String hostname;
  // 任务尝试最终状态字符串
  private String state;
  // 任务尝试执行计数器
  private Counters counters;
  // 任务尝试开始时间戳，用于YARN时间线服务v2
  private long startTime;

  /**
   * 构造任务尝试完成事件，用于记录任务尝试执行完成信息
   * @param id 任务尝试ID
   * @param taskType 任务类型
   * @param taskStatus 任务状态
   * @param finishTime 任务尝试完成时间
   * @param hostname 任务尝试运行的主机名
   * @param state 任务尝试状态字符串
   * @param counters 任务尝试执行计数器
   * @param startTs 任务尝试开始时间，用于写入ATSv2
   */
  public TaskAttemptFinishedEvent(TaskAttemptID id, 
      TaskType taskType, String taskStatus, 
      long finishTime, String rackName,
      String hostname, String state, Counters counters, long startTs) {
    this.attemptId = id;
    this.taskType = taskType;
    this.taskStatus = taskStatus;
    this.finishTime = finishTime;
    this.rackName = rackName;
    this.hostname = hostname;
    this.state = state;
    this.counters = counters;
    this.startTime = startTs;
  }

  /**
   * 构造任务尝试完成事件，自动获取当前时间作为开始时间
   * @param id 任务尝试ID
   * @param taskType 任务类型
   * @param taskStatus 任务状态
   * @param finishTime 任务尝试完成时间
   * @param rackName 任务尝试运行的机架名
   * @param hostname 任务尝试运行的主机名
   * @param state 任务尝试状态字符串
   * @param counters 任务尝试执行计数器
   */
  public TaskAttemptFinishedEvent(TaskAttemptID id, TaskType taskType,
      String taskStatus, long finishTime, String rackName, String hostname,
      String state, Counters counters) {
    this(id, taskType, taskStatus, finishTime, rackName, hostname, state,
        counters, SystemClock.getInstance().getTime());
  }

  TaskAttemptFinishedEvent() {}

  /**
   * 获取用于序列化的Avro数据对象，构造并填充事件数据
   * @return Avro格式的事件数据对象
   */
  public Object getDatum() {
    if (datum == null) {
      datum = new TaskAttemptFinished();
      datum.setTaskid(new Utf8(attemptId.getTaskID().toString()));
      datum.setAttemptId(new Utf8(attemptId.toString()));
      datum.setTaskType(new Utf8(taskType.name()));
      datum.setTaskStatus(new Utf8(taskStatus));
      datum.setFinishTime(finishTime);
      if (rackName != null) {
        datum.setRackname(new Utf8(rackName));
      }
      datum.setHostname(new Utf8(hostname));
      datum.setState(new Utf8(state));
      datum.setCounters(EventWriter.toAvro(counters));
    }
    return datum;
  }

  /**
   * 从Avro数据对象反序列化，恢复事件信息
   * @param oDatum Avro格式的事件数据对象
   */
  public void setDatum(Object oDatum) {
    this.datum = (TaskAttemptFinished)oDatum;
    this.attemptId = TaskAttemptID.forName(datum.getAttemptId().toString());
    this.taskType = TaskType.valueOf(datum.getTaskType().toString());
    this.taskStatus = datum.getTaskStatus().toString();
    this.finishTime = datum.getFinishTime();
    this.rackName = datum.getRackname().toString();
    this.hostname = datum.getHostname().toString();
    this.state = datum.getState().toString();
    this.counters = EventReader.fromAvro(datum.getCounters());
  }

  /** 获取任务ID */
  public TaskID getTaskId() { return attemptId.getTaskID(); }
  /** 获取任务尝试ID */
  public TaskAttemptID getAttemptId() {
    return attemptId;
  }
  /** 获取任务类型 */
  public TaskType getTaskType() {
    return taskType;
  }
  /** 获取任务状态 */
  public String getTaskStatus() { return taskStatus.toString(); }
  /** 获取任务尝试完成时间 */
  public long getFinishTime() { return finishTime; }
  /**
   * 获取任务尝试开始时间，用于发布到ATSv2
   * @return 任务尝试开始时间戳
   */
  public long getStartTime() {
    return startTime;
  }
  /** 获取任务尝试运行的主机名 */
  public String getHostname() { return hostname.toString(); }
  
  /** 获取任务尝试运行的机架名 */
  public String getRackName() {
    return rackName == null ? null : rackName.toString();
  }
  
  /**
   * 获取任务尝试状态字符串
   * @return 任务尝试状态
   */
  public String getState() { return state.toString(); }
  /** 获取任务尝试执行计数器 */
  Counters getCounters() { return counters; }
  /**
   * 获取事件类型，根据任务类型返回MAP或REDUCE尝试完成类型
   * @return 事件类型枚举
   */
  public EventType getEventType() {
    // Note that the task type can be setup/map/reduce/cleanup but the 
    // attempt-type can only be map/reduce.
    return getTaskId().getTaskType() == TaskType.MAP 
           ? EventType.MAP_ATTEMPT_FINISHED
           : EventType.REDUCE_ATTEMPT_FINISHED;
  }

  /**
   * 将当前事件转换为YARN时间线服务事件格式
   * @return 时间线服务事件对象
   */
  @Override
  public TimelineEvent toTimelineEvent() {
    TimelineEvent tEvent = new TimelineEvent();
    tEvent.setId(StringUtils.toUpperCase(getEventType().name()));
    tEvent.addInfo("TASK_TYPE", getTaskType().toString());
    tEvent.addInfo("ATTEMPT_ID", getAttemptId() == null ?
        "" : getAttemptId().toString());
    tEvent.addInfo("FINISH_TIME", getFinishTime());
    tEvent.addInfo("STATUS", getTaskStatus());
    tEvent.addInfo("STATE", getState());
    tEvent.addInfo("HOSTNAME", getHostname());
    return tEvent;
  }

  /**
   * 将任务计数器转换为YARN时间线服务指标集合
   * @return 时间线服务指标集合
   */
  @Override
  public Set<TimelineMetric> getTimelineMetrics() {
    Set<TimelineMetric> metrics = JobHistoryEventUtils
        .countersToTimelineMetric(getCounters(), finishTime);
    return metrics;
  }
}