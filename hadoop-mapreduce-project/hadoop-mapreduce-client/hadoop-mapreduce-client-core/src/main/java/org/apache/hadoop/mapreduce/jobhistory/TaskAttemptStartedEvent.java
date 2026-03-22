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
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric;

/**
 * 任务尝试启动事件，用于记录任务尝试开始运行的历史信息，供作业历史服务存储和查询
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class TaskAttemptStartedEvent implements HistoryEvent {
  private TaskAttemptStarted datum = new TaskAttemptStarted();

  /**
   * 构造任务尝试启动事件，记录任务尝试启动的各项信息
   * @param attemptId 任务尝试ID
   * @param taskType 任务类型（Map/Reduce）
   * @param startTime 任务尝试启动时间戳
   * @param trackerName 运行该任务尝试的TaskTracker节点名称
   * @param httpPort TaskTracker节点的HTTP服务端口
   * @param shufflePort 容器的Shuffle服务端口
   * @param containerId 运行该任务尝试的YARN容器ID
   * @param locality 任务尝试的数据本地性信息
   * @param avataar 任务尝试的调度标识（推测执行相关）
   */
  public TaskAttemptStartedEvent( TaskAttemptID attemptId,  
      TaskType taskType, long startTime, String trackerName,
      int httpPort, int shufflePort, ContainerId containerId,
      String locality, String avataar) {
    datum.setAttemptId(new Utf8(attemptId.toString()));
    datum.setTaskid(new Utf8(attemptId.getTaskID().toString()));
    datum.setStartTime(startTime);
    datum.setTaskType(new Utf8(taskType.name()));
    datum.setTrackerName(new Utf8(trackerName));
    datum.setHttpPort(httpPort);
    datum.setShufflePort(shufflePort);
    datum.setContainerId(new Utf8(containerId.toString()));
    if (locality != null) {
      datum.setLocality(new Utf8(locality));
    }
    if (avataar != null) {
      datum.setAvataar(new Utf8(avataar));
    }
  }

  // TODO Remove after MrV1 is removed.
  // Using a dummy containerId to prevent jobHistory parse failures.
  /**
   * 兼容MRv1的构造方法，使用虚拟容器ID避免作业历史解析失败，MRv1移除后将删除该方法
   * @param attemptId 任务尝试ID
   * @param taskType 任务类型（Map/Reduce）
   * @param startTime 任务尝试启动时间戳
   * @param trackerName 运行该任务尝试的TaskTracker节点名称
   * @param httpPort TaskTracker节点的HTTP服务端口
   * @param shufflePort 容器的Shuffle服务端口
   * @param locality 任务尝试的数据本地性信息
   * @param avataar 任务尝试的调度标识（推测执行相关）
   */
  public TaskAttemptStartedEvent(TaskAttemptID attemptId, TaskType taskType,
      long startTime, String trackerName, int httpPort, int shufflePort,
      String locality, String avataar) {
    this(attemptId, taskType, startTime, trackerName, httpPort, shufflePort,
        ContainerId.fromString("container_-1_-1_-1_-1"), locality,
            avataar);
  }

  TaskAttemptStartedEvent() {}

  public Object getDatum() { return datum; }
  public void setDatum(Object datum) {
    this.datum = (TaskAttemptStarted)datum;
  }

  /** 获取任务ID */
  public TaskID getTaskId() {
    return TaskID.forName(datum.getTaskid().toString());
  }
  /** 获取运行该任务尝试的节点名称 */
  public String getTrackerName() { return datum.getTrackerName().toString(); }
  /** 获取任务尝试启动时间 */
  public long getStartTime() { return datum.getStartTime(); }
  /** 获取任务类型 */
  public TaskType getTaskType() {
    return TaskType.valueOf(datum.getTaskType().toString());
  }
  /** 获取HTTP服务端口 */
  public int getHttpPort() { return datum.getHttpPort(); }
  /** 获取Shuffle服务端口 */
  public int getShufflePort() { return datum.getShufflePort(); }
  /** 获取任务尝试ID */
  public TaskAttemptID getTaskAttemptId() {
    return TaskAttemptID.forName(datum.getAttemptId().toString());
  }
  /** 获取事件类型，根据任务类型返回Map尝试启动或Reduce尝试启动 */
  public EventType getEventType() {
    // Note that the task type can be setup/map/reduce/cleanup but the 
    // attempt-type can only be map/reduce.
   return getTaskId().getTaskType() == TaskType.MAP 
           ? EventType.MAP_ATTEMPT_STARTED 
           : EventType.REDUCE_ATTEMPT_STARTED;
  }
  /** 获取运行该任务尝试的YARN容器ID */
  public ContainerId getContainerId() {
    return ContainerId.fromString(datum.getContainerId().toString());
  }
  /** 获取任务尝试的数据本地性信息 */
  public String getLocality() {
    if (datum.getLocality() != null) {
      return datum.getLocality().toString();
    }
    return null;
  }
  /** 获取任务尝试的调度标识 */
  public String getAvataar() {
    if (datum.getAvataar() != null) {
      return datum.getAvataar().toString();
    }
    return null;
  }

  @Override
  /** 将当前事件转换为YARN时间线服务可用的事件对象 */
  public TimelineEvent toTimelineEvent() {
    TimelineEvent tEvent = new TimelineEvent();
    tEvent.setId(StringUtils.toUpperCase(getEventType().name()));
    tEvent.addInfo("TASK_TYPE", getTaskType().toString());
    tEvent.addInfo("TASK_ATTEMPT_ID",
        getTaskAttemptId().toString());
    tEvent.addInfo("START_TIME", getStartTime());
    tEvent.addInfo("HTTP_PORT", getHttpPort());
    tEvent.addInfo("TRACKER_NAME", getTrackerName());
    tEvent.addInfo("SHUFFLE_PORT", getShufflePort());
    tEvent.addInfo("CONTAINER_ID", getContainerId() == null ?
        "" : getContainerId().toString());
    return tEvent;
  }

  @Override
  /** 获取当前事件关联的时间线指标，本事件无指标，返回null */
  public Set<TimelineMetric> getTimelineMetrics() {
    return null;
  }

}