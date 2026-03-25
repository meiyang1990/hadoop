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
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric;

/**
 * 任务启动事件类，用于在作业历史中记录任务启动的相关信息
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class TaskStartedEvent implements HistoryEvent {
  private TaskStarted datum = new TaskStarted();

  /**
   * 构造任务启动事件，记录任务启动相关信息
   * @param id 任务ID
   * @param startTime 任务启动时间
   * @param taskType 任务类型（Map/Reduce等）
   * @param splitLocations 数据分片位置，仅对Map任务有效
   */
  public TaskStartedEvent(TaskID id, long startTime, 
      TaskType taskType, String splitLocations) {
    datum.setTaskid(new Utf8(id.toString()));
    datum.setSplitLocations(new Utf8(splitLocations));
    datum.setStartTime(startTime);
    datum.setTaskType(new Utf8(taskType.name()));
  }

  TaskStartedEvent() {}

  /**
   * 获取Avro序列化数据对象
   * @return Avro序列化的任务启动数据对象
   */
  public Object getDatum() { return datum; }

  /**
   * 设置Avro序列化数据对象
   * @param datum Avro序列化的任务启动数据对象
   */
  public void setDatum(Object datum) { this.datum = (TaskStarted)datum; }

  /**
   * 获取任务ID
   * @return 任务ID
   */
  public TaskID getTaskId() {
    return TaskID.forName(datum.getTaskid().toString());
  }

  /**
   * 获取数据分片位置，仅对Map任务有效
   * @return 数据分片位置字符串
   */
  public String getSplitLocations() {
    return datum.getSplitLocations().toString();
  }

  /**
   * 获取任务启动时间
   * @return 任务启动时间戳
   */
  public long getStartTime() { return datum.getStartTime(); }

  /**
   * 获取任务类型
   * @return 任务类型枚举
   */
  public TaskType getTaskType() {
    return TaskType.valueOf(datum.getTaskType().toString());
  }

  /**
   * 获取事件类型
   * @return 事件类型，固定为TASK_STARTED
   */
  public EventType getEventType() {
    return EventType.TASK_STARTED;
  }

  @Override
  /**
   * 将当前事件转换为YARN时间线服务事件，用于应用监控
   * @return 转换后的YARN时间线事件
   */
  public TimelineEvent toTimelineEvent() {
    TimelineEvent tEvent = new TimelineEvent();
    tEvent.setId(StringUtils.toUpperCase(getEventType().name()));
    tEvent.addInfo("TASK_TYPE", getTaskType().toString());
    tEvent.addInfo("START_TIME", getStartTime());
    tEvent.addInfo("SPLIT_LOCATIONS", getSplitLocations());
    return tEvent;
  }

  @Override
  /**
   * 获取当前事件对应的时间线指标集合，本事件无指标数据
   * @return 固定返回null
   */
  public Set<TimelineMetric> getTimelineMetrics() {
    return null;
  }

}