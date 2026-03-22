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
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric;

/**
 * 任务更新事件，用于记录任务运行过程中的更新信息到作业历史
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class TaskUpdatedEvent implements HistoryEvent {
  private TaskUpdated datum = new TaskUpdated();

  /**
   * 创建任务更新事件，用于记录任务更新
   * @param id 任务ID
   * @param finishTime 任务完成时间
   */
  public TaskUpdatedEvent(TaskID id, long finishTime) {
    datum.setTaskid(new Utf8(id.toString()));
    datum.setFinishTime(finishTime);
  }

  TaskUpdatedEvent() {}

  public Object getDatum() { return datum; }
  public void setDatum(Object datum) { this.datum = (TaskUpdated)datum; }

  /** 获取任务ID */
  public TaskID getTaskId() {
    return TaskID.forName(datum.getTaskid().toString());
  }
  /** 获取任务完成时间 */
  public long getFinishTime() { return datum.getFinishTime(); }
  /** 获取事件类型 */
  public EventType getEventType() {
    return EventType.TASK_UPDATED;
  }

  @Override
  /** 将当前事件转换为YARN时间线服务可存储的事件格式 */
  public TimelineEvent toTimelineEvent() {
    TimelineEvent tEvent = new TimelineEvent();
    // 设置事件ID，使用大写化的事件类型名称
    tEvent.setId(StringUtils.toUpperCase(getEventType().name()));
    // 添加任务完成时间信息
    tEvent.addInfo("FINISH_TIME", getFinishTime());
    return tEvent;
  }

  @Override
  /** 获取该事件对应的时间线指标，此事件无指标返回null */
  public Set<TimelineMetric> getTimelineMetrics() {
    return null;
  }

}