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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric;

/**
 * 文件说明：MapReduce作业历史记录中归一化资源请求事件，记录归一化后的Map/Reduce任务资源需求
 * 用于作业历史中存储资源调度相关的归一化资源请求信息，支持时间线服务指标导出
 */
/**
 * Event to record the normalized map/reduce requirements.
 * 
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class NormalizedResourceEvent implements HistoryEvent {
  private long memory;
  private TaskType taskType;
  
  /**
   * 构造归一化资源请求事件，保存提交到ResourceManager的归一化资源需求
   * @param taskType 请求资源的任务类型（Map/Reduce）
   * @param memory 归一化后的内存资源需求大小
   */
  public NormalizedResourceEvent(TaskType taskType, long memory) {
    this.memory = memory;
    this.taskType = taskType;
  }
  
  /**
   * 获取事件对应的任务类型
   * @return 任务类型（Map/Reduce）
   */
  public TaskType getTaskType() {
    return this.taskType;
  }
  
  /**
   * 获取归一化后的内存资源需求
   * @return 归一化后的内存大小
   */
  public long getMemory() {
    return this.memory;
  }
  
  @Override
  public EventType getEventType() {
    return EventType.NORMALIZED_RESOURCE;
  }

  @Override
  public Object getDatum() {
    throw new UnsupportedOperationException("Not a seriable object");
  }

  @Override
  public void setDatum(Object datum) {
    throw new UnsupportedOperationException("Not a seriable object");
  }

  @Override
  public TimelineEvent toTimelineEvent() {
    // 创建时间线事件对象
    TimelineEvent tEvent = new TimelineEvent();
    // 设置事件ID为事件类型大写名称
    tEvent.setId(StringUtils.toUpperCase(getEventType().name()));
    // 添加归一化内存信息到事件
    tEvent.addInfo("MEMORY", "" + getMemory());
    // 添加任务类型信息到事件
    tEvent.addInfo("TASK_TYPE", getTaskType());
    return tEvent;
  }

  @Override
  public Set<TimelineMetric> getTimelineMetrics() {
    return null;
  }
}