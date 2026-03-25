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
import org.apache.hadoop.mapred.JobPriority;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric;

/**
 * 作业优先级变更事件，用于在作业历史中记录作业优先级变更的操作
 * 属于MapReduce作业历史日志体系中的事件类型，用于审计和状态回溯
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class JobPriorityChangeEvent implements HistoryEvent {
  private JobPriorityChange datum = new JobPriorityChange();

  /**
   * 构造作业优先级变更事件，记录作业ID和变更后的新优先级
   * @param id 发生优先级变更的作业ID
   * @param priority 变更后的作业新优先级
   */
  public JobPriorityChangeEvent(JobID id, JobPriority priority) {
    datum.setJobid(new Utf8(id.toString()));
    datum.setPriority(new Utf8(priority.name()));
  }

  JobPriorityChangeEvent() { }

  @Override
  public Object getDatum() { return datum; }

  @Override
  public void setDatum(Object datum) {
    this.datum = (JobPriorityChange)datum;
  }

  /** 获取发生优先级变更的作业ID */
  public JobID getJobId() {
    return JobID.forName(datum.getJobid().toString());
  }

  /** 获取变更后的作业优先级 */
  public JobPriority getPriority() {
    return JobPriority.valueOf(datum.getPriority().toString());
  }

  /** 获取事件类型 */
  public EventType getEventType() {
    return EventType.JOB_PRIORITY_CHANGED;
  }

  @Override
  public TimelineEvent toTimelineEvent() {
    // 创建YARN时间线服务事件对象
    TimelineEvent tEvent = new TimelineEvent();
    // 设置事件ID为大写的事件类型名称
    tEvent.setId(StringUtils.toUpperCase(getEventType().name()));
    // 添加优先级信息到事件元数据
    tEvent.addInfo("PRIORITY", getPriority().toString());
    return tEvent;
  }

  @Override
  public Set<TimelineMetric> getTimelineMetrics() {
    return null;
  }

}