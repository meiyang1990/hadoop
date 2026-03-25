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
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric;

/**
 * 任务状态变更事件，用于在作业历史中记录作业运行状态的变更
 * 属于MapReduce作业历史日志系统，负责存储作业状态变更的审计信息
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class JobStatusChangedEvent implements HistoryEvent {
  private JobStatusChanged datum = new JobStatusChanged();

  /**
   * 构造作业状态变更事件，记录指定作业的新状态
   * @param id 作业ID
   * @param jobStatus 变更后的新作业状态
   */
  public JobStatusChangedEvent(JobID id, String jobStatus) {
    datum.setJobid(new Utf8(id.toString()));
    datum.setJobStatus(new Utf8(jobStatus));
  }

  JobStatusChangedEvent() {}

  public Object getDatum() { return datum; }
  public void setDatum(Object datum) {
    this.datum = (JobStatusChanged)datum;
  }

  /** 获取发生状态变更的作业ID */
  public JobID getJobId() { return JobID.forName(datum.getJobid().toString()); }
  /** 获取变更后的作业状态 */
  public String getStatus() { return datum.getJobStatus().toString(); }
  /** 获取事件类型 */
  public EventType getEventType() {
    return EventType.JOB_STATUS_CHANGED;
  }

  @Override
  /** 将当前事件转换为YARN时间线服务可识别的事件格式 */
  public TimelineEvent toTimelineEvent() {
    TimelineEvent tEvent = new TimelineEvent();
    // 设置事件ID为大写的事件类型名
    tEvent.setId(StringUtils.toUpperCase(getEventType().name()));
    // 添加作业状态信息到事件中
    tEvent.addInfo("STATUS", getStatus());
    return tEvent;
  }

  @Override
  /** 获取当前事件关联的时间线指标，本事件无指标返回null */
  public Set<TimelineMetric> getTimelineMetrics() {
    return null;
  }

}