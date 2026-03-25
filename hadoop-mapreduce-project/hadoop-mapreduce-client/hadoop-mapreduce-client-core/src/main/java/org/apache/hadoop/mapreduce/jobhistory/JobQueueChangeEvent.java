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

import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric;

/**
 * 作业队列变更历史事件，记录作业从一个队列移动到另一个队列的变更信息，
 * 用于MapReduce作业历史日志记录，支持作业调度变更的审计和回溯。
 */
@SuppressWarnings("deprecation")
public class JobQueueChangeEvent implements HistoryEvent {
  private JobQueueChange datum = new JobQueueChange();
  
  /**
   * 构造作业队列变更事件，记录作业ID和新队列名称
   * @param id 作业ID
   * @param queueName 变更后的新队列名称
   */
  public JobQueueChangeEvent(JobID id, String queueName) {
    datum.setJobid(id.toString());
    datum.setJobQueueName(queueName);
  }
  
  /**
   * 空构造器，用于反序列化历史事件
   */
  JobQueueChangeEvent() { }
  
  @Override
  public EventType getEventType() {
    return EventType.JOB_QUEUE_CHANGED;
  }

  @Override
  public Object getDatum() {
    return datum;
  }

  @Override
  public void setDatum(Object datum) {
    this.datum = (JobQueueChange) datum;
  }
  
  /** 获取作业ID */
  public JobID getJobId() {
    return JobID.forName(datum.getJobid().toString());
  }
  
  /** 获取变更后的新作业队列名称 */
  public String getJobQueueName() {
    java.lang.CharSequence jobQueueName = datum.getJobQueueName();
    if (jobQueueName != null) {
      return jobQueueName.toString();
    }
    return null;
  }

  @Override
  public TimelineEvent toTimelineEvent() {
    // 创建时间线服务事件对象
    TimelineEvent tEvent = new TimelineEvent();
    // 设置事件ID为大写的事件类型名
    tEvent.setId(StringUtils.toUpperCase(getEventType().name()));
    // 添加队列名称信息到事件中
    tEvent.addInfo("QUEUE_NAMES", getJobQueueName());
    return tEvent;
  }

  @Override
  public Set<TimelineMetric> getTimelineMetrics() {
    return null;
  }

}