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
 * 作业信息变更事件，用于记录作业提交时间和启动时间的变更，存储到作业历史日志
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class JobInfoChangeEvent implements HistoryEvent {
  private JobInfoChange datum = new JobInfoChange();

  /** 
   * 构造作业信息变更事件，记录作业ID、提交时间和启动时间
   * @param id 作业ID 
   * @param submitTime 作业提交时间
   * @param launchTime 作业启动时间
   */
  public JobInfoChangeEvent(JobID id, long submitTime, long launchTime) {
    datum.setJobid(new Utf8(id.toString()));
    datum.setSubmitTime(submitTime);
    datum.setLaunchTime(launchTime);
  }

  JobInfoChangeEvent() { }

  @Override
  public Object getDatum() { return datum; }
  @Override
  public void setDatum(Object datum) {
    this.datum = (JobInfoChange)datum;
  }

  /** 获取作业ID */
  public JobID getJobId() { return JobID.forName(datum.getJobid().toString()); }
  /** 获取作业提交时间 */
  public long getSubmitTime() { return datum.getSubmitTime(); }
  /** 获取作业启动时间 */
  public long getLaunchTime() { return datum.getLaunchTime(); }

  @Override
  public EventType getEventType() {
    return EventType.JOB_INFO_CHANGED;
  }

  @Override
  public TimelineEvent toTimelineEvent() {
    // 创建YARN时间线服务事件对象
    TimelineEvent tEvent = new TimelineEvent();
    // 设置事件ID为事件类型大写名称
    tEvent.setId(StringUtils.toUpperCase(getEventType().name()));
    // 添加提交时间信息
    tEvent.addInfo("SUBMIT_TIME", getSubmitTime());
    // 添加启动时间信息
    tEvent.addInfo("LAUNCH_TIME", getLaunchTime());
    return tEvent;
  }

  @Override
  public Set<TimelineMetric> getTimelineMetrics() {
    return null;
  }
}