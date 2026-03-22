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
 * 作业初始化完成事件，用于在作业历史中记录作业初始化完成的信息
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class JobInitedEvent implements HistoryEvent {
  private JobInited datum = new JobInited();

  /**
   * 创建作业初始化完成事件，用于记录作业初始化信息
   * @param id 作业ID
   * @param launchTime 作业启动时间
   * @param totalMaps 作业总的Map任务数
   * @param totalReduces 作业总的Reduce任务数
   * @param jobStatus 作业初始状态
   * @param uberized 作业是否开启uber模式（Map和Reduce阶段在同一个JVM中运行）
   */
  public JobInitedEvent(JobID id, long launchTime, int totalMaps,
                        int totalReduces, String jobStatus, boolean uberized) {
    datum.setJobid(new Utf8(id.toString()));
    datum.setLaunchTime(launchTime);
    datum.setTotalMaps(totalMaps);
    datum.setTotalReduces(totalReduces);
    datum.setJobStatus(new Utf8(jobStatus));
    datum.setUberized(uberized);
  }

  JobInitedEvent() { }

  public Object getDatum() { return datum; }
  public void setDatum(Object datum) { this.datum = (JobInited)datum; }

  /** 获取作业ID */
  public JobID getJobId() { return JobID.forName(datum.getJobid().toString()); }
  /** 获取作业启动时间 */
  public long getLaunchTime() { return datum.getLaunchTime(); }
  /** 获取作业总的Map任务数 */
  public int getTotalMaps() { return datum.getTotalMaps(); }
  /** 获取作业总的Reduce任务数 */
  public int getTotalReduces() { return datum.getTotalReduces(); }
  /** 获取作业初始状态 */
  public String getStatus() { return datum.getJobStatus().toString(); }
  /** 获取事件类型 */
  public EventType getEventType() {
    return EventType.JOB_INITED;
  }
  /** 获取作业是否开启uber模式 */
  public boolean getUberized() { return datum.getUberized(); }

  @Override
  /** 将当前事件转换为YARN时间线服务事件 */
  public TimelineEvent toTimelineEvent() {
    TimelineEvent tEvent = new TimelineEvent();
    // 设置事件ID为事件类型大写名称
    tEvent.setId(StringUtils.toUpperCase(getEventType().name()));
    // 添加启动时间信息
    tEvent.addInfo("START_TIME", getLaunchTime());
    // 添加作业状态信息
    tEvent.addInfo("STATUS", getStatus());
    // 添加总Map任务数信息
    tEvent.addInfo("TOTAL_MAPS", getTotalMaps());
    // 添加总Reduce任务数信息
    tEvent.addInfo("TOTAL_REDUCES", getTotalReduces());
    // 添加uber模式标识信息
    tEvent.addInfo("UBERIZED", getUberized());
    return tEvent;
  }

  @Override
  /** 获取时间线指标，本事件无指标，返回null */
  public Set<TimelineMetric> getTimelineMetrics() {
    return null;
  }
}