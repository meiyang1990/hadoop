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
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.util.JobHistoryEventUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric;

/**
 * 作业完成事件，用于记录作业成功完成的相关信息，存储在作业历史日志中
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class JobFinishedEvent implements HistoryEvent {

  // Avro序列化后的事件数据对象
  private JobFinished datum = null;

  // 作业ID
  private JobID jobId;
  // 作业完成时间戳
  private long finishTime;
  // 成功完成的Map任务数
  private int succeededMaps;
  // 成功完成的Reduce任务数
  private int succeededReduces;
  // 失败的Map任务数
  private int failedMaps;
  // 失败的Reduce任务数
  private int failedReduces;
  // 被杀死的Map任务数
  private int killedMaps;
  // 被杀死的Reduce任务数
  private int killedReduces;
  // Map阶段计数器集合
  private Counters mapCounters;
  // Reduce阶段计数器集合
  private Counters reduceCounters;
  // 作业全局计数器集合
  private Counters totalCounters;

  /** 
   * 构造作业完成事件，记录作业完成时的核心信息
   * @param id 作业ID
   * @param finishTime 作业完成时间戳
   * @param succeededMaps 成功完成的Map任务数
   * @param succeededReduces 成功完成的Reduce任务数
   * @param failedMaps 失败的Map任务数
   * @param failedReduces 失败的Reduce任务数
   * @param mapCounters Map阶段计数器
   * @param reduceCounters Reduce阶段计数器
   * @param totalCounters 作业全局计数器
   */
  public JobFinishedEvent(JobID id, long finishTime,
      int succeededMaps, int succeededReduces,
      int failedMaps, int failedReduces,
      int killedMaps, int killedReduces,
      Counters mapCounters, Counters reduceCounters,
      Counters totalCounters) {
    this.jobId = id;
    this.finishTime = finishTime;
    this.succeededMaps = succeededMaps;
    this.succeededReduces = succeededReduces;
    this.failedMaps = failedMaps;
    this.failedReduces = failedReduces;
    this.killedMaps = killedMaps;
    this.killedReduces = killedReduces;
    this.mapCounters = mapCounters;
    this.reduceCounters = reduceCounters;
    this.totalCounters = totalCounters;
  }

  JobFinishedEvent() {}

  /**
   * 获取Avro序列化后的事件数据对象，延迟初始化并填充所有字段
   * @return 可序列化的Avro数据对象
   */
  public Object getDatum() {
    if (datum == null) {
      datum = new JobFinished();
      datum.setJobid(new Utf8(jobId.toString()));
      datum.setFinishTime(finishTime);
      // 保持Avro schema向后兼容性，沿用旧字段名finishedMaps/finishedReduces
      datum.setFinishedMaps(succeededMaps);
      datum.setFinishedReduces(succeededReduces);
      datum.setFailedMaps(failedMaps);
      datum.setFailedReduces(failedReduces);
      datum.setKilledMaps(killedMaps);
      datum.setKilledReduces(killedReduces);
      datum.setMapCounters(EventWriter.toAvro(mapCounters, "MAP_COUNTERS"));
      datum.setReduceCounters(EventWriter.toAvro(reduceCounters,
          "REDUCE_COUNTERS"));
      datum.setTotalCounters(EventWriter.toAvro(totalCounters,
          "TOTAL_COUNTERS"));
    }
    return datum;
  }

  /**
   * 从Avro数据对象反序列化恢复事件信息
   * @param oDatum Avro序列化的事件数据对象
   */
  public void setDatum(Object oDatum) {
    this.datum = (JobFinished) oDatum;
    this.jobId = JobID.forName(datum.getJobid().toString());
    this.finishTime = datum.getFinishTime();
    this.succeededMaps = datum.getFinishedMaps();
    this.succeededReduces = datum.getFinishedReduces();
    this.failedMaps = datum.getFailedMaps();
    this.failedReduces = datum.getFailedReduces();
    this.killedMaps = datum.getKilledMaps();
    this.killedReduces = datum.getKilledReduces();
    this.mapCounters = EventReader.fromAvro(datum.getMapCounters());
    this.reduceCounters = EventReader.fromAvro(datum.getReduceCounters());
    this.totalCounters = EventReader.fromAvro(datum.getTotalCounters());
  }

  public EventType getEventType() {
    return EventType.JOB_FINISHED;
  }

  /** Get the Job ID */
  public JobID getJobid() { return jobId; }
  /** Get the job finish time */
  public long getFinishTime() { return finishTime; }
  /** Get the number of finished maps for the job */
  public int getSucceededMaps() { return succeededMaps; }
  /** Get the number of finished reducers for the job */
  public int getSucceededReduces() { return succeededReduces; }
  /** Get the number of failed maps for the job */
  public int getFailedMaps() { return failedMaps; }
  /** Get the number of failed reducers for the job */
  public int getFailedReduces() { return failedReduces; }
  /** Get the number of killed maps */
  public int getKilledMaps() { return killedMaps; }
  /** Get the number of killed reduces */
  public int getKilledReduces() { return killedReduces; }
  /** Get the counters for the job */
  public Counters getTotalCounters() {
    return totalCounters;
  }
  /** Get the Map counters for the job */
  public Counters getMapCounters() {
    return mapCounters;
  }
  /** Get the reduce counters for the job */
  public Counters getReduceCounters() {
    return reduceCounters;
  }

  /**
   * 将当前事件转换为YARN Timeline Service可存储的事件对象，暴露作业完成指标信息
   * @return Timeline Service格式的事件对象
   */
  @Override
  public TimelineEvent toTimelineEvent() {
    TimelineEvent tEvent = new TimelineEvent();
    tEvent.setId(StringUtils.toUpperCase(getEventType().name()));
    tEvent.addInfo("FINISH_TIME", getFinishTime());
    // 计算总Map任务数（成功+失败+杀死）
    tEvent.addInfo("NUM_MAPS", getSucceededMaps() + getFailedMaps()
        + getKilledMaps());
    // 计算总Reduce任务数（成功+失败+杀死）
    tEvent.addInfo("NUM_REDUCES", getSucceededReduces() + getFailedReduces()
        + getKilledReduces());
    tEvent.addInfo("FAILED_MAPS", getFailedMaps());
    tEvent.addInfo("FAILED_REDUCES", getFailedReduces());
    tEvent.addInfo("SUCCESSFUL_MAPS", getSucceededMaps());
    tEvent.addInfo("SUCCESSFUL_REDUCES", getSucceededReduces());
    tEvent.addInfo("KILLED_MAPS", getKilledMaps());
    tEvent.addInfo("KILLED_REDUCES", getKilledReduces());
    // TODO replace SUCCEEDED with JobState.SUCCEEDED.toString()
    tEvent.addInfo("JOB_STATUS", "SUCCEEDED");
    return tEvent;
  }

  /**
   * 从作业计数器提取YARN Timeline Service可存储的度量指标集合
   * @return Timeline Service格式的度量指标集合
   */
  @Override
  public Set<TimelineMetric> getTimelineMetrics() {
    // 转换全局计数器为Timeline指标
    Set<TimelineMetric> jobMetrics = JobHistoryEventUtils.
        countersToTimelineMetric(getTotalCounters(), finishTime);
    // 添加Map阶段计数器，添加MAP前缀区分
    jobMetrics.addAll(JobHistoryEventUtils.
        countersToTimelineMetric(getMapCounters(), finishTime, "MAP:"));
    // 添加Reduce阶段计数器，添加REDUCE前缀区分
    jobMetrics.addAll(JobHistoryEventUtils.
        countersToTimelineMetric(getReduceCounters(), finishTime, "REDUCE:"));
    return jobMetrics;
  }
}