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

import java.util.Collections;
import java.util.Set;

import org.apache.avro.util.Utf8;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric;

import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;

/**
 * 作业未成功完成（失败/被杀死）的作业历史事件，用于记录作业运行结束状态信息
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class JobUnsuccessfulCompletionEvent implements HistoryEvent {
  private static final String NODIAGS = "";
  private static final Iterable<String> NODIAGS_LIST =
      Collections.singletonList(NODIAGS);

  private JobUnsuccessfulCompletion datum
    = new JobUnsuccessfulCompletion();

  /**
   * 创建作业未成功完成事件，不带诊断信息
   * @param id 作业ID
   * @param finishTime 作业结束时间
   * @param succeededMaps 成功完成的Map任务数量
   * @param succeededReduces 成功完成的Reduce任务数量
   * @param failedMaps 失败的Map任务数量
   * @param failedReduces 失败的Reduce任务数量
   * @param killedMaps 被杀死的Map任务数量
   * @param killedReduces 被杀死的Reduce任务数量
   * @param status 作业最终状态
   */
  public JobUnsuccessfulCompletionEvent(JobID id, long finishTime,
      int succeededMaps,
      int succeededReduces,
      int failedMaps,
      int failedReduces,
      int killedMaps,
      int killedReduces,
      String status) {
    this(id, finishTime, succeededMaps, succeededReduces, failedMaps,
            failedReduces, killedMaps, killedReduces, status, NODIAGS_LIST);
  }

  /**
   * 创建作业未成功完成事件，带诊断信息
   * @param id 作业ID
   * @param finishTime 作业结束时间
   * @param succeededMaps 成功完成的Map任务数量
   * @param succeededReduces 成功完成的Reduce任务数量
   * @param failedMaps 失败的Map任务数量
   * @param failedReduces 失败的Reduce任务数量
   * @param killedMaps 被杀死的Map任务数量
   * @param killedReduces 被杀死的Reduce任务数量
   * @param status 作业最终状态
   * @param diagnostics 作业运行时诊断信息列表
   */
  public JobUnsuccessfulCompletionEvent(JobID id, long finishTime,
      int succeededMaps,
      int succeededReduces,
      int failedMaps,
      int failedReduces,
      int killedMaps,
      int killedReduces,
      String status,
      Iterable<String> diagnostics) {
    // 设置作业ID
    datum.setJobid(new Utf8(id.toString()));
    // 设置作业结束时间
    datum.setFinishTime(finishTime);
    // 为保持向后兼容性，Avro schema中仍使用finishedMaps和finishedReduces字段名
    datum.setFinishedMaps(succeededMaps);
    datum.setFinishedReduces(succeededReduces);
    // 设置失败任务数量
    datum.setFailedMaps(failedMaps);
    datum.setFailedReduces(failedReduces);
    // 设置被杀死任务数量
    datum.setKilledMaps(killedMaps);
    datum.setKilledReduces(killedReduces);
    // 设置作业状态
    datum.setJobStatus(new Utf8(status));
    // 诊断信息为空时使用默认空值
    if (diagnostics == null) {
      diagnostics = NODIAGS_LIST;
    }
    // 将诊断信息按换行符拼接后存储
    datum.setDiagnostics(new Utf8(Joiner.on('\n').skipNulls()
        .join(diagnostics)));
  }

  JobUnsuccessfulCompletionEvent() {}

  public Object getDatum() { return datum; }
  public void setDatum(Object datum) {
    this.datum = (JobUnsuccessfulCompletion)datum;
  }

  /** 获取作业ID */
  public JobID getJobId() {
    return JobID.forName(datum.getJobid().toString());
  }
  /** 获取作业结束时间 */
  public long getFinishTime() { return datum.getFinishTime(); }
  /** 获取成功完成的Map任务数量 */
  public int getSucceededMaps() { return datum.getFinishedMaps(); }
  /** 获取成功完成的Reduce任务数量 */
  public int getSucceededReduces() { return datum.getFinishedReduces(); }
  /** 获取失败的Map任务数量 */
  public int getFailedMaps() { return datum.getFailedMaps(); }
  /** 获取失败的Reduce任务数量 */
  public int getFailedReduces() { return datum.getFailedReduces(); }
  /** 获取被杀死的Map任务数量 */
  public int getKilledMaps() { return datum.getKilledMaps(); }
  /** 获取被杀死的Reduce任务数量 */
  public int getKilledReduces() { return datum.getKilledReduces(); }

  /** 获取作业最终状态 */
  public String getStatus() { return datum.getJobStatus().toString(); }
  /** 获取事件类型 */
  public EventType getEventType() {
    if ("FAILED".equals(getStatus())) {
      return EventType.JOB_FAILED;
    } else if ("ERROR".equals(getStatus())) {
      return EventType.JOB_ERROR;
    } else
      return EventType.JOB_KILLED;
  }

  /**
   * 获取保存在历史文件中的诊断信息
   *
   * @return 作业结束时的诊断信息文本
   */
  public String getDiagnostics() {
    final CharSequence diagnostics = datum.getDiagnostics();
    return diagnostics == null ? NODIAGS : diagnostics.toString();
  }

  @Override
  /** 将当前事件转换为时间轴服务可存储的TimelineEvent */
  public TimelineEvent toTimelineEvent() {
    TimelineEvent tEvent = new TimelineEvent();
    // 设置事件ID为事件类型大写名称
    tEvent.setId(StringUtils.toUpperCase(getEventType().name()));
    // 添加作业结束时间信息
    tEvent.addInfo("FINISH_TIME", getFinishTime());
    // 添加总Map任务数信息
    tEvent.addInfo("NUM_MAPS", getSucceededMaps() + getFailedMaps()
        + getKilledMaps());
    // 添加总Reduce任务数信息
    tEvent.addInfo("NUM_REDUCES", getSucceededReduces() + getFailedReduces()
        + getKilledReduces());
    // 添加作业状态信息
    tEvent.addInfo("JOB_STATUS", getStatus());
    // 添加诊断信息
    tEvent.addInfo("DIAGNOSTICS", getDiagnostics());
    // 添加各状态任务数量明细
    tEvent.addInfo("SUCCESSFUL_MAPS", getSucceededMaps());
    tEvent.addInfo("SUCCESSFUL_REDUCES", getSucceededReduces());
    tEvent.addInfo("FAILED_MAPS", getFailedMaps());
    tEvent.addInfo("FAILED_REDUCES", getFailedReduces());
    tEvent.addInfo("KILLED_MAPS", getKilledMaps());
    tEvent.addInfo("KILLED_REDUCES", getKilledReduces());

    return tEvent;
  }

  @Override
  /** 获取事件对应的时间轴指标，当前事件无指标返回null */
  public Set<TimelineMetric> getTimelineMetrics() {
    return null;
  }
}