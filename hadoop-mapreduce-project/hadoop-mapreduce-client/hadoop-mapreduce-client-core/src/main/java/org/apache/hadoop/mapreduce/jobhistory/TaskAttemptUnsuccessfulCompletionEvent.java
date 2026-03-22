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
import org.apache.hadoop.mapred.ProgressSplitsBlock;
import org.apache.hadoop.mapred.TaskStatus;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.util.JobHistoryEventUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineMetric;
import org.apache.hadoop.yarn.util.SystemClock;

/**
 * 任务尝试失败/被杀死的未成功完成事件，用于作业历史日志记录
 * 记录任务尝试未成功完成的相关信息，供后续作业审计和问题排查使用
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class TaskAttemptUnsuccessfulCompletionEvent implements HistoryEvent {

  private TaskAttemptUnsuccessfulCompletion datum = null;

  private TaskAttemptID attemptId;
  private TaskType taskType;
  private String status;
  private long finishTime;
  private String hostname;
  private int port;
  private String rackName;
  private String error;
  private Counters counters;
  int[][] allSplits;
  int[] clockSplits;
  int[] cpuUsages;
  int[] vMemKbytes;
  int[] physMemKbytes;
  private long startTime;
  private static final Counters EMPTY_COUNTERS = new Counters();

  /**
   * 构造任务尝试未成功完成事件，包含完整信息
   * @param id 任务尝试ID
   * @param taskType 任务类型（MAP/REDUCE等）
   * @param status 任务尝试状态（FAILED/KILLED）
   * @param finishTime 任务尝试完成时间
   * @param hostname 任务尝试执行所在节点的主机名
   * @param port 节点Tracker的RPC端口
   * @param rackName 任务尝试执行所在机架名称
   * @param error 错误信息字符串
   * @param counters 任务尝试的计数器
   * @param allSplits 进度拆分数据，包含运行时长、CPU使用率、内存使用等按进度分段的统计数据
   * @param startTs 任务尝试开始时间，用于写入ATSv2时间线服务
   */
  public TaskAttemptUnsuccessfulCompletionEvent
       (TaskAttemptID id, TaskType taskType,
        String status, long finishTime,
        String hostname, int port, String rackName,
        String error, Counters counters, int[][] allSplits, long startTs) {
    this.attemptId = id;
    this.taskType = taskType;
    this.status = status;
    this.finishTime = finishTime;
    this.hostname = hostname;
    this.port = port;
    this.rackName = rackName;
    this.error = error;
    this.counters = counters;
    this.allSplits = allSplits;
    // 从拆分数据中提取墙钟时间分段
    this.clockSplits =
        ProgressSplitsBlock.arrayGetWallclockTime(allSplits);
    // 从拆分数据中提取CPU使用时间分段
    this.cpuUsages =
        ProgressSplitsBlock.arrayGetCPUTime(allSplits);
    // 从拆分数据中提取虚拟内存使用分段
    this.vMemKbytes =
        ProgressSplitsBlock.arrayGetVMemKbytes(allSplits);
    // 从拆分数据中提取物理内存使用分段
    this.physMemKbytes =
        ProgressSplitsBlock.arrayGetPhysMemKbytes(allSplits);
    this.startTime = startTs;
  }

  /**
   * 构造任务尝试未成功完成事件，自动获取当前时间作为开始时间
   * @param id 任务尝试ID
   * @param taskType 任务类型
   * @param status 任务尝试状态
   * @param finishTime 任务尝试完成时间
   * @param hostname 执行节点主机名
   * @param port 节点Tracker端口
   * @param rackName 执行机架名称
   * @param error 错误信息
   * @param counters 任务计数器
   * @param allSplits 进度拆分统计数据
   */
  public TaskAttemptUnsuccessfulCompletionEvent(TaskAttemptID id,
      TaskType taskType, String status, long finishTime, String hostname,
      int port, String rackName, String error, Counters counters,
      int[][] allSplits) {
    this(id, taskType, status, finishTime, hostname, port, rackName, error,
        counters, allSplits, SystemClock.getInstance().getTime());
  }

  /**
   * @deprecated 请使用包含进度拆分数组参数的构造方法，该重载保留用于向后兼容
   * 构造任务尝试未成功完成事件
   * @param id 任务尝试ID
   * @param taskType 任务类型
   * @param status 任务尝试状态
   * @param finishTime 任务尝试完成时间
   * @param hostname 执行节点主机名
   * @param error 错误信息
   */
  public TaskAttemptUnsuccessfulCompletionEvent
       (TaskAttemptID id, TaskType taskType,
        String status, long finishTime,
        String hostname, String error) {
    this(id, taskType, status, finishTime, hostname, -1, "",
        error, EMPTY_COUNTERS, null);
  }

  /**
   * 构造任务尝试未成功完成事件，使用空计数器
   * @param id 任务尝试ID
   * @param taskType 任务类型
   * @param status 任务尝试状态
   * @param finishTime 任务尝试完成时间
   * @param hostname 执行节点主机名
   * @param port 节点Tracker端口
   * @param rackName 执行机架名称
   * @param error 错误信息
   * @param allSplits 进度拆分统计数据
   */
  public TaskAttemptUnsuccessfulCompletionEvent
      (TaskAttemptID id, TaskType taskType,
       String status, long finishTime,
       String hostname, int port, String rackName,
       String error, int[][] allSplits) {
    this(id, taskType, status, finishTime, hostname, port,
        rackName, error, EMPTY_COUNTERS, allSplits);
  }

  /**
   * 无参构造器，用于反序列化时创建对象
   */
  TaskAttemptUnsuccessfulCompletionEvent() {}

  /**
   * 获取Avro序列化后的数据对象，用于写入作业历史日志
   * @return 序列化后的Avro数据对象
   */
  public Object getDatum() {
    if(datum == null) {
      datum = new TaskAttemptUnsuccessfulCompletion();
      // 设置任务ID
      datum.setTaskid(new Utf8(attemptId.getTaskID().toString()));
      // 设置任务类型
      datum.setTaskType(new Utf8(taskType.name()));
      // 设置任务尝试ID
      datum.setAttemptId(new Utf8(attemptId.toString()));
      // 设置完成时间
      datum.setFinishTime(finishTime);
      // 设置主机名
      datum.setHostname(new Utf8(hostname));
      // 设置机架名（如果存在）
      if (rackName != null) {
        datum.setRackname(new Utf8(rackName));
      }
      // 设置端口
      datum.setPort(port);
      // 设置错误信息
      datum.setError(new Utf8(error));
      // 设置状态
      datum.setStatus(new Utf8(status));
      // 将计数器转换为Avro格式存储
      datum.setCounters(EventWriter.toAvro(counters));
      // 写入墙钟时间分段
      datum.setClockSplits(AvroArrayUtils.toAvro(ProgressSplitsBlock
          .arrayGetWallclockTime(allSplits)));
      // 写入CPU使用率分段
      datum.setCpuUsages(AvroArrayUtils.toAvro(ProgressSplitsBlock
          .arrayGetCPUTime(allSplits)));
      // 写入虚拟内存使用分段
      datum.setVMemKbytes(AvroArrayUtils.toAvro(ProgressSplitsBlock
          .arrayGetVMemKbytes(allSplits)));
      // 写入物理内存使用分段
      datum.setPhysMemKbytes(AvroArrayUtils.toAvro(ProgressSplitsBlock
          .arrayGetPhysMemKbytes(allSplits)));
    }
    return datum;
  }

  /**
   * 从Avro数据对象中反序列化事件信息
   * @param odatum Avro序列化的数据对象
   */
  public void setDatum(Object odatum) {
    this.datum =
        (TaskAttemptUnsuccessfulCompletion)odatum;
    // 解析任务尝试ID
    this.attemptId =
        TaskAttemptID.forName(datum.getAttemptId().toString());
    // 解析任务类型
    this.taskType =
        TaskType.valueOf(datum.getTaskType().toString());
    // 解析完成时间
    this.finishTime = datum.getFinishTime();
    // 解析主机名
    this.hostname = datum.getHostname().toString();
    // 解析机架名
    this.rackName = datum.getRackname().toString();
    // 解析端口
    this.port = datum.getPort();
    // 解析状态
    this.status = datum.getStatus().toString();
    // 解析错误信息
    this.error = datum.getError().toString();
    // 解析计数器
    this.counters =
        EventReader.fromAvro(datum.getCounters());
    // 解析墙钟时间分段
    this.clockSplits =
        AvroArrayUtils.fromAvro(datum.getClockSplits());
    // 解析CPU使用分段
    this.cpuUsages =
        AvroArrayUtils.fromAvro(datum.getCpuUsages());
    // 解析虚拟内存使用分段
    this.vMemKbytes =
        AvroArrayUtils.fromAvro(datum.getVMemKbytes());
    // 解析物理内存使用分段
    this.physMemKbytes =
        AvroArrayUtils.fromAvro(datum.getPhysMemKbytes());
  }

  /**
   * 获取任务ID
   * @return 任务ID
   */
  public TaskID getTaskId() {
    return attemptId.getTaskID();
  }

  /**
   * 获取任务类型
   * @return 任务类型
   */
  public TaskType getTaskType() {
    return TaskType.valueOf(taskType.toString());
  }

  /**
   * 获取任务尝试ID
   * @return 任务尝试ID
   */
  public TaskAttemptID getTaskAttemptId() {
    return attemptId;
  }

  /**
   * 获取任务尝试完成时间
   * @return 完成时间戳
   */
  public long getFinishTime() { return finishTime; }

  /**
   * 获取任务尝试开始时间，用于发布到ATSv2时间线服务
   * @return 任务尝试开始时间戳
   */
  public long getStartTime() {
    return startTime;
  }

  /**
   * 获取执行节点的主机名
   * @return 主机名
   */
  public String getHostname() { return hostname; }

  /**
   * 获取执行节点Tracker的RPC端口
   * @return RPC端口号
   */
  public int getPort() { return port; }

  /**
   * 获取执行节点所在的机架名称
   * @return 机架名称，若未设置则返回null
   */
  public String getRackName() {
    return rackName == null ? null : rackName.toString();
  }

  /**
   * 获取错误信息字符串
   * @return 错误信息
   */
  public String getError() { return error.toString(); }

  /**
   * 获取任务尝试状态
   * @return 任务尝试状态（FAILED/KILLED）
   */
  public String getTaskStatus() {
    return status.toString();
  }

  /**
   * 获取任务尝试的计数器
   * @return 计数器对象
   */
  Counters getCounters() { return counters; }

  /**
   * 获取事件类型，根据任务类型和状态返回对应事件类型
   * @return 作业历史事件类型
   */
  public EventType getEventType() {
    // 判断是否为失败状态（区别于被杀死）
    boolean failed = TaskStatus.State.FAILED.toString().equals(getTaskStatus());
    // 根据任务类型和状态返回对应事件类型
    return getTaskId().getTaskType() == TaskType.MAP
           ? (failed
              ? EventType.MAP_ATTEMPT_FAILED
              : EventType.MAP_ATTEMPT_KILLED)
           : (failed
              ? EventType.REDUCE_ATTEMPT_FAILED
              : EventType.REDUCE_ATTEMPT_KILLED);
  }

  /**
   * 获取墙钟时间分段数据
   * @return 墙钟时间分段数组
   */
  public int[] getClockSplits() {
    return clockSplits;
  }

  /**
   * 获取CPU使用率分段数据
   * @return CPU使用率分段数组
   */
  public int[] getCpuUsages() {
    return cpuUsages;
  }

  /**
   * 获取虚拟内存使用分段数据
   * @return 虚拟内存使用分段数组（单位KB）
   */
  public int[] getVMemKbytes() {
    return vMemKbytes;
  }

  /**
   * 获取物理内存使用分段数据
   * @return 物理内存使用分段数组（单位KB）
   */
  public int[] getPhysMemKbytes() {
    return physMemKbytes;
  }

  @Override
  /**
   * 将当前事件转换为YARN时间线服务事件，用于可视化监控
   * @return 时间线服务事件对象
   */
  public TimelineEvent toTimelineEvent() {
    TimelineEvent tEvent = new TimelineEvent();
    // 设置事件ID为事件类型大写名称
    tEvent.setId(StringUtils.toUpperCase(getEventType().name()));
    // 添加任务类型信息
    tEvent.addInfo("TASK_TYPE", getTaskType().toString());
    // 添加任务尝试ID信息
    tEvent.addInfo("TASK_ATTEMPT_ID", getTaskAttemptId() == null ?
        "" : getTaskAttemptId().toString());
    // 添加完成时间信息
    tEvent.addInfo("FINISH_TIME", getFinishTime());
    // 添加错误信息
    tEvent.addInfo("ERROR", getError());
    // 添加状态信息
    tEvent.addInfo("STATUS", getTaskStatus());
    // 添加主机名信息
    tEvent.addInfo("HOSTNAME", getHostname());
    // 添加端口信息
    tEvent.addInfo("PORT", getPort());
    // 添加机架名信息
    tEvent.addInfo("RACK_NAME", getRackName());
    // 未成功完成时，各阶段完成时间统一填完成时间
    tEvent.addInfo("SHUFFLE_FINISH_TIME", getFinishTime());
    tEvent.addInfo("SORT_FINISH_TIME", getFinishTime());
    tEvent.addInfo("MAP_FINISH_TIME", getFinishTime());
    return tEvent;
  }

  @Override
  /**
   * 从计数器提取转换为YARN时间线服务指标，用于监控展示
   * @return 时间线指标集合
   */
  public Set<TimelineMetric> getTimelineMetrics() {
    Set<TimelineMetric> metrics = JobHistoryEventUtils
        .countersToTimelineMetric(getCounters(), finishTime);
    return metrics;
  }
}