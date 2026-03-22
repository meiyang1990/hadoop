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
 * Reduce尝试执行完成事件，用于记录MapReduce作业中一个Reduce尝试执行完成的相关信息，写入作业历史日志
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class ReduceAttemptFinishedEvent implements HistoryEvent {

  private ReduceAttemptFinished datum = null;

  private TaskAttemptID attemptId;
  private TaskType taskType;
  private String taskStatus;
  private long shuffleFinishTime;
  private long sortFinishTime;
  private long finishTime;
  private String hostname;
  private String rackName;
  private int port;
  private String state;
  private Counters counters;
  int[][] allSplits;
  int[] clockSplits;
  int[] cpuUsages;
  int[] vMemKbytes;
  int[] physMemKbytes;
  private long startTime;

  /**
   * 构造Reduce尝试完成事件，初始化所有必要信息
   * @param id 尝试ID
   * @param taskType 任务类型
   * @param taskStatus 任务状态
   * @param shuffleFinishTime shuffle阶段完成时间戳
   * @param sortFinishTime sort阶段完成时间戳
   * @param finishTime Reduce尝试整体完成时间戳
   * @param hostname 执行尝试的节点主机名
   * @param port 节点Tracker的RPC端口
   * @param rackName 执行尝试的节点所在机架名
   * @param state Reduce尝试执行状态
   * @param counters 尝试的计数器信息
   * @param allSplits 进度分块数据，记录各进度点的资源使用情况，包含墙钟时间、CPU时间、虚拟内存、物理内存
   * @param startTs 任务尝试开始时间戳，用于写入时间线服务ATSv2
   */
  public ReduceAttemptFinishedEvent(TaskAttemptID id, TaskType taskType,
      String taskStatus, long shuffleFinishTime, long sortFinishTime,
      long finishTime, String hostname, int port,  String rackName,
      String state, Counters counters, int[][] allSplits, long startTs) {
    this.attemptId = id;
    this.taskType = taskType;
    this.taskStatus = taskStatus;
    this.shuffleFinishTime = shuffleFinishTime;
    this.sortFinishTime = sortFinishTime;
    this.finishTime = finishTime;
    this.hostname = hostname;
    this.rackName = rackName;
    this.port = port;
    this.state = state;
    this.counters = counters;
    this.allSplits = allSplits;
    // 从全量分块中提取墙钟时间分块
    this.clockSplits = ProgressSplitsBlock.arrayGetWallclockTime(allSplits);
    // 从全量分块中提取CPU使用时间分块
    this.cpuUsages = ProgressSplitsBlock.arrayGetCPUTime(allSplits);
    // 从全量分块中提取虚拟内存使用分块
    this.vMemKbytes = ProgressSplitsBlock.arrayGetVMemKbytes(allSplits);
    // 从全量分块中提取物理内存使用分块
    this.physMemKbytes = ProgressSplitsBlock.arrayGetPhysMemKbytes(allSplits);
    this.startTime = startTs;
  }

  /**
   * 构造Reduce尝试完成事件，使用系统当前时间作为开始时间戳
   * @param id 尝试ID
   * @param taskType 任务类型
   * @param taskStatus 任务状态
   * @param shuffleFinishTime shuffle阶段完成时间戳
   * @param sortFinishTime sort阶段完成时间戳
   * @param finishTime Reduce尝试整体完成时间戳
   * @param hostname 执行尝试的节点主机名
   * @param port 节点Tracker的RPC端口
   * @param rackName 执行尝试的节点所在机架名
   * @param state Reduce尝试执行状态
   * @param counters 尝试的计数器信息
   * @param allSplits 进度分块数据，记录各进度点的资源使用情况
   */
  public ReduceAttemptFinishedEvent(TaskAttemptID id, TaskType taskType,
      String taskStatus, long shuffleFinishTime, long sortFinishTime,
      long finishTime, String hostname, int port,  String rackName,
      String state, Counters counters, int[][] allSplits) {
    this(id, taskType, taskStatus, shuffleFinishTime, sortFinishTime,
        finishTime, hostname, port, rackName, state, counters, allSplits,
        SystemClock.getInstance().getTime());
  }

  /**
   * @deprecated 请使用带进度分块参数的构造方法，参考{@link org.apache.hadoop.mapred.ProgressSplitsBlock}了解参数含义
   *
   * 构造旧版Reduce尝试完成事件，不包含进度分块信息
   * @param id 尝试ID
   * @param taskType 任务类型
   * @param taskStatus 任务状态
   * @param shuffleFinishTime shuffle阶段完成时间戳
   * @param sortFinishTime sort阶段完成时间戳
   * @param finishTime Reduce尝试整体完成时间戳
   * @param hostname 执行尝试的节点主机名
   * @param state Reduce尝试执行状态
   * @param counters 尝试的计数器信息
   */
  public ReduceAttemptFinishedEvent(TaskAttemptID id, TaskType taskType,
      String taskStatus, long shuffleFinishTime, long sortFinishTime,
      long finishTime, String hostname, String state, Counters counters) {
    this(id, taskType, taskStatus,
        shuffleFinishTime, sortFinishTime, finishTime,
        hostname, -1, "", state, counters, null);
  }

  /**
   * 无参构造器，用于反序列化时创建空对象
   */
  ReduceAttemptFinishedEvent() {}

  /**
   * 获取Avro序列化后的事件数据对象，用于写入作业历史日志
   * @return 填充完成的Avro ReduceAttemptFinished对象
   */
  public Object getDatum() {
    if (datum == null) {
      datum = new ReduceAttemptFinished();
      // 设置任务ID
      datum.setTaskid(new Utf8(attemptId.getTaskID().toString()));
      // 设置尝试ID
      datum.setAttemptId(new Utf8(attemptId.toString()));
      // 设置任务类型
      datum.setTaskType(new Utf8(taskType.name()));
      // 设置任务状态
      datum.setTaskStatus(new Utf8(taskStatus));
      // 设置shuffle完成时间
      datum.setShuffleFinishTime(shuffleFinishTime);
      // 设置sort完成时间
      datum.setSortFinishTime(sortFinishTime);
      // 设置尝试整体完成时间
      datum.setFinishTime(finishTime);
      // 设置执行节点主机名
      datum.setHostname(new Utf8(hostname));
      // 设置RPC端口
      datum.setPort(port);
      // 设置机架名，不为空时才写入
      if (rackName != null) {
        datum.setRackname(new Utf8(rackName));
      }
      // 设置执行状态
      datum.setState(new Utf8(state));
      // 将Hadoop计数器转换为Avro格式并设置
      datum.setCounters(EventWriter.toAvro(counters));

      // 写入墙钟时间分块数据
      datum.setClockSplits(AvroArrayUtils.toAvro(ProgressSplitsBlock
          .arrayGetWallclockTime(allSplits)));
      // 写入CPU使用分块数据
      datum.setCpuUsages(AvroArrayUtils.toAvro(ProgressSplitsBlock
          .arrayGetCPUTime(allSplits)));
      // 写入虚拟内存使用分块数据
      datum.setVMemKbytes(AvroArrayUtils.toAvro(ProgressSplitsBlock
          .arrayGetVMemKbytes(allSplits)));
      // 写入物理内存使用分块数据
      datum.setPhysMemKbytes(AvroArrayUtils.toAvro(ProgressSplitsBlock
          .arrayGetPhysMemKbytes(allSplits)));
    }
    return datum;
  }

  /**
   * 从Avro序列化对象中反序列化，填充事件信息
   * @param oDatum Avro序列化的ReduceAttemptFinished对象
   */
  public void setDatum(Object oDatum) {
    this.datum = (ReduceAttemptFinished)oDatum;
    // 从字符串解析得到尝试ID
    this.attemptId = TaskAttemptID.forName(datum.getAttemptId().toString());
    // 解析任务类型
    this.taskType = TaskType.valueOf(datum.getTaskType().toString());
    // 获取任务状态
    this.taskStatus = datum.getTaskStatus().toString();
    // 获取shuffle完成时间
    this.shuffleFinishTime = datum.getShuffleFinishTime();
    // 获取sort完成时间
    this.sortFinishTime = datum.getSortFinishTime();
    // 获取尝试整体完成时间
    this.finishTime = datum.getFinishTime();
    // 获取执行节点主机名
    this.hostname = datum.getHostname().toString();
    // 获取机架名
    this.rackName = datum.getRackname().toString();
    // 获取RPC端口
    this.port = datum.getPort();
    // 获取执行状态
    this.state = datum.getState().toString();
    // 从Avro格式转换得到Hadoop计数器
    this.counters = EventReader.fromAvro(datum.getCounters());
    // 从Avro数组转换得到墙钟时间分块
    this.clockSplits = AvroArrayUtils.fromAvro(datum.getClockSplits());
    // 从Avro数组转换得到CPU使用分块
    this.cpuUsages = AvroArrayUtils.fromAvro(datum.getCpuUsages());
    // 从Avro数组转换得到虚拟内存使用分块
    this.vMemKbytes = AvroArrayUtils.fromAvro(datum.getVMemKbytes());
    // 从Avro数组转换得到物理内存使用分块
    this.physMemKbytes = AvroArrayUtils.fromAvro(datum.getPhysMemKbytes());
  }

  /** Gets the Task ID. */
  public TaskID getTaskId() { return attemptId.getTaskID(); }
  /** Gets the attempt id. */
  public TaskAttemptID getAttemptId() {
    return attemptId;
  }
  /** Gets the task type. */
  public TaskType getTaskType() {
    return taskType;
  }
  /** Gets the task status. */
  public String getTaskStatus() { return taskStatus.toString(); }
  /** Gets the finish time of the sort phase. */
  public long getSortFinishTime() { return sortFinishTime; }
  /** Gets the finish time of the shuffle phase. */
  public long getShuffleFinishTime() { return shuffleFinishTime; }
  /** Gets the finish time of the attempt. */
  public long getFinishTime() { return finishTime; }
  /**
   * Gets the start time.
   * @return task attempt start time.
   */
  public long getStartTime() {
    return startTime;
  }
  /** Gets the name of the host where the attempt ran. */
  public String getHostname() { return hostname.toString(); }
  /** Gets the tracker rpc port. */
  public int getPort() { return port; }
  
  /** Gets the rack name of the node where the attempt ran. */
  public String getRackName() {
    return rackName == null ? null : rackName.toString();
  }
  /**
   * Gets the state string.
   * @return reduce attempt state
   */
  public String getState() {
    return state.toString();
  }
  /**
   * Gets the counters.
   * @return counters
   */
  Counters getCounters() {
    return counters;
  }
  /** Gets the event type. */
  public EventType getEventType() {
    return EventType.REDUCE_ATTEMPT_FINISHED;
  }


  public int[] getClockSplits() {
    return clockSplits;
  }
  public int[] getCpuUsages() {
    return cpuUsages;
  }
  public int[] getVMemKbytes() {
    return vMemKbytes;
  }
  public int[] getPhysMemKbytes() {
    return physMemKbytes;
  }

  /**
   * 将当前事件转换为YARN时间线服务 TimelineEvent 对象，用于写入时间线服务
   * @return 填充完成的TimelineEvent对象
   */
  @Override
  public TimelineEvent toTimelineEvent() {
    TimelineEvent tEvent = new TimelineEvent();
    // 设置事件ID
    tEvent.setId(StringUtils.toUpperCase(getEventType().name()));
    // 添加任务类型信息
    tEvent.addInfo("TASK_TYPE", getTaskType().toString());
    // 添加尝试ID信息
    tEvent.addInfo("ATTEMPT_ID", getAttemptId() == null ?
        "" : getAttemptId().toString());
    // 添加完成时间信息
    tEvent.addInfo("FINISH_TIME", getFinishTime());
    // 添加任务状态信息
    tEvent.addInfo("STATUS", getTaskStatus());
    // 添加执行状态信息
    tEvent.addInfo("STATE", getState());
    // 添加shuffle完成时间信息
    tEvent.addInfo("SHUFFLE_FINISH_TIME", getShuffleFinishTime());
    // 添加sort完成时间信息
    tEvent.addInfo("SORT_FINISH_TIME", getSortFinishTime());
    // 添加执行节点主机名信息
    tEvent.addInfo("HOSTNAME", getHostname());
    // 添加RPC端口信息
    tEvent.addInfo("PORT", getPort());
    // 添加机架名信息
    tEvent.addInfo("RACK_NAME", getRackName());
    return tEvent;
  }

  /**
   * 将当前事件的计数器转换为YARN时间线服务TimelineMetric集合，用于指标展示
   * @return 转换后的时间线指标集合
   */
  @Override
  public Set<TimelineMetric> getTimelineMetrics() {
    Set<TimelineMetric> metrics = JobHistoryEventUtils
        .countersToTimelineMetric(getCounters(), finishTime);
    return metrics;
  }

}