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
 * MapReduce任务中Map尝试运行完成事件，记录Map尝试运行结束相关信息用于作业历史日志
 * 负责存储Map尝试完成后的状态、时间、资源使用等元数据，支持Avro序列化和时间线服务导出
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class MapAttemptFinishedEvent implements HistoryEvent {

  private MapAttemptFinished datum = null;

  private TaskAttemptID attemptId;
  private TaskType taskType;
  private String taskStatus;
  private long finishTime;
  private String hostname;
  private String rackName;
  private int port;
  private long mapFinishTime;
  private String state;
  private Counters counters;
  int[][] allSplits;
  int[] clockSplits;
  int[] cpuUsages;
  int[] vMemKbytes;
  int[] physMemKbytes;
  private long startTime;

  /** 
   * 构造Map尝试完成事件，初始化所有事件字段，拆分进度分片数据
   * @param id 任务尝试ID
   * @param taskType 任务类型
   * @param taskStatus 任务状态
   * @param mapFinishTime Map阶段完成时间戳
   * @param finishTime 尝试完成时间戳
   * @param hostname 执行尝试的节点主机名
   * @param port 节点追踪器RPC端口
   * @param rackName 执行尝试的机架名称
   * @param state 尝试状态字符串
   * @param counters 尝试的计数器集合
   * @param allSplits 进度分片数据，包含墙钟时间、CPU时间、虚拟内存、物理内存的进度分布，无数据则传null
   * @param startTs 任务开始时间戳，用于写入ATSv2时间线服务
   */
  public MapAttemptFinishedEvent(TaskAttemptID id, TaskType taskType,
      String taskStatus, long mapFinishTime, long finishTime, String hostname,
      int port, String rackName, String state, Counters counters,
      int[][] allSplits, long startTs) {
    this.attemptId = id;
    this.taskType = taskType;
    this.taskStatus = taskStatus;
    this.mapFinishTime = mapFinishTime;
    this.finishTime = finishTime;
    this.hostname = hostname;
    this.rackName = rackName;
    this.port = port;
    this.state = state;
    this.counters = counters;
    this.allSplits = allSplits;
    // 从分片数组中提取墙钟时间分片
    this.clockSplits = ProgressSplitsBlock.arrayGetWallclockTime(allSplits);
    // 从分片数组中提取CPU使用时间分片
    this.cpuUsages = ProgressSplitsBlock.arrayGetCPUTime(allSplits);
    // 从分片数组中提取虚拟内存使用分片
    this.vMemKbytes = ProgressSplitsBlock.arrayGetVMemKbytes(allSplits);
    // 从分片数组中提取物理内存使用分片
    this.physMemKbytes = ProgressSplitsBlock.arrayGetPhysMemKbytes(allSplits);
    this.startTime = startTs;
  }

  /**
   * 简化构造函数，自动获取当前系统时间作为开始时间
   */
  public MapAttemptFinishedEvent(TaskAttemptID id, TaskType taskType,
      String taskStatus, long mapFinishTime, long finishTime, String hostname,
      int port, String rackName, String state, Counters counters,
      int[][] allSplits) {
    this(id, taskType, taskStatus, mapFinishTime, finishTime, hostname, port,
        rackName, state, counters, allSplits,
        SystemClock.getInstance().getTime());
  }

  /** 
   * @deprecated 请使用带进度分片参数的新构造函数，该构造函数留作向后兼容
   *
   * 创建Map尝试完成事件
   * @param id Task Attempt ID
   * @param taskType Type of the task
   * @param taskStatus Status of the task
   * @param mapFinishTime Finish time of the map phase
   * @param finishTime Finish time of the attempt
   * @param hostname Name of the host where the map executed
   * @param state State string for the attempt
   * @param counters Counters for the attempt
   */
  @Deprecated
  public MapAttemptFinishedEvent(TaskAttemptID id, TaskType taskType,
      String taskStatus, long mapFinishTime, long finishTime, String hostname,
      String state, Counters counters) {
    this(id, taskType, taskStatus, mapFinishTime, finishTime, hostname, -1, "",
        state, counters, null);
  }

  /**
   * 无参构造函数，供Avro反序列化使用
   */
  MapAttemptFinishedEvent() {}

  /**
   * 获取Avro序列化数据对象，延迟初始化并填充所有字段
   * @return 填充完成的Avro MapAttemptFinished对象
   */
  public Object getDatum() {
    if (datum == null) {
      datum = new MapAttemptFinished();
      datum.setTaskid(new Utf8(attemptId.getTaskID().toString()));
      datum.setAttemptId(new Utf8(attemptId.toString()));
      datum.setTaskType(new Utf8(taskType.name()));
      datum.setTaskStatus(new Utf8(taskStatus));
      datum.setMapFinishTime(mapFinishTime);
      datum.setFinishTime(finishTime);
      datum.setHostname(new Utf8(hostname));
      datum.setPort(port);
      if (rackName != null) {
        datum.setRackname(new Utf8(rackName));
      }
      datum.setState(new Utf8(state));
      datum.setCounters(EventWriter.toAvro(counters));

      // 将墙钟时间分片转换为Avro数组格式
      datum.setClockSplits(AvroArrayUtils.toAvro(ProgressSplitsBlock
          .arrayGetWallclockTime(allSplits)));
      // 将CPU使用分片转换为Avro数组格式
      datum.setCpuUsages(AvroArrayUtils.toAvro(ProgressSplitsBlock
          .arrayGetCPUTime(allSplits)));
      // 将虚拟内存分片转换为Avro数组格式
      datum.setVMemKbytes(AvroArrayUtils.toAvro(ProgressSplitsBlock
          .arrayGetVMemKbytes(allSplits)));
      // 将物理内存分片转换为Avro数组格式
      datum.setPhysMemKbytes(AvroArrayUtils.toAvro(ProgressSplitsBlock
          .arrayGetPhysMemKbytes(allSplits)));
    }
    return datum;
  }

  /**
   * 从Avro对象反序列化事件数据，解析并填充所有字段
   * @param oDatum Avro序列化的MapAttemptFinished对象
   */
  public void setDatum(Object oDatum) {
    this.datum = (MapAttemptFinished)oDatum;
    this.attemptId = TaskAttemptID.forName(datum.getAttemptId().toString());
    this.taskType = TaskType.valueOf(datum.getTaskType().toString());
    this.taskStatus = datum.getTaskStatus().toString();
    this.mapFinishTime = datum.getMapFinishTime();
    this.finishTime = datum.getFinishTime();
    this.hostname = datum.getHostname().toString();
    this.rackName = datum.getRackname().toString();
    this.port = datum.getPort();
    this.state = datum.getState().toString();
    this.counters = EventReader.fromAvro(datum.getCounters());
    this.clockSplits = AvroArrayUtils.fromAvro(datum.getClockSplits());
    this.cpuUsages = AvroArrayUtils.fromAvro(datum.getCpuUsages());
    this.vMemKbytes = AvroArrayUtils.fromAvro(datum.getVMemKbytes());
    this.physMemKbytes = AvroArrayUtils.fromAvro(datum.getPhysMemKbytes());
  }

  /** Gets the task ID. */
  public TaskID getTaskId() {
    return attemptId.getTaskID();
  }
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
  /** Gets the map phase finish time. */
  public long getMapFinishTime() { return mapFinishTime; }
  /** Gets the attempt finish time. */
  public long getFinishTime() { return finishTime; }
  /**
   * Gets the task attempt start time.
   * @return task attempt start time.
   */
  public long getStartTime() {
    return startTime;
  }
  /** Gets the host name. */
  public String getHostname() { return hostname.toString(); }
  /** Gets the tracker rpc port. */
  public int getPort() { return port; }
  
  /** Gets the rack name. */
  public String getRackName() {
    return rackName == null ? null : rackName.toString();
  }
  /**
   * Gets the attempt state string.
   * @return map attempt state
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
    return EventType.MAP_ATTEMPT_FINISHED;
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
  
  @Override
  /**
   * 将当前事件转换为YARN时间线服务可识别的TimelineEvent对象，填充所有事件元信息
   * @return 填充完成的TimelineEvent对象
   */
  public TimelineEvent toTimelineEvent() {
    TimelineEvent tEvent = new TimelineEvent();
    tEvent.setId(StringUtils.toUpperCase(getEventType().name()));
    tEvent.addInfo("TASK_TYPE", getTaskType().toString());
    tEvent.addInfo("FINISH_TIME", getFinishTime());
    tEvent.addInfo("STATUS", getTaskStatus());
    tEvent.addInfo("STATE", getState());
    tEvent.addInfo("MAP_FINISH_TIME", getMapFinishTime());
    tEvent.addInfo("HOSTNAME", getHostname());
    tEvent.addInfo("PORT", getPort());
    tEvent.addInfo("RACK_NAME", getRackName());
    tEvent.addInfo("ATTEMPT_ID", getAttemptId() == null ?
        "" : getAttemptId().toString());
    return tEvent;
  }

  @Override
  /**
   * 将事件中的计数器转换为YARN时间线服务可识别的指标集合
   * @return 转换完成的TimelineMetric集合
   */
  public Set<TimelineMetric> getTimelineMetrics() {
    Set<TimelineMetric> metrics = JobHistoryEventUtils
        .countersToTimelineMetric(getCounters(), finishTime);
    return metrics;
  }

}