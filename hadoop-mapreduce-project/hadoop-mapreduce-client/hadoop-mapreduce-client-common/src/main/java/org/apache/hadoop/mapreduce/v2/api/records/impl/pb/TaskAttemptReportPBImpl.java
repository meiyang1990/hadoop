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

package org.apache.hadoop.mapreduce.v2.api.records.impl.pb;

import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.v2.api.records.Counters;
import org.apache.hadoop.mapreduce.v2.api.records.Phase;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptReport;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptState;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.CountersProto;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.PhaseProto;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.TaskAttemptIdProto;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.TaskAttemptReportProto;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.TaskAttemptReportProtoOrBuilder;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.TaskAttemptStateProto;
import org.apache.hadoop.mapreduce.v2.util.MRProtoUtils;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoBase;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerIdProto;

/**
 * TaskAttemptReport的Protobuf实现类，基于Protobuf序列化协议，
 * 封装MapReduce任务尝试的运行状态报告，在客户端和服务端之间传输任务尝试信息。
 */
public class TaskAttemptReportPBImpl extends ProtoBase<TaskAttemptReportProto> implements TaskAttemptReport {
  // 存储序列化后的Protobuf对象
  TaskAttemptReportProto proto = TaskAttemptReportProto.getDefaultInstance();
  // 用于构建Protobuf对象的Builder
  TaskAttemptReportProto.Builder builder = null;
  // 标记当前是否通过Protobuf对象直接读取数据
  boolean viaProto = false;

  // 缓存反序列化后的任务尝试ID对象
  private TaskAttemptId taskAttemptId = null;
  // 缓存反序列化后的计数器对象
  private Counters counters = null;
  // 缓存旧版本API的计数器对象
  private org.apache.hadoop.mapreduce.Counters rawCounters = null;
  // 缓存反序列化后的容器ID对象
  private ContainerId containerId = null;

  /**
   * 构造函数，初始化空的Builder用于构建任务尝试报告
   */
  public TaskAttemptReportPBImpl() {
    builder = TaskAttemptReportProto.newBuilder();
  }

  /**
   * 基于已有的Protobuf对象构造任务尝试报告
   * @param proto 已序列化的任务尝试报告Protobuf对象
   */
  public TaskAttemptReportPBImpl(TaskAttemptReportProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  @Override
  public TaskAttemptReportProto getProto() {
      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  /**
   * 将本地缓存的Java对象合并到Protobuf Builder中
   */
  private void mergeLocalToBuilder() {
    if (this.taskAttemptId != null) {
      builder.setTaskAttemptId(convertToProtoFormat(this.taskAttemptId));
    }
    convertRawCountersToCounters();
    if (this.counters != null) {
      builder.setCounters(convertToProtoFormat(this.counters));
    }
    if (this.containerId != null) {
      builder.setContainerId(convertToProtoFormat(this.containerId));
    }
  }

  /**
   * 将本地修改合并到Protobuf对象中，完成序列化
   */
  private void mergeLocalToProto() {
    if (viaProto) 
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  /**
   * 如果当前使用Protobuf对象存储，初始化Builder用于修改
   */
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = TaskAttemptReportProto.newBuilder(proto);
    }
    viaProto = false;
  }


  @Override
  public Counters getCounters() {
    TaskAttemptReportProtoOrBuilder p = viaProto ? proto : builder;
    convertRawCountersToCounters();
    if (this.counters != null) {
      return this.counters;
    }
    if (!p.hasCounters()) {
      return null;
    }
    this.counters = convertFromProtoFormat(p.getCounters());
    return this.counters;
  }

  @Override
  public void setCounters(Counters counters) {
    maybeInitBuilder();
    if (counters == null) {
      builder.clearCounters();
    }
    this.counters = counters;
    this.rawCounters = null;
  }

  @Override
  public org.apache.hadoop.mapreduce.Counters
        getRawCounters() {
    return this.rawCounters;
  }

  @Override
  public void setRawCounters(org.apache.hadoop.mapreduce.Counters rCounters) {
    setCounters(null);
    this.rawCounters = rCounters;
  }

  /**
   * 将旧版本API的原始计数器转换为YARN版本的计数器对象
   */
  private void convertRawCountersToCounters() {
    if (this.counters == null && this.rawCounters != null) {
      this.counters = TypeConverter.toYarn(rawCounters);
      this.rawCounters = null;
    }
  }

  @Override
  public long getStartTime() {
    TaskAttemptReportProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getStartTime());
  }

  @Override
  public void setStartTime(long startTime) {
    maybeInitBuilder();
    builder.setStartTime((startTime));
  }

  @Override
  public long getFinishTime() {
    TaskAttemptReportProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getFinishTime());
  }

  @Override
  public void setFinishTime(long finishTime) {
    maybeInitBuilder();
    builder.setFinishTime((finishTime));
  }
  
  @Override
  public long getShuffleFinishTime() {
    TaskAttemptReportProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getShuffleFinishTime());
  }

  @Override
  public void setShuffleFinishTime(long time) {
    maybeInitBuilder();
    builder.setShuffleFinishTime(time);
  }

  @Override
  public long getSortFinishTime() {
    TaskAttemptReportProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getSortFinishTime());
  }

  @Override
  public void setSortFinishTime(long time) {
    maybeInitBuilder();
    builder.setSortFinishTime(time);
  }

  @Override
  public TaskAttemptId getTaskAttemptId() {
    TaskAttemptReportProtoOrBuilder p = viaProto ? proto : builder;
    if (this.taskAttemptId != null) {
      return this.taskAttemptId;
    }
    if (!p.hasTaskAttemptId()) {
      return null;
    }
    this.taskAttemptId = convertFromProtoFormat(p.getTaskAttemptId());
    return this.taskAttemptId;
  }

  @Override
  public void setTaskAttemptId(TaskAttemptId taskAttemptId) {
    maybeInitBuilder();
    if (taskAttemptId == null) 
      builder.clearTaskAttemptId();
    this.taskAttemptId = taskAttemptId;
  }

  @Override
  public TaskAttemptState getTaskAttemptState() {
    TaskAttemptReportProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasTaskAttemptState()) {
      return null;
    }
    return convertFromProtoFormat(p.getTaskAttemptState());
  }

  @Override
  public void setTaskAttemptState(TaskAttemptState taskAttemptState) {
    maybeInitBuilder();
    if (taskAttemptState == null) {
      builder.clearTaskAttemptState();
      return;
    }
    builder.setTaskAttemptState(convertToProtoFormat(taskAttemptState));
  }

  @Override
  public float getProgress() {
    TaskAttemptReportProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getProgress());
  }

  @Override
  public void setProgress(float progress) {
    maybeInitBuilder();
    builder.setProgress((progress));
  }

  @Override
  public String getDiagnosticInfo() {
    TaskAttemptReportProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasDiagnosticInfo()) {
      return null;
    }
    return (p.getDiagnosticInfo());
  }

  @Override
  public void setDiagnosticInfo(String diagnosticInfo) {
    maybeInitBuilder();
    if (diagnosticInfo == null) {
      builder.clearDiagnosticInfo();
      return;
    }
    builder.setDiagnosticInfo((diagnosticInfo));
  }

  @Override
  public String getStateString() {
    TaskAttemptReportProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasStateString()) {
      return null;
    }
    return (p.getStateString());
  }

  @Override
  public void setStateString(String stateString) {
    maybeInitBuilder();
    if (stateString == null) {
      builder.clearStateString();
      return;
    }
    builder.setStateString((stateString));
  }

  @Override
  public Phase getPhase() {
    TaskAttemptReportProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasPhase()) {
      return null;
    }
    return convertFromProtoFormat(p.getPhase());
  }

  @Override
  public void setPhase(Phase phase) {
    maybeInitBuilder();
    if (phase == null) {
      builder.clearPhase();
      return;
    }
    builder.setPhase(convertToProtoFormat(phase));
  }
  
  @Override
  public String getNodeManagerHost() {
    TaskAttemptReportProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasNodeManagerHost()) {
      return null;
    }
    return p.getNodeManagerHost();
  }
  
  @Override
  public void setNodeManagerHost(String nmHost) {
    maybeInitBuilder();
    if (nmHost == null) {
      builder.clearNodeManagerHost();
      return;
    }
    builder.setNodeManagerHost(nmHost);
  }
  
  @Override
  public int getNodeManagerPort() {
    TaskAttemptReportProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getNodeManagerPort());
  }
  
  @Override
  public void setNodeManagerPort(int nmPort) {
    maybeInitBuilder();
    builder.setNodeManagerPort(nmPort);
  }
  
  @Override
  public int getNodeManagerHttpPort() {
    TaskAttemptReportProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getNodeManagerHttpPort());
  }
  
  @Override
  public void setNodeManagerHttpPort(int nmHttpPort) {
    maybeInitBuilder();
    builder.setNodeManagerHttpPort(nmHttpPort);
  }
  
  @Override
  public ContainerId getContainerId() {
    TaskAttemptReportProtoOrBuilder p = viaProto ? proto : builder;
    if (containerId != null) {
      return containerId;
    } // Else via proto
    if (!p.hasContainerId()) {
      return null;
    }
    containerId = convertFromProtoFormat(p.getContainerId());
    return containerId;
  }

  @Override
  public void setContainerId(ContainerId containerId) {
    maybeInitBuilder();
    if (containerId == null) {
      builder.clearContainerId();
    }
    this.containerId = containerId;
  }

  /**
   * 将ContainerId对象转换为Protobuf格式
   * @param t 容器ID对象
   * @return Protobuf格式的容器ID
   */
  private ContainerIdProto convertToProtoFormat(ContainerId t) {
    return ((ContainerIdPBImpl)t).getProto();
  }
  
  /**
   * 将Protobuf格式的容器ID转换为Java对象
   * @param p Protobuf格式的容器ID
   * @return Java容器ID对象
   */
  private ContainerIdPBImpl convertFromProtoFormat(ContainerIdProto p) {
    return new ContainerIdPBImpl(p);
  }
  
  /**
   * 将Protobuf格式的计数器转换为Java对象
   * @param p Protobuf格式的计数器
   * @return Java计数器对象
   */
  private CountersPBImpl convertFromProtoFormat(CountersProto p) {
    return new CountersPBImpl(p);
  }

  /**
   * 将Java计数器对象转换为Protobuf格式
   * @param t Java计数器对象
   * @return Protobuf格式的计数器
   */
  private CountersProto convertToProtoFormat(Counters t) {
    return ((CountersPBImpl)t).getProto();
  }

  /**
   * 将Protobuf格式的任务尝试ID转换为Java对象
   * @param p Protobuf格式的任务尝试ID
   * @return Java任务尝试ID对象
   */
  private TaskAttemptIdPBImpl convertFromProtoFormat(TaskAttemptIdProto p) {
    return new TaskAttemptIdPBImpl(p);
  }

  /**
   * 将Java任务尝试ID对象转换为Protobuf格式
   * @param t Java任务尝试ID对象
   * @return Protobuf格式的任务尝试ID
   */
  private TaskAttemptIdProto convertToProtoFormat(TaskAttemptId t) {
    return ((TaskAttemptIdPBImpl)t).getProto();
  }

  /**
   * 将任务尝试状态枚举转换为Protobuf格式
   * @param e 任务尝试状态枚举
   * @return Protobuf格式的任务尝试状态
   */
  private TaskAttemptStateProto convertToProtoFormat(TaskAttemptState e) {
    return MRProtoUtils.convertToProtoFormat(e);
  }

  /**
   * 将Protobuf格式的任务尝试状态转换为枚举
   * @param e Protobuf格式的任务尝试状态
   * @return 任务尝试状态枚举
   */
  private TaskAttemptState convertFromProtoFormat(TaskAttemptStateProto e) {
    return MRProtoUtils.convertFromProtoFormat(e);
  }

  /**
   * 将任务阶段枚举转换为Protobuf格式
   * @param e 任务阶段枚举
   * @return Protobuf格式的任务阶段
   */
  private PhaseProto convertToProtoFormat(Phase e) {
    return MRProtoUtils.convertToProtoFormat(e);
  }

  /**
   * 将Protobuf格式的任务阶段转换为枚举
   * @param e Protobuf格式的任务阶段
   * @return 任务阶段枚举
   */
  private Phase convertFromProtoFormat(PhaseProto e) {
    return MRProtoUtils.convertFromProtoFormat(e);
  }
}