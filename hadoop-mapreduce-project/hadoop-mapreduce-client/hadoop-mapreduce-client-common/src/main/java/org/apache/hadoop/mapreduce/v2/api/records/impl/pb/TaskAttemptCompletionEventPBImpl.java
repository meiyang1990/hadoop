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


import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptCompletionEvent;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptCompletionEventStatus;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.TaskAttemptCompletionEventProto;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.TaskAttemptCompletionEventProtoOrBuilder;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.TaskAttemptCompletionEventStatusProto;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.TaskAttemptIdProto;
import org.apache.hadoop.mapreduce.v2.util.MRProtoUtils;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoBase;

/**
 * 任务尝试完成事件的Protobuf序列化实现，基于ProtoBase实现，负责MapReduce任务尝试完成事件的PB格式转换与存储
 * 是Hadoop MapReduce API层记录类型的PB实现，用于RPC通信中的序列化与反序列化
 */
public class TaskAttemptCompletionEventPBImpl extends ProtoBase<TaskAttemptCompletionEventProto> implements TaskAttemptCompletionEvent {
  // Protobuf默认实例，当通过只读模式访问时使用
  TaskAttemptCompletionEventProto proto = TaskAttemptCompletionEventProto.getDefaultInstance();
  // Protobuf构建器，当修改对象状态时使用
  TaskAttemptCompletionEventProto.Builder builder = null;
  // 当前是否通过现有proto实例构建标记
  boolean viaProto = false;
  
  // 缓存任务尝试ID对象，避免重复转换
  private TaskAttemptId taskAttemptId = null;
  
  /**
   * 空构造函数，初始化Protobuf构建器
   */
  public TaskAttemptCompletionEventPBImpl() {
    builder = TaskAttemptCompletionEventProto.newBuilder();
  }

  /**
   * 基于已有Protobuf对象构造实现类
   * @param proto 已构造完成的TaskAttemptCompletionEventProto实例
   */
  public TaskAttemptCompletionEventPBImpl(TaskAttemptCompletionEventProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  /**
   * 获取当前对象对应的Protobuf实例，合并本地修改后生成最终proto
   * @return 序列化完成的TaskAttemptCompletionEventProto实例
   */
  public TaskAttemptCompletionEventProto getProto() {
      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  /**
   * 将本地缓存的领域对象合并到Protobuf构建器中
   */
  private void mergeLocalToBuilder() {
    if (this.taskAttemptId != null) {
      builder.setAttemptId(convertToProtoFormat(this.taskAttemptId));
    }
  }

  /**
   * 将本地修改合并到Protobuf实例中
   */
  private void mergeLocalToProto() {
    if (viaProto) 
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  /**
   * 如果当前是只读proto模式，初始化构建器以便修改
   */
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = TaskAttemptCompletionEventProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public TaskAttemptId getAttemptId() {
    TaskAttemptCompletionEventProtoOrBuilder p = viaProto ? proto : builder;
    if (this.taskAttemptId != null) {
      return this.taskAttemptId;
    }
    if (!p.hasAttemptId()) {
      return null;
    }
    // 从Protobuf格式转换为领域对象并缓存
    this.taskAttemptId = convertFromProtoFormat(p.getAttemptId());
    return this.taskAttemptId;
  }

  @Override
  public void setAttemptId(TaskAttemptId attemptId) {
    maybeInitBuilder();
    if (attemptId == null) 
      builder.clearAttemptId();
    this.taskAttemptId = attemptId;
  }

  @Override
  public TaskAttemptCompletionEventStatus getStatus() {
    TaskAttemptCompletionEventProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasStatus()) {
      return null;
    }
    // 从Protobuf枚举转换为领域枚举
    return convertFromProtoFormat(p.getStatus());
  }

  @Override
  public void setStatus(TaskAttemptCompletionEventStatus status) {
    maybeInitBuilder();
    if (status == null) {
      builder.clearStatus();
      return;
    }
    // 将领域枚举转换为Protobuf枚举
    builder.setStatus(convertToProtoFormat(status));
  }

  @Override
  public String getMapOutputServerAddress() {
    TaskAttemptCompletionEventProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasMapOutputServerAddress()) {
      return null;
    }
    return (p.getMapOutputServerAddress());
  }

  @Override
  public void setMapOutputServerAddress(String mapOutputServerAddress) {
    maybeInitBuilder();
    if (mapOutputServerAddress == null) {
      builder.clearMapOutputServerAddress();
      return;
    }
    builder.setMapOutputServerAddress((mapOutputServerAddress));
  }

  @Override
  public int getAttemptRunTime() {
    TaskAttemptCompletionEventProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getAttemptRunTime());
  }

  @Override
  public void setAttemptRunTime(int attemptRunTime) {
    maybeInitBuilder();
    builder.setAttemptRunTime((attemptRunTime));
  }

  @Override
  public int getEventId() {
    TaskAttemptCompletionEventProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getEventId());
  }

  @Override
  public void setEventId(int eventId) {
    maybeInitBuilder();
    builder.setEventId((eventId));
  }

  /**
   * 将Protobuf格式的TaskAttemptId转换为PBImpl领域对象
   * @param p Protobuf格式的TaskAttemptIdProto
   * @return 转换后的TaskAttemptIdPBImpl实例
   */
  private TaskAttemptIdPBImpl convertFromProtoFormat(TaskAttemptIdProto p) {
    return new TaskAttemptIdPBImpl(p);
  }

  /**
   * 将领域格式的TaskAttemptId转换为Protobuf格式
   * @param t 领域对象TaskAttemptId
   * @return Protobuf格式的TaskAttemptIdProto
   */
  private TaskAttemptIdProto convertToProtoFormat(TaskAttemptId t) {
    return ((TaskAttemptIdPBImpl)t).getProto();
  }

  /**
   * 将领域枚举TaskAttemptCompletionEventStatus转换为Protobuf枚举
   * @param e 领域枚举实例
   * @return Protobuf枚举实例
   */
  private TaskAttemptCompletionEventStatusProto convertToProtoFormat(TaskAttemptCompletionEventStatus e) {
    return MRProtoUtils.convertToProtoFormat(e);
  }

  /**
   * 将Protobuf枚举转换为领域枚举TaskAttemptCompletionEventStatus
   * @param e Protobuf枚举实例
   * @return 领域枚举实例
   */
  private TaskAttemptCompletionEventStatus convertFromProtoFormat(TaskAttemptCompletionEventStatusProto e) {
    return MRProtoUtils.convertFromProtoFormat(e);
  }

}