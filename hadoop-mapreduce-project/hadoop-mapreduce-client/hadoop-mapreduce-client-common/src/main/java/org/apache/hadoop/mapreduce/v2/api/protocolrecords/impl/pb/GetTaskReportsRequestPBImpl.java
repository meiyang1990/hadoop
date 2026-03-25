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

package org.apache.hadoop.mapreduce.v2.api.protocolrecords.impl.pb;


import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetTaskReportsRequest;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.api.records.impl.pb.JobIdPBImpl;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.JobIdProto;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.TaskTypeProto;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.GetTaskReportsRequestProto;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.GetTaskReportsRequestProtoOrBuilder;
import org.apache.hadoop.mapreduce.v2.util.MRProtoUtils;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoBase;

/**
 * GetTaskReportsRequest的Protobuf序列化实现类
 * 用于承载获取指定作业指定类型任务报告的RPC请求数据，基于ProtoBase实现Protobuf编解码
 */
public class GetTaskReportsRequestPBImpl extends ProtoBase<GetTaskReportsRequestProto> implements GetTaskReportsRequest {
  // Protobuf消息对象，只读模式下使用
  GetTaskReportsRequestProto proto = GetTaskReportsRequestProto.getDefaultInstance();
  // Protobuf消息构造器，可写模式下使用
  GetTaskReportsRequestProto.Builder builder = null;
  // 当前是否通过已有的proto构造，标识当前数据载体模式
  boolean viaProto = false;
  
  // 缓存的作业ID对象，避免重复反序列化
  private JobId jobId = null;
  
  
  /**
   * 空构造函数，初始化构造器用于构建新请求
   */
  public GetTaskReportsRequestPBImpl() {
    builder = GetTaskReportsRequestProto.newBuilder();
  }

  /**
   * 基于已有Protobuf对象构造请求封装
   * @param proto 已序列化的GetTaskReportsRequestProto对象
   */
  public GetTaskReportsRequestPBImpl(GetTaskReportsRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  @Override
  public GetTaskReportsRequestProto getProto() {
      // 将本地缓存的字段合并到Protobuf构造器
      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  // 将本地缓存的字段合并到Protobuf构造器
  private void mergeLocalToBuilder() {
    if (this.jobId != null) {
      builder.setJobId(convertToProtoFormat(this.jobId));
    }
  }

  // 将本地缓存字段合并生成最终Protobuf对象
  private void mergeLocalToProto() {
    if (viaProto) 
      // 如果当前是只读proto模式，初始化构造器
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  // 初始化Protobuf构造器，延迟初始化
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = GetTaskReportsRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public JobId getJobId() {
    GetTaskReportsRequestProtoOrBuilder p = viaProto ? proto : builder;
    // 优先返回本地缓存的作业ID
    if (this.jobId != null) {
      return this.jobId;
    }
    // Protobuf中不存在作业ID时返回null
    if (!p.hasJobId()) {
      return null;
    }
    // 从Protobuf反序列化为JobId对象并缓存
    this.jobId = convertFromProtoFormat(p.getJobId());
    return this.jobId;
  }

  @Override
  public void setJobId(JobId jobId) {
    maybeInitBuilder();
    if (jobId == null) 
      builder.clearJobId();
    this.jobId = jobId;
  }

  @Override
  public TaskType getTaskType() {
    GetTaskReportsRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasTaskType()) {
      return null;
    }
    // 从Protobuf枚举转换为业务层TaskType
    return convertFromProtoFormat(p.getTaskType());
  }

  @Override
  public void setTaskType(TaskType taskType) {
    maybeInitBuilder();
    if (taskType == null) {
      builder.clearTaskType();
      return;
    }
    // 将业务层TaskType转换为Protobuf枚举存储
    builder.setTaskType(convertToProtoFormat(taskType));
  }

  // 将Protobuf格式JobId转换为业务层JobId实现
  private JobIdPBImpl convertFromProtoFormat(JobIdProto p) {
    return new JobIdPBImpl(p);
  }

  // 将业务层JobId转换为Protobuf格式JobId
  private JobIdProto convertToProtoFormat(JobId t) {
    return ((JobIdPBImpl)t).getProto();
  }

  // 将业务层TaskType转换为Protobuf格式枚举
  private TaskTypeProto convertToProtoFormat(TaskType e) {
    return MRProtoUtils.convertToProtoFormat(e);
  }

  // 将Protobuf格式枚举转换为业务层TaskType
  private TaskType convertFromProtoFormat(TaskTypeProto e) {
    return MRProtoUtils.convertFromProtoFormat(e);
  }

}