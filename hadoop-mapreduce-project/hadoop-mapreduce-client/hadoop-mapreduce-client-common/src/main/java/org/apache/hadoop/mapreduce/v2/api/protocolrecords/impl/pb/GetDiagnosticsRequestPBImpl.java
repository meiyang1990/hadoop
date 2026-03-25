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


import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetDiagnosticsRequest;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.impl.pb.TaskAttemptIdPBImpl;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.TaskAttemptIdProto;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.GetDiagnosticsRequestProto;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.GetDiagnosticsRequestProtoOrBuilder;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoBase;

/**
 * 获取任务尝试诊断信息请求的Protobuf实现类
 * 基于Protobuf序列化协议实现，负责MapReduce服务端与客户端之间的请求数据序列化
 * 核心职责是封装请求参数（任务尝试ID），并提供Protobuf格式的转换能力
 */
public class GetDiagnosticsRequestPBImpl extends ProtoBase<GetDiagnosticsRequestProto> implements GetDiagnosticsRequest {
  // Protobuf默认实例，用于只读场景
  GetDiagnosticsRequestProto proto = GetDiagnosticsRequestProto.getDefaultInstance();
  // Protobuf构建器，用于可写场景
  GetDiagnosticsRequestProto.Builder builder = null;
  // 标识当前是否使用已构建的proto实例，false表示正在使用builder构建
  boolean viaProto = false;
  
  // 缓存的任务尝试ID对象
  private TaskAttemptId taskAttemptId = null;
  
  
  /**
   * 空构造方法，初始化Protobuf构建器，用于构建新请求
   */
  public GetDiagnosticsRequestPBImpl() {
    builder = GetDiagnosticsRequestProto.newBuilder();
  }

  /**
   * 基于已有Protobuf对象构造请求包装类，用于反序列化场景
   * @param proto 已序列化的GetDiagnosticsRequestProto对象
   */
  public GetDiagnosticsRequestPBImpl(GetDiagnosticsRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  @Override
  public GetDiagnosticsRequestProto getProto() {
      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  // 将本地缓存的业务对象合并到Protobuf构建器
  private void mergeLocalToBuilder() {
    if (this.taskAttemptId != null) {
      builder.setTaskAttemptId(convertToProtoFormat(this.taskAttemptId));
    }
  }

  // 将本地缓存数据合并生成最终Protobuf对象
  private void mergeLocalToProto() {
    if (viaProto) 
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  // 初始化Protobuf构建器，从已有proto拷贝数据
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = GetDiagnosticsRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public TaskAttemptId getTaskAttemptId() {
    GetDiagnosticsRequestProtoOrBuilder p = viaProto ? proto : builder;
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

  // 将Protobuf格式的任务尝试ID转换为业务对象
  private TaskAttemptIdPBImpl convertFromProtoFormat(TaskAttemptIdProto p) {
    return new TaskAttemptIdPBImpl(p);
  }

  // 将业务格式的任务尝试ID转换为Protobuf对象
  private TaskAttemptIdProto convertToProtoFormat(TaskAttemptId t) {
    return ((TaskAttemptIdPBImpl)t).getProto();
  }



}