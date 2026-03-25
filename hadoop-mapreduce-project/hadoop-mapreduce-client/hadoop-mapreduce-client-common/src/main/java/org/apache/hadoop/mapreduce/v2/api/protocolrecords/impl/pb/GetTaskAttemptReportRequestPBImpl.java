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


import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetTaskAttemptReportRequest;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.impl.pb.TaskAttemptIdPBImpl;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.TaskAttemptIdProto;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.GetTaskAttemptReportRequestProto;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.GetTaskAttemptReportRequestProtoOrBuilder;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoBase;

/**
 * 获取任务尝试报告请求的Protocol Buffer实现类，封装MapReduce服务端RPC请求的序列化/反序列化逻辑
 */
public class GetTaskAttemptReportRequestPBImpl extends ProtoBase<GetTaskAttemptReportRequestProto> implements GetTaskAttemptReportRequest {
  // 存储请求的Proto对象实例
  GetTaskAttemptReportRequestProto proto = GetTaskAttemptReportRequestProto.getDefaultInstance();
  // Proto构建器，用于构建/修改请求对象
  GetTaskAttemptReportRequestProto.Builder builder = null;
  // 标记当前是否通过已有Proto对象构建
  boolean viaProto = false;
  
  // 缓存反序列化后的任务尝试ID对象
  private TaskAttemptId taskAttemptId = null;
  
  /**
   * 构造函数，初始化空的请求构建器
   */
  public GetTaskAttemptReportRequestPBImpl() {
    builder = GetTaskAttemptReportRequestProto.newBuilder();
  }

  /**
   * 构造函数，基于已有Proto对象构建请求实例
   * @param proto 已有的获取任务尝试报告请求Proto对象
   */
  public GetTaskAttemptReportRequestPBImpl(GetTaskAttemptReportRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  @Override
  public GetTaskAttemptReportRequestProto getProto() {
      // 合并本地缓存数据到Proto
      mergeLocalToProto();
    // 生成最终Proto对象
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  // 合并本地缓存对象到Proto构建器
  private void mergeLocalToBuilder() {
    if (this.taskAttemptId != null) {
      builder.setTaskAttemptId(convertToProtoFormat(this.taskAttemptId));
    }
  }

  // 合并本地缓存数据生成最终Proto对象
  private void mergeLocalToProto() {
    if (viaProto) 
      // 若当前通过已有Proto构建，初始化构建器
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  // 初始化Proto构建器，用于从已有Proto修改数据
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = GetTaskAttemptReportRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public TaskAttemptId getTaskAttemptId() {
    GetTaskAttemptReportRequestProtoOrBuilder p = viaProto ? proto : builder;
    // 优先返回本地缓存
    if (this.taskAttemptId != null) {
      return this.taskAttemptId;
    }
    if (!p.hasTaskAttemptId()) {
      return null;
    }
    // 从Proto反序列化得到任务尝试ID
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

  // 将Proto格式的任务尝试ID转换为API对象格式
  private TaskAttemptIdPBImpl convertFromProtoFormat(TaskAttemptIdProto p) {
    return new TaskAttemptIdPBImpl(p);
  }

  // 将API对象格式的任务尝试ID转换为Proto格式
  private TaskAttemptIdProto convertToProtoFormat(TaskAttemptId t) {
    return ((TaskAttemptIdPBImpl)t).getProto();
  }

}