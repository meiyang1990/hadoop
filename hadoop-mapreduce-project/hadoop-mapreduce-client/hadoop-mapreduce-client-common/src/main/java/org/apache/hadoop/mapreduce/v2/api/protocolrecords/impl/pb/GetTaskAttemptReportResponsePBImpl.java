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

/**
 * @file GetTaskAttemptReportResponsePBImpl.java
 * 获取任务尝试报告响应的Protobuf实现，基于ProtoBase实现协议数据转换
 */
package org.apache.hadoop.mapreduce.v2.api.protocolrecords.impl.pb;


import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetTaskAttemptReportResponse;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptReport;
import org.apache.hadoop.mapreduce.v2.api.records.impl.pb.TaskAttemptReportPBImpl;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.TaskAttemptReportProto;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.GetTaskAttemptReportResponseProto;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.GetTaskAttemptReportResponseProtoOrBuilder;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoBase;

/**
 * 获取任务尝试报告响应的Protobuf实现类
 * 负责将MapReduce服务端获取任务尝试报告的响应在高级API对象与Protobuf协议对象之间转换
 * 实现了GetTaskAttemptReportResponse接口，继承ProtoBase完成通用协议转换逻辑
 */    
public class GetTaskAttemptReportResponsePBImpl extends ProtoBase<GetTaskAttemptReportResponseProto> implements GetTaskAttemptReportResponse {
  // 原始Protobuf协议对象
  GetTaskAttemptReportResponseProto proto = GetTaskAttemptReportResponseProto.getDefaultInstance();
  // Protobuf构建器
  GetTaskAttemptReportResponseProto.Builder builder = null;
  // 标识当前是否通过proto对象存储数据
  boolean viaProto = false;
  
  // 高级API层的任务尝试报告对象
  private TaskAttemptReport taskAttemptReport = null;
  
  
  /**
   * 空构造函数，初始化Protobuf构建器
   */
  public GetTaskAttemptReportResponsePBImpl() {
    builder = GetTaskAttemptReportResponseProto.newBuilder();
  }

  /**
   * 基于已有Protobuf对象构造响应对象
   * @param proto 已序列化的获取任务尝试报告响应Protobuf对象
   */
  public GetTaskAttemptReportResponsePBImpl(GetTaskAttemptReportResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  @Override
  public GetTaskAttemptReportResponseProto getProto() {
      // 将本地高级API对象合并到Protobuf构建器
      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  /**
   * 将本地缓存的高级API对象合并到Protobuf构建器
   */
  private void mergeLocalToBuilder() {
    if (this.taskAttemptReport != null) {
      builder.setTaskAttemptReport(convertToProtoFormat(this.taskAttemptReport));
    }
  }

  /**
   * 将本地数据合并生成最终Protobuf对象
   */
  private void mergeLocalToProto() {
    if (viaProto) 
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  /**
   * 如果需要修改数据，基于现有proto初始化构建器
   */
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = GetTaskAttemptReportResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public TaskAttemptReport getTaskAttemptReport() {
    GetTaskAttemptReportResponseProtoOrBuilder p = viaProto ? proto : builder;
    // 如果已经缓存了高级API对象，直接返回
    if (this.taskAttemptReport != null) {
      return this.taskAttemptReport;
    }
    // Protobuf中不存在该字段，返回null
    if (!p.hasTaskAttemptReport()) {
      return null;
    }
    // 从Protobuf转换为高级API对象并缓存
    this.taskAttemptReport =  convertFromProtoFormat(p.getTaskAttemptReport());
    return this.taskAttemptReport;
  }

  @Override
  public void setTaskAttemptReport(TaskAttemptReport taskAttemptReport) {
    maybeInitBuilder();
    if (taskAttemptReport == null) 
      builder.clearTaskAttemptReport();
    this.taskAttemptReport = taskAttemptReport;
  }

  /**
   * 将Protobuf格式的任务尝试报告转换为高级API实现对象
   * @param p Protobuf格式的任务尝试报告
   * @return 高级API层的任务尝试报告对象
   */
  private TaskAttemptReportPBImpl convertFromProtoFormat(TaskAttemptReportProto p) {
    return new TaskAttemptReportPBImpl(p);
  }

  /**
   * 将高级API格式的任务尝试报告转换为Protobuf对象
   * @param t 高级API层的任务尝试报告对象
   * @return Protobuf格式的任务尝试报告
   */
  private TaskAttemptReportProto convertToProtoFormat(TaskAttemptReport t) {
    return ((TaskAttemptReportPBImpl)t).getProto();
  }

}