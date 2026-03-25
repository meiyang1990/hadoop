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


import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetTaskReportResponse;
import org.apache.hadoop.mapreduce.v2.api.records.TaskReport;
import org.apache.hadoop.mapreduce.v2.api.records.impl.pb.TaskReportPBImpl;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.TaskReportProto;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.GetTaskReportResponseProto;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.GetTaskReportResponseProtoOrBuilder;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoBase;

/**
 * GetTaskReportResponse协议的Protobuf实现类，用于封装获取任务报告响应的PB序列化/反序列化逻辑
 * 实现MapReduce服务端与客户端之间获取任务报告响应的序列化传输
 */    
public class GetTaskReportResponsePBImpl extends ProtoBase<GetTaskReportResponseProto> implements GetTaskReportResponse {
  GetTaskReportResponseProto proto = GetTaskReportResponseProto.getDefaultInstance();
  GetTaskReportResponseProto.Builder builder = null;
  boolean viaProto = false;
  
  private TaskReport taskReport = null;
  
  
  /**
   * 构造函数，初始化PB构建器，用于创建新响应对象
   */
  public GetTaskReportResponsePBImpl() {
    builder = GetTaskReportResponseProto.newBuilder();
  }

  /**
   * 构造函数，基于已有的Protobuf对象包装实现
   * @param proto 已构建好的GetTaskReportResponseProto对象
   */
  public GetTaskReportResponsePBImpl(GetTaskReportResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  /**
   * 获取当前对象对应的Protobuf原生对象，合并本地修改并构建最终Proto
   * @return 构建完成的GetTaskReportResponseProto对象
   */
  public GetTaskReportResponseProto getProto() {
      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  /**
   * 将本地缓存的任务报告对象合并到PB构建器中
   */
  private void mergeLocalToBuilder() {
    if (this.taskReport != null) {
      builder.setTaskReport(convertToProtoFormat(this.taskReport));
    }
  }

  /**
   * 将本地修改合并到Proto对象中，完成Proto构建
   */
  private void mergeLocalToProto() {
    if (viaProto) 
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  /**
   * 按需初始化PB构建器，若当前基于已有Proto则基于它创建构建器
   */
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = GetTaskReportResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public TaskReport getTaskReport() {
    GetTaskReportResponseProtoOrBuilder p = viaProto ? proto : builder;
    if (this.taskReport != null) {
      return this.taskReport;
    }
    if (!p.hasTaskReport()) {
      return null;
    }
    // 从Proto反序列化为TaskReport对象并缓存
    this.taskReport =  convertFromProtoFormat(p.getTaskReport());
    return this.taskReport;
  }

  @Override
  public void setTaskReport(TaskReport taskReport) {
    maybeInitBuilder();
    if (taskReport == null) 
      builder.clearTaskReport();
    this.taskReport = taskReport;
  }

  /**
   * 将Protobuf格式的TaskReport转换为MapReduce API层的TaskReport实现
   * @param p Protobuf格式的TaskReportProto对象
   * @return 转换后的TaskReportPBImpl对象
   */
  private TaskReportPBImpl convertFromProtoFormat(TaskReportProto p) {
    return new TaskReportPBImpl(p);
  }

  /**
   * 将MapReduce API层的TaskReport转换为Protobuf格式
   * @param t API层的TaskReport对象
   * @return 转换后的TaskReportProto对象
   */
  private TaskReportProto convertToProtoFormat(TaskReport t) {
    return ((TaskReportPBImpl)t).getProto();
  }



}