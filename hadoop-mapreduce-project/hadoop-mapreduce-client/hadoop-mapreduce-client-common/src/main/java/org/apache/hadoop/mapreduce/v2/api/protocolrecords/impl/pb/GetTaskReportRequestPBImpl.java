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


import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetTaskReportRequest;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.impl.pb.TaskIdPBImpl;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.TaskIdProto;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.GetTaskReportRequestProto;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.GetTaskReportRequestProtoOrBuilder;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoBase;

/**
 * 获取任务报告请求的Protobuf实现类，基于ProtoBase实现协议缓冲数据序列化
 * 负责MapReduce服务端与客户端之间获取任务报告请求数据的封装与转换
 */    
public class GetTaskReportRequestPBImpl extends ProtoBase<GetTaskReportRequestProto> implements GetTaskReportRequest {
  GetTaskReportRequestProto proto = GetTaskReportRequestProto.getDefaultInstance();
  GetTaskReportRequestProto.Builder builder = null;
  boolean viaProto = false;
  
  private TaskId taskId = null;
  
  
  /**
   * 构造函数，初始化Builder用于构建请求对象
   */
  public GetTaskReportRequestPBImpl() {
    builder = GetTaskReportRequestProto.newBuilder();
  }

  /**
   * 构造函数，基于已有的Protobuf对象构造请求包装类
   * @param proto 已构造完成的GetTaskReportRequestProto对象
   */
  public GetTaskReportRequestPBImpl(GetTaskReportRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  /**
   * 获取当前请求对应的Protobuf对象，合并本地修改生成最终proto
   * @return 序列化后的GetTaskReportRequestProto对象
   */
  public GetTaskReportRequestProto getProto() {
      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  /**
   * 将本地缓存的任务ID合并到Protobuf Builder中
   */
  private void mergeLocalToBuilder() {
    if (this.taskId != null) {
      builder.setTaskId(convertToProtoFormat(this.taskId));
    }
  }

  /**
   * 将本地修改合并生成最终Protobuf对象
   */
  private void mergeLocalToProto() {
    if (viaProto) 
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  /**
   * 如果当前通过proto读取，初始化Builder用于修改
   */
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = GetTaskReportRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public TaskId getTaskId() {
    GetTaskReportRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (this.taskId != null) {
      return this.taskId;
    }
    if (!p.hasTaskId()) {
      return null;
    }
    // 将Protobuf格式转换为API层对象并缓存
    this.taskId =  convertFromProtoFormat(p.getTaskId());
    return this.taskId;
  }

  @Override
  public void setTaskId(TaskId taskId) {
    maybeInitBuilder();
    if (taskId == null) 
      builder.clearTaskId();
    this.taskId = taskId;
  }

  /**
   * 将Protobuf格式的TaskId转换为API层TaskId对象
   * @param p Protobuf格式的TaskIdProto
   * @return API层TaskId实现
   */
  private TaskIdPBImpl convertFromProtoFormat(TaskIdProto p) {
    return new TaskIdPBImpl(p);
  }

  /**
   * 将API层TaskId对象转换为Protobuf格式
   * @param t API层TaskId对象
   * @return Protobuf格式的TaskIdProto
   */
  private TaskIdProto convertToProtoFormat(TaskId t) {
    return ((TaskIdPBImpl)t).getProto();
  }

}