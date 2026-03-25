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


import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetTaskAttemptCompletionEventsRequest;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.impl.pb.JobIdPBImpl;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.JobIdProto;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.GetTaskAttemptCompletionEventsRequestProto;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.GetTaskAttemptCompletionEventsRequestProtoOrBuilder;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoBase;

/**
 * 获取任务尝试完成事件请求的Protobuf实现类
 * 基于ProtoBase实现，封装Protobuf序列化逻辑，用于MapReduce服务端与客户端之间的RPC通信
 */    
public class GetTaskAttemptCompletionEventsRequestPBImpl extends ProtoBase<GetTaskAttemptCompletionEventsRequestProto> implements GetTaskAttemptCompletionEventsRequest {
  // Protobuf默认实例，当通过已有proto构造时使用
  GetTaskAttemptCompletionEventsRequestProto proto = GetTaskAttemptCompletionEventsRequestProto.getDefaultInstance();
  // Protobuf构建器，当构造新请求或修改请求时使用
  GetTaskAttemptCompletionEventsRequestProto.Builder builder = null;
  // 标识当前是否使用已有proto对象，false表示正在通过builder构建
  boolean viaProto = false;
  
  // 缓存的作业ID对象，避免重复解析
  private JobId jobId = null;
  
  
  /**
   * 默认构造函数，初始化空的请求构建器
   */
  public GetTaskAttemptCompletionEventsRequestPBImpl() {
    builder = GetTaskAttemptCompletionEventsRequestProto.newBuilder();
  }

  /**
   * 基于已有Protobuf对象构造请求包装类
   * @param proto 已序列化的请求proto对象
   */
  public GetTaskAttemptCompletionEventsRequestPBImpl(GetTaskAttemptCompletionEventsRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  @Override
  public GetTaskAttemptCompletionEventsRequestProto getProto() {
      // 将本地缓存的字段合并到proto中
      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  // 将本地缓存的字段合并到Protobuf构建器中
  private void mergeLocalToBuilder() {
    if (this.jobId != null) {
      builder.setJobId(convertToProtoFormat(this.jobId));
    }
  }

  // 将本地缓存的字段合并生成最终的proto对象
  private void mergeLocalToProto() {
    if (viaProto) 
      // 如果当前是从proto读取，先初始化构建器
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  // 初始化Protobuf构建器，基于已有proto对象
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = GetTaskAttemptCompletionEventsRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public JobId getJobId() {
    GetTaskAttemptCompletionEventsRequestProtoOrBuilder p = viaProto ? proto : builder;
    // 先返回缓存
    if (this.jobId != null) {
      return this.jobId;
    }
    // proto中不存在则返回null
    if (!p.hasJobId()) {
      return null;
    }
    // 从proto解析并缓存
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
  public int getFromEventId() {
    GetTaskAttemptCompletionEventsRequestProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getFromEventId());
  }

  @Override
  public void setFromEventId(int fromEventId) {
    maybeInitBuilder();
    builder.setFromEventId((fromEventId));
  }
  
  @Override
  public int getMaxEvents() {
    GetTaskAttemptCompletionEventsRequestProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getMaxEvents());
  }

  @Override
  public void setMaxEvents(int maxEvents) {
    maybeInitBuilder();
    builder.setMaxEvents((maxEvents));
  }

  // 将Protobuf格式的JobId转换为MapReduce API包装类
  private JobIdPBImpl convertFromProtoFormat(JobIdProto p) {
    return new JobIdPBImpl(p);
  }

  // 将MapReduce API格式的JobId转换为Protobuf格式
  private JobIdProto convertToProtoFormat(JobId t) {
    return ((JobIdPBImpl)t).getProto();
  }

}