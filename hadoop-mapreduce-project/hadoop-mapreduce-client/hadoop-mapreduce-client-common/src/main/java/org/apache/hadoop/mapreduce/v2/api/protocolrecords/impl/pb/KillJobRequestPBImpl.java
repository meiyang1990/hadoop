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


import org.apache.hadoop.mapreduce.v2.api.protocolrecords.KillJobRequest;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.impl.pb.JobIdPBImpl;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.JobIdProto;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.KillJobRequestProto;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.KillJobRequestProtoOrBuilder;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoBase;

/**
 * 终止Job请求的Protobuf实现，基于ProtoBase实现协议序列化与反序列化
 * 用于MapReduce客户端与服务端之间终止作业请求的协议传输
 */    
public class KillJobRequestPBImpl extends ProtoBase<KillJobRequestProto> implements KillJobRequest {
  // Protobuf消息对象，只读模式下使用
  KillJobRequestProto proto = KillJobRequestProto.getDefaultInstance();
  // Protobuf构建器，可写模式下使用
  KillJobRequestProto.Builder builder = null;
  // 当前是否通过proto对象构造标记
  boolean viaProto = false;
  
  // 缓存的作业ID对象
  private JobId jobId = null;
  
  
  /**
   * 构造空的终止Job请求对象，用于构造新请求
   */
  public KillJobRequestPBImpl() {
    builder = KillJobRequestProto.newBuilder();
  }

  /**
   * 基于已有Protobuf消息构造终止Job请求对象，用于解析接收到的请求
   * @param proto 已序列化的Protobuf终止Job请求消息
   */
  public KillJobRequestPBImpl(KillJobRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  @Override
  public KillJobRequestProto getProto() {
      // 将本地缓存的对象合并到Protobuf结构
      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  // 将本地缓存的作业ID合并到Protobuf Builder中
  private void mergeLocalToBuilder() {
    if (this.jobId != null) {
      builder.setJobId(convertToProtoFormat(this.jobId));
    }
  }

  // 将本地缓存数据合并生成最终Protobuf对象
  private void mergeLocalToProto() {
    if (viaProto) 
      // 如果当前是只读proto模式，初始化Builder
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  // 初始化Builder，从现有proto对象拷贝数据
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = KillJobRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public JobId getJobId() {
    KillJobRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (this.jobId != null) {
      return this.jobId;
    }
    if (!p.hasJobId()) {
      return null;
    }
    // 从Protobuf格式转换为MapReduce API对象并缓存
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

  // 将Protobuf格式的JobId转换为API层对象
  private JobIdPBImpl convertFromProtoFormat(JobIdProto p) {
    return new JobIdPBImpl(p);
  }

  // 将API层JobId对象转换为Protobuf格式
  private JobIdProto convertToProtoFormat(JobId t) {
    return ((JobIdPBImpl)t).getProto();
  }


}