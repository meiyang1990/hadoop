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


import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetJobReportRequest;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.impl.pb.JobIdPBImpl;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.JobIdProto;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.GetJobReportRequestProto;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.GetJobReportRequestProtoOrBuilder;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoBase;

/**
 * GetJobReportRequest协议的Protobuf实现类，封装获取作业报告请求的Protobuf序列化/反序列化逻辑
 * 用于MapReduce客户端与服务端之间获取作业信息的RPC请求传输
 */
public class GetJobReportRequestPBImpl extends ProtoBase<GetJobReportRequestProto> implements GetJobReportRequest {
  GetJobReportRequestProto proto = GetJobReportRequestProto.getDefaultInstance();
  GetJobReportRequestProto.Builder builder = null;
  boolean viaProto = false;
  
  // 缓存的作业ID对象，避免重复反序列化
  private JobId jobId = null;
  
  
  /**
   * 构造方法，创建空的获取作业报告请求，用于构建新请求
   */
  public GetJobReportRequestPBImpl() {
    builder = GetJobReportRequestProto.newBuilder();
  }

  /**
   * 构造方法，从已有的Protobuf对象包装获取作业报告请求，用于反序列化
   * @param proto 已构建好的GetJobReportRequestProto对象
   */
  public GetJobReportRequestPBImpl(GetJobReportRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  @Override
  public GetJobReportRequestProto getProto() {
      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  // 将本地缓存的领域对象合并到Protobuf Builder中
  private void mergeLocalToBuilder() {
    if (this.jobId  != null) {
      builder.setJobId(convertToProtoFormat(this.jobId));
    }
  }

  // 合并本地变更生成最终Protobuf对象
  private void mergeLocalToProto() {
    if (viaProto) 
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  // 延迟初始化Builder，从已有Protobuf对象创建Builder以便修改
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = GetJobReportRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public JobId getJobId() {
    GetJobReportRequestProtoOrBuilder p = viaProto ? proto : builder;
    // 优先返回缓存的领域对象
    if (this.jobId != null) {
      return this.jobId;
    }
    // Protobuf中不存在JobId时返回null
    if (!p.hasJobId()) {
      return null;
    }
    // 从Protobuf反序列化为领域对象并缓存
    this.jobId = convertFromProtoFormat(p.getJobId());
    return this.jobId;
  }

  @Override
  public void setJobId(JobId jobId) {
    maybeInitBuilder();
    // 清空Protobuf中的JobId字段
    if (jobId == null) 
      builder.clearJobId();
    this.jobId = jobId;
  }

  // 将Protobuf格式的JobId转换为领域对象格式
  private JobIdPBImpl convertFromProtoFormat(JobIdProto p) {
    return new JobIdPBImpl(p);
  }

  // 将领域对象格式的JobId转换为Protobuf格式
  private JobIdProto convertToProtoFormat(JobId t) {
    return ((JobIdPBImpl)t).getProto();
  }

}