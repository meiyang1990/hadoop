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


import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetCountersRequest;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.impl.pb.JobIdPBImpl;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.JobIdProto;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.GetCountersRequestProto;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.GetCountersRequestProtoOrBuilder;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoBase;

/**
 * 获取作业计数器请求的Protobuf序列化实现类
 * 负责GetCountersRequest请求结构的PB序列化与反序列化，用于MapReduce客户端与服务端RPC通信
 */    
public class GetCountersRequestPBImpl extends ProtoBase<GetCountersRequestProto> implements GetCountersRequest {
  // PB默认实例对象
  GetCountersRequestProto proto = GetCountersRequestProto.getDefaultInstance();
  // PB构建器
  GetCountersRequestProto.Builder builder = null;
  // 标记是否通过已构建的proto对象初始化
  boolean viaProto = false;
  
  // 缓存的作业ID对象
  private JobId jobId = null;
  
  
  /**
   * 构造函数，初始化空的PB构建器
   */
  public GetCountersRequestPBImpl() {
    builder = GetCountersRequestProto.newBuilder();
  }

  /**
   * 通过已有的PB对象构造请求实例
   * @param proto 已序列化的GetCountersRequestProto对象
   */
  public GetCountersRequestPBImpl(GetCountersRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  @Override
  public GetCountersRequestProto getProto() {
      // 将本地缓存的字段合并到PB构建器
      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    // 如果本地缓存了JobId，将其转换为PB格式写入构建器
    if (this.jobId != null) {
      builder.setJobId(convertToProtoFormat(this.jobId));
    }
  }

  private void mergeLocalToProto() {
    // 如果是从现有proto初始化，先初始化构建器
    if (viaProto) 
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private void maybeInitBuilder() {
    // 如果是从proto读取或者构建器未初始化，基于现有proto创建构建器
    if (viaProto || builder == null) {
      builder = GetCountersRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public JobId getJobId() {
    GetCountersRequestProtoOrBuilder p = viaProto ? proto : builder;
    // 返回本地缓存的JobId如果已存在
    if (this.jobId != null) {
      return this.jobId;
    }
    // PB中不存在JobId则返回null
    if (!p.hasJobId()) {
      return null;
    }
    // 从PB反序列化为JobId对象并缓存
    this.jobId = convertFromProtoFormat(p.getJobId());
    return this.jobId;
  }

  @Override
  public void setJobId(JobId jobId) {
    maybeInitBuilder();
    // 清空PB中的JobId如果传入null
    if (jobId == null) 
      builder.clearJobId();
    // 缓存JobId到本地
    this.jobId = jobId;
  }

  /**
   * 将PB格式的JobIdProto转换为MapReduce API的JobId对象
   * @param p PB格式JobId
   * @return 转换后的JobId对象
   */
  private JobIdPBImpl convertFromProtoFormat(JobIdProto p) {
    return new JobIdPBImpl(p);
  }

  /**
   * 将MapReduce API的JobId对象转换为PB格式
   * @param t JobId对象
   * @return 转换后的PB格式JobIdProto
   */
  private JobIdProto convertToProtoFormat(JobId t) {
    return ((JobIdPBImpl)t).getProto();
  }

}