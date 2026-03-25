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


import org.apache.hadoop.mapreduce.v2.api.protocolrecords.FailTaskAttemptRequest;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.impl.pb.TaskAttemptIdPBImpl;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.TaskAttemptIdProto;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.FailTaskAttemptRequestProto;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.FailTaskAttemptRequestProtoOrBuilder;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoBase;

/**
 * 标记任务尝试失败请求的Protobuf实现类
 * 实现FailTaskAttemptRequest接口，基于Protobuf进行序列化和反序列化
 * 用于MapReduce服务端与客户端之间标记任务尝试失败的请求传输
 */    
public class FailTaskAttemptRequestPBImpl extends ProtoBase<FailTaskAttemptRequestProto> implements FailTaskAttemptRequest {
  // Protobuf默认实例，当通过已有proto构造时使用
  FailTaskAttemptRequestProto proto = FailTaskAttemptRequestProto.getDefaultInstance();
  // Protobuf构建器，当构造新请求时使用
  FailTaskAttemptRequestProto.Builder builder = null;
  // 标识当前是否使用已构建好的proto对象
  boolean viaProto = false;
  
  // 缓存待失败任务尝试ID对象
  private TaskAttemptId taskAttemptId = null;
  
  
  /**
   * 构造空的失败任务尝试请求对象，用于新建请求
   */
  public FailTaskAttemptRequestPBImpl() {
    builder = FailTaskAttemptRequestProto.newBuilder();
  }

  /**
   * 基于已有Protobuf对象构造失败任务尝试请求，用于反序列化
   * @param proto 已序列化的失败请求Protobuf对象
   */
  public FailTaskAttemptRequestPBImpl(FailTaskAttemptRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  @Override
  public FailTaskAttemptRequestProto getProto() {
      // 将本地缓存的对象合并到Protobuf构建器
      mergeLocalToProto();
    // 生成最终proto对象
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  /**
   * 将本地缓存的任务尝试ID合并到Protobuf构建器中
   */
  private void mergeLocalToBuilder() {
    if (this.taskAttemptId != null) {
      builder.setTaskAttemptId(convertToProtoFormat(this.taskAttemptId));
    }
  }

  /**
   * 将本地缓存合并到最终proto对象
   */
  private void mergeLocalToProto() {
    // 如果当前是从proto读取，需要先初始化构建器
    if (viaProto) 
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  /**
   * 如果需要修改内容，初始化Protobuf构建器
   */
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = FailTaskAttemptRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public TaskAttemptId getTaskAttemptId() {
    FailTaskAttemptRequestProtoOrBuilder p = viaProto ? proto : builder;
    // 如果已经缓存，直接返回缓存对象
    if (this.taskAttemptId != null) {
      return this.taskAttemptId;
    }
    // proto中不存在该字段，返回null
    if (!p.hasTaskAttemptId()) {
      return null;
    }
    // 从proto反序列化为对象并缓存
    this.taskAttemptId = convertFromProtoFormat(p.getTaskAttemptId());
    return this.taskAttemptId;
  }

  @Override
  public void setTaskAttemptId(TaskAttemptId taskAttemptId) {
    maybeInitBuilder();
    // 清空对应字段
    if (taskAttemptId == null) 
      builder.clearTaskAttemptId();
    // 缓存设置的对象
    this.taskAttemptId = taskAttemptId;
  }

  /**
   * 将Protobuf格式的任务尝试ID转换为内部API对象
   * @param p Protobuf格式的任务尝试ID
   * @return 内部API格式的任务尝试ID对象
   */
  private TaskAttemptIdPBImpl convertFromProtoFormat(TaskAttemptIdProto p) {
    return new TaskAttemptIdPBImpl(p);
  }

  /**
   * 将内部API格式的任务尝试ID转换为Protobuf格式
   * @param t 内部API格式的任务尝试ID对象
   * @return Protobuf格式的任务尝试ID
   */
  private TaskAttemptIdProto convertToProtoFormat(TaskAttemptId t) {
    return ((TaskAttemptIdPBImpl)t).getProto();
  }



}