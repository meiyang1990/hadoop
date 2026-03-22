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
 * @file KillTaskAttemptRequestPBImpl.java
 * 杀死任务尝试请求PB实现类，基于Protobuf序列化协议实现
 */
package org.apache.hadoop.mapreduce.v2.api.protocolrecords.impl.pb;


import org.apache.hadoop.mapreduce.v2.api.protocolrecords.KillTaskAttemptRequest;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.impl.pb.TaskAttemptIdPBImpl;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.TaskAttemptIdProto;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.KillTaskAttemptRequestProto;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.KillTaskAttemptRequestProtoOrBuilder;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoBase;

/**
 * 杀死任务尝试请求的Protobuf实现类
 * 负责封装KillTaskAttemptRequest接口的PB序列化逻辑，用于MapReduce服务端与客户端之间的RPC通信
 * 继承ProtoBase提供通用PB序列化能力，实现KillTaskAttemptRequest接口定义业务方法
 */    
public class KillTaskAttemptRequestPBImpl extends ProtoBase<KillTaskAttemptRequestProto> implements KillTaskAttemptRequest {
  // 保存构建完成的PB消息对象
  KillTaskAttemptRequestProto proto = KillTaskAttemptRequestProto.getDefaultInstance();
  // PB消息构建器，用于构造新消息
  KillTaskAttemptRequestProto.Builder builder = null;
  // 标记当前是否通过现有PB对象构建
  boolean viaProto = false;
  
  // 缓存的任务尝试ID对象
  private TaskAttemptId taskAttemptId = null;
  
  /**
   * 无参构造函数，初始化PB构建器
   */
  public KillTaskAttemptRequestPBImpl() {
    builder = KillTaskAttemptRequestProto.newBuilder();
  }

  /**
   * 通过现有PB对象构造请求实例
   * @param proto 已序列化的杀死任务尝试请求PB对象
   */
  public KillTaskAttemptRequestPBImpl(KillTaskAttemptRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  @Override
  public KillTaskAttemptRequestProto getProto() {
      // 将本地缓存的业务对象合并到PB构建器
      mergeLocalToProto();
    // 根据构建方式生成最终PB对象
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  // 将本地缓存的任务尝试ID合并到PB构建器
  private void mergeLocalToBuilder() {
    if (this.taskAttemptId != null) {
      builder.setTaskAttemptId(convertToProtoFormat(this.taskAttemptId));
    }
  }

  // 将本地缓存的字段合并到最终PB对象
  private void mergeLocalToProto() {
    // 如果当前基于已有PB对象，先初始化构建器
    if (viaProto) 
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  // 初始化PB构建器，如果当前基于已有PB则从现有对象创建构建器
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = KillTaskAttemptRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public TaskAttemptId getTaskAttemptId() {
    // 根据构建方式选择PB对象或构建器
    KillTaskAttemptRequestProtoOrBuilder p = viaProto ? proto : builder;
    // 已有缓存直接返回
    if (this.taskAttemptId != null) {
      return this.taskAttemptId;
    }
    // PB中不存在该字段返回null
    if (!p.hasTaskAttemptId()) {
      return null;
    }
    // 从PB反序列化为业务对象并缓存
    this.taskAttemptId = convertFromProtoFormat(p.getTaskAttemptId());
    return this.taskAttemptId;
  }

  @Override
  public void setTaskAttemptId(TaskAttemptId taskAttemptId) {
    // 确保构建器已初始化
    maybeInitBuilder();
    // 清空PB中对应字段
    if (taskAttemptId == null) 
      builder.clearTaskAttemptId();
    // 缓存业务对象
    this.taskAttemptId = taskAttemptId;
  }

  // 将PB格式的任务尝试ID转换为业务对象
  private TaskAttemptIdPBImpl convertFromProtoFormat(TaskAttemptIdProto p) {
    return new TaskAttemptIdPBImpl(p);
  }

  // 将业务对象格式的任务尝试ID转换为PB格式
  private TaskAttemptIdProto convertToProtoFormat(TaskAttemptId t) {
    return ((TaskAttemptIdPBImpl)t).getProto();
  }

}