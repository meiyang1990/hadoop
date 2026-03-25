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
 * @file KillTaskRequestPBImpl.java
 * 基于Protobuf序列化实现的杀死任务请求实体，用于MapReduce客户端与服务端之间的KillTask RPC请求数据交换
 */
package org.apache.hadoop.mapreduce.v2.api.protocolrecords.impl.pb;


import org.apache.hadoop.mapreduce.v2.api.protocolrecords.KillTaskRequest;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.impl.pb.TaskIdPBImpl;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.TaskIdProto;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.KillTaskRequestProto;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.KillTaskRequestProtoOrBuilder;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoBase;

/**
 * Protobuf格式的KillTask请求实体实现类
 * 继承YARN ProtoBase，将KillTaskRequest接口转换为基于Protobuf的存储实现
 * 负责封装杀死MapReduce任务请求的序列化与反序列化逻辑，在客户端与MR服务端之间传递请求数据
 */
public class KillTaskRequestPBImpl extends ProtoBase<KillTaskRequestProto> implements KillTaskRequest {
  // Protobuf默认实例，只读使用
  KillTaskRequestProto proto = KillTaskRequestProto.getDefaultInstance();
  // Protobuf构建器，用于构造修改请求对象
  KillTaskRequestProto.Builder builder = null;
  // 标记当前是否通过已有Protobuf实例构造
  boolean viaProto = false;
  
  // 缓存待杀死任务的ID对象
  private TaskId taskId = null;
  
  
  /**
   * 构造空的KillTask请求对象，初始化Protobuf构建器
   */
  public KillTaskRequestPBImpl() {
    builder = KillTaskRequestProto.newBuilder();
  }

  /**
   * 基于已有Protobuf实例构造KillTask请求对象
   * @param proto 已有的KillTaskRequestProtobuf实例
   */
  public KillTaskRequestPBImpl(KillTaskRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  @Override
  public KillTaskRequestProto getProto() {
      // 将本地缓存的业务对象合并到Protobuf构建器
      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  /**
   * 将本地缓存的TaskId合并到Protobuf构建器中
   */
  private void mergeLocalToBuilder() {
    if (this.taskId != null) {
      builder.setTaskId(convertToProtoFormat(this.taskId));
    }
  }

  /**
   * 将本地缓存的业务对象合并生成最终Protobuf实例
   */
  private void mergeLocalToProto() {
    if (viaProto) 
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  /**
   * 按需初始化Protobuf构建器，当从已有proto修改时需要先构造构建器
   */
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = KillTaskRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public TaskId getTaskId() {
    KillTaskRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (this.taskId != null) {
      return this.taskId;
    }
    if (!p.hasTaskId()) {
      return null;
    }
    // 将Protobuf格式转换为业务层TaskId对象并缓存
    this.taskId = convertFromProtoFormat(p.getTaskId());
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
   * 将Protobuf格式的TaskId转换为业务层TaskId实现
   * @param p Protobuf格式的TaskIdProto
   * @return 业务层TaskId对象
   */
  private TaskIdPBImpl convertFromProtoFormat(TaskIdProto p) {
    return new TaskIdPBImpl(p);
  }

  /**
   * 将业务层TaskId转换为Protobuf格式
   * @param t 业务层TaskId对象
   * @return Protobuf格式的TaskIdProto
   */
  private TaskIdProto convertToProtoFormat(TaskId t) {
    return ((TaskIdPBImpl)t).getProto();
  }


}