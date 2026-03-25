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

package org.apache.hadoop.mapreduce.v2.api.records.impl.pb;

import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.TaskAttemptIdProto;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.TaskAttemptIdProtoOrBuilder;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.TaskIdProto;

/**
 * TaskAttemptId的Protobuf协议缓冲实现类，负责MapReduce任务尝试标识的序列化与反序列化
 * 基于Protobuf实现，用于RPC通信中任务尝试标识的编解码
 */
public class TaskAttemptIdPBImpl extends TaskAttemptId {
  TaskAttemptIdProto proto = TaskAttemptIdProto.getDefaultInstance();
  TaskAttemptIdProto.Builder builder = null;
  boolean viaProto = false;
  
  private TaskId taskId = null;
  
  
  
  /**
   * 空构造方法，初始化Protobuf Builder用于构建新对象
   */
  public TaskAttemptIdPBImpl() {
    builder = TaskAttemptIdProto.newBuilder();
  }

  /**
   * 通过已有Protobuf对象构造包装类
   * @param proto 已序列化的TaskAttemptIdProto对象
   */
  public TaskAttemptIdPBImpl(TaskAttemptIdProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  /**
   * 获取当前对象的Protobuf序列化形式，合并本地变更生成最终Proto对象
   * @return 序列化后的TaskAttemptIdProto对象
   */
  public synchronized TaskAttemptIdProto getProto() {
      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  /**
   * 将本地缓存的字段合并到Protobuf Builder中
   */
  private synchronized void mergeLocalToBuilder() {
    if (this.taskId != null
        && !((TaskIdPBImpl) this.taskId).getProto().equals(builder.getTaskId())) {
      builder.setTaskId(convertToProtoFormat(this.taskId));
    }
  }

  /**
   * 将本地变更合并生成最终Protobuf对象
   */
  private synchronized void mergeLocalToProto() {
    if (viaProto) 
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  /**
   * 如果从Proto读取，初始化Builder用于修改操作
   */
  private synchronized void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = TaskAttemptIdProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public synchronized int getId() {
    TaskAttemptIdProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getId());
  }

  @Override
  public synchronized void setId(int id) {
    maybeInitBuilder();
    builder.setId((id));
  }

  @Override
  public synchronized TaskId getTaskId() {
    TaskAttemptIdProtoOrBuilder p = viaProto ? proto : builder;
    if (this.taskId != null) {
      return this.taskId;
    }
    if (!p.hasTaskId()) {
      return null;
    }
    // 延迟反序列化：从Proto转换为TaskId对象并缓存
    taskId = convertFromProtoFormat(p.getTaskId());
    return taskId;
  }

  @Override
  public synchronized void setTaskId(TaskId taskId) {
    maybeInitBuilder();
    if (taskId == null)
      builder.clearTaskId();
    this.taskId = taskId;
  }

  /**
   * 将Protobuf格式的TaskId转换为本地PBImpl实现对象
   * @param p Protobuf格式的TaskIdProto
   * @return 本地包装后的TaskIdPBImpl对象
   */
  private TaskIdPBImpl convertFromProtoFormat(TaskIdProto p) {
    return new TaskIdPBImpl(p);
  }

  /**
   * 将本地TaskId对象转换为Protobuf格式
   * @param t 本地包装后的TaskId对象
   * @return Protobuf格式的TaskIdProto对象
   */
  private TaskIdProto convertToProtoFormat(TaskId t) {
    return ((TaskIdPBImpl)t).getProto();
  }
}