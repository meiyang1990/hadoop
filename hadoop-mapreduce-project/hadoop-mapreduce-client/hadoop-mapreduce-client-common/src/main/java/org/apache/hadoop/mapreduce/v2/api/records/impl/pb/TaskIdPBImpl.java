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

import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.JobIdProto;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.TaskIdProto;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.TaskIdProtoOrBuilder;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.TaskTypeProto;
import org.apache.hadoop.mapreduce.v2.util.MRProtoUtils;

/**
 * TaskId 的Protobuf协议实现，基于Protobuf序列化框架实现任务ID的存储与转换，
 * 用于MapReduce API中任务标识符的序列化和反序列化，支持RPC通信。
 */
public class TaskIdPBImpl extends TaskId {
  // Protobuf默认实例，用于初始化
  TaskIdProto proto = TaskIdProto.getDefaultInstance();
  // Protobuf构建器，用于修改对象
  TaskIdProto.Builder builder = null;
  // 当前是否通过proto存储数据，false表示数据在本地对象中需要合并
  boolean viaProto = false;

  // 缓存所属作业ID对象，避免重复反序列化
  private JobId jobId = null;  

  /**
   * 空构造函数，初始化空的Protobuf构建器。
   */
  public TaskIdPBImpl() {
    builder = TaskIdProto.newBuilder(proto);
  }

  /**
   * 基于已有的TaskIdProto构造对象，数据直接从proto读取。
   * @param proto 已构造完成的TaskIdProto实例
   */
  public TaskIdPBImpl(TaskIdProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  /**
   * 获取当前对象对应的Protobuf实例，合并本地修改后构建最终proto。
   * @return 序列化用的TaskIdProto实例
   */
  public synchronized TaskIdProto getProto() {
      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  /**
   * 将本地缓存的对象数据合并到Protobuf构建器中。
   */
  private synchronized void mergeLocalToBuilder() {
    if (this.jobId != null
        && !((JobIdPBImpl) this.jobId).getProto().equals(builder.getJobId())) {
      builder.setJobId(convertToProtoFormat(this.jobId));
    }
  }

  /**
   * 将本地修改合并到Protobuf实例中，完成序列化。
   */
  private synchronized void mergeLocalToProto() {
    if (viaProto) 
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  /**
   * 如果当前数据在proto中，初始化构建器准备修改。
   */
  private synchronized void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = TaskIdProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public synchronized int getId() {
    TaskIdProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getId());
  }

  @Override
  public synchronized void setId(int id) {
    maybeInitBuilder();
    builder.setId((id));
  }

  @Override
  public synchronized JobId getJobId() {
    TaskIdProtoOrBuilder p = viaProto ? proto : builder;
    if (this.jobId != null) {
      return this.jobId;
    }
    if (!p.hasJobId()) {
      return null;
    }
    // 反序列化Protobuf的JobId为本地对象并缓存
    jobId = convertFromProtoFormat(p.getJobId());
    return jobId;
  }

  @Override
  public synchronized void setJobId(JobId jobId) {
    maybeInitBuilder();
    if (jobId == null)
      builder.clearJobId();
    this.jobId = jobId;
  }

  @Override
  public synchronized TaskType getTaskType() {
    TaskIdProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasTaskType()) {
      return null;
    }
    return convertFromProtoFormat(p.getTaskType());
  }

  @Override
  public synchronized void setTaskType(TaskType taskType) {
    maybeInitBuilder();
    if (taskType == null) {
      builder.clearTaskType();
      return;
    }
    builder.setTaskType(convertToProtoFormat(taskType));
  }

  /**
   * 将Protobuf格式的JobId转换为本地实现对象。
   * @param p Protobuf格式的JobIdProto
   * @return 本地JobIdPBImpl实例
   */
  private JobIdPBImpl convertFromProtoFormat(JobIdProto p) {
    return new JobIdPBImpl(p);
  }

  /**
   * 将本地JobId对象转换为Protobuf格式。
   * @param t 本地JobId实例
   * @return Protobuf格式的JobIdProto
   */
  private JobIdProto convertToProtoFormat(JobId t) {
    return ((JobIdPBImpl)t).getProto();
  }

  /**
   * 将本地TaskType枚举转换为Protobuf枚举。
   * @param e 本地TaskType枚举
   * @return Protobuf格式的TaskTypeProto枚举
   */
  private TaskTypeProto convertToProtoFormat(TaskType e) {
    return MRProtoUtils.convertToProtoFormat(e);
  }

  /**
   * 将Protobuf枚举转换为本地TaskType枚举。
   * @param e Protobuf格式的TaskTypeProto枚举
   * @return 本地TaskType枚举
   */
  private TaskType convertFromProtoFormat(TaskTypeProto e) {
    return MRProtoUtils.convertFromProtoFormat(e);
  }
}