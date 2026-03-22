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
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.JobIdProto;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.JobIdProtoOrBuilder;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationIdPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationIdProto;

/**
 * JobId的Protobuf实现类，基于Protobuf序列化协议存储作业ID信息，
 * 在MapReduce V2的RPC通信和持久化中承载作业唯一标识。
 */
public class JobIdPBImpl extends JobId {

  // 默认Protobuf对象实例
  JobIdProto proto = JobIdProto.getDefaultInstance();
  // Protobuf构建器，用于构建对象
  JobIdProto.Builder builder = null;
  // 标识当前是否通过Protobuf对象存储数据
  boolean viaProto = false;
  
  // 缓存所属应用ID对象，避免重复转换
  private ApplicationId applicationId = null;

  /**
   * 无参构造函数，初始化Protobuf构建器。
   */
  public JobIdPBImpl() {
    builder = JobIdProto.newBuilder();
  }

  /**
   * 基于已有的JobIdProto构造JobIdPBImpl。
   * @param proto 已构造完成的JobIdProto对象
   */
  public JobIdPBImpl(JobIdProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  /**
   * 获取当前对象对应的JobIdProto实例，处理本地字段与Proto的合并。
   * @return 合并完成的JobIdProto实例
   */
  public synchronized JobIdProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  /**
   * 将本地缓存的字段合并到Protobuf构建器中。
   */
  private synchronized void mergeLocalToBuilder() {
    if (this.applicationId != null
        && !((ApplicationIdPBImpl) this.applicationId).getProto().equals(
            builder.getAppId())) {
      builder.setAppId(convertToProtoFormat(this.applicationId));
    }
  }

  /**
   * 将本地缓存的字段合并到Protobuf对象中。
   */
  private synchronized void mergeLocalToProto() {
    if (viaProto) 
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  /**
   * 如果当前是Proto存储模式，初始化构建器用于修改。
   */
  private synchronized void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = JobIdProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public synchronized ApplicationId getAppId() {
    JobIdProtoOrBuilder p = viaProto ? proto : builder;
    if (applicationId != null) {
      return applicationId;
    } // 从Protobuf中读取
    if (!p.hasAppId()) {
      return null;
    }
    // 将Protobuf格式转换为ApplicationId对象并缓存
    applicationId = convertFromProtoFormat(p.getAppId());
    return applicationId;
  }

  @Override
  public synchronized void setAppId(ApplicationId appId) {
    maybeInitBuilder();
    if (appId == null) {
      builder.clearAppId();
    }
    // 缓存设置的应用ID对象
    this.applicationId = appId;
  }

  @Override
  public synchronized int getId() {
    JobIdProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getId());
  }

  @Override
  public synchronized void setId(int id) {
    maybeInitBuilder();
    builder.setId((id));
  }

  /**
   * 将Protobuf格式的ApplicationId转换为Yarn ApplicationId对象。
   * @param p Protobuf格式的ApplicationId
   * @return 转换后的ApplicationIdPBImpl实例
   */
  private ApplicationIdPBImpl convertFromProtoFormat(
      ApplicationIdProto p) {
    return new ApplicationIdPBImpl(p);
  }

  /**
   * 将Yarn ApplicationId对象转换为Protobuf格式。
   * @param t ApplicationId对象
   * @return Protobuf格式的ApplicationIdProto
   */
  private ApplicationIdProto convertToProtoFormat(ApplicationId t) {
    return ((ApplicationIdPBImpl) t).getProto();
  }
}