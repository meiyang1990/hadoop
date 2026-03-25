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
package org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb;


import org.apache.hadoop.thirdparty.protobuf.TextFormat;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationIdPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.GetTimelineCollectorContextRequestProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.GetTimelineCollectorContextRequestProto;
import org.apache.hadoop.yarn.server.api.protocolrecords.GetTimelineCollectorContextRequest;

/**
 * 获取时间线收集器上下文请求的Protobuf实现类
 * 基于Protobuf序列化协议实现协议请求对象，负责YARN服务端间获取时间线收集器上下文的请求数据处理
 */
public class GetTimelineCollectorContextRequestPBImpl extends
    GetTimelineCollectorContextRequest {

  // Protobuf消息对象，当通过已有proto构造时使用
  private GetTimelineCollectorContextRequestProto
      proto = GetTimelineCollectorContextRequestProto.getDefaultInstance();
  // Protobuf消息构造器，当本地修改对象时使用
  private GetTimelineCollectorContextRequestProto.Builder builder = null;
  // 标记当前对象是否直接通过proto实例使用
  private boolean viaProto = false;

  // 缓存的应用ID对象，延迟从proto转换
  private ApplicationId appId = null;

  /**
   * 空构造函数，初始化构造器用于构建新请求
   */
  public GetTimelineCollectorContextRequestPBImpl() {
    builder = GetTimelineCollectorContextRequestProto.newBuilder();
  }

  /**
   * 基于已有Protobuf消息构造请求对象
   * @param proto 已有的Protobuf请求消息
   */
  public GetTimelineCollectorContextRequestPBImpl(
      GetTimelineCollectorContextRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  /**
   * 获取当前请求对应的Protobuf消息，合并本地修改后返回
   * @return 序列化后的Protobuf请求消息
   */
  public GetTimelineCollectorContextRequestProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  @Override
  public int hashCode() {
    return getProto().hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other == null) {
      return false;
    }
    if (other.getClass().isAssignableFrom(this.getClass())) {
      return this.getProto().equals(this.getClass().cast(other).getProto());
    }
    return false;
  }

  @Override
  public String toString() {
    return TextFormat.shortDebugString(getProto());
  }

  // 将本地缓存的应用ID合并到Protobuf构造器中
  private void mergeLocalToBuilder() {
    if (appId != null) {
      builder.setAppId(convertToProtoFormat(this.appId));
    }
  }

  // 将本地修改合并到Protobuf消息中
  private void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  // 如有需要，基于现有proto初始化构造器
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = GetTimelineCollectorContextRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public ApplicationId getApplicationId() {
    // 已缓存直接返回
    if (this.appId != null) {
      return this.appId;
    }

    // 根据当前状态获取proto或builder
    GetTimelineCollectorContextRequestProtoOrBuilder p =
        viaProto ? proto : builder;
    // proto中不存在该字段返回null
    if (!p.hasAppId()) {
      return null;
    }

    // 从proto转换得到应用ID对象并缓存
    this.appId = convertFromProtoFormat(p.getAppId());
    return this.appId;
  }

  @Override
  public void setApplicationId(ApplicationId id) {
    maybeInitBuilder();
    // 清除字段如果传入null
    if (id == null) {
      builder.clearAppId();
    }
    this.appId = id;
  }

  // 将Protobuf格式的ApplicationId转换为内部实现对象
  private ApplicationIdPBImpl convertFromProtoFormat(
      YarnProtos.ApplicationIdProto p) {
    return new ApplicationIdPBImpl(p);
  }

  // 将内部ApplicationId对象转换为Protobuf格式
  private YarnProtos.ApplicationIdProto convertToProtoFormat(ApplicationId t) {
    return ((ApplicationIdPBImpl)t).getProto();
  }
}