// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.yarn.server.federation.store.records.impl.pb;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationIdPBImpl;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.GetApplicationHomeSubClusterRequestProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.GetApplicationHomeSubClusterRequestProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationIdProto;
import org.apache.hadoop.yarn.server.federation.store.records.GetApplicationHomeSubClusterRequest;

import org.apache.hadoop.thirdparty.protobuf.TextFormat;

/**
 * 基于Protocol Buffer实现的{@link GetApplicationHomeSubClusterRequest}，用于联邦场景下查询应用归属子集群请求的PB序列化
 */
@Private
@Unstable
public class GetApplicationHomeSubClusterRequestPBImpl
    extends GetApplicationHomeSubClusterRequest {

  // PB协议对象，只读模式下使用
  private GetApplicationHomeSubClusterRequestProto proto =
      GetApplicationHomeSubClusterRequestProto.getDefaultInstance();
  // PB构建器，可写模式下使用
  private GetApplicationHomeSubClusterRequestProto.Builder builder = null;
  // 当前是否通过proto对象存储数据（false表示当前数据在builder中）
  private boolean viaProto = false;

  // 缓存的应用ID对象
  private ApplicationId applicationId = null;

  /**
   * 构造函数，初始化空的PB构建器
   */
  public GetApplicationHomeSubClusterRequestPBImpl() {
    builder = GetApplicationHomeSubClusterRequestProto.newBuilder();
  }

  /**
   * 基于已有PB对象构造请求实例
   * @param proto 已构造好的PB协议对象
   */
  public GetApplicationHomeSubClusterRequestPBImpl(
      GetApplicationHomeSubClusterRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  /**
   * 获取当前请求对应的最终PB协议对象，合并本地缓存数据后生成
   * @return 序列化可用的PB协议对象
   */
  public GetApplicationHomeSubClusterRequestProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  /**
   * 将本地缓存的字段数据合并到PB协议对象中
   */
  private void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  /**
   * 如果当前是只读模式，初始化可写的PB构建器
   */
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = GetApplicationHomeSubClusterRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  /**
   * 将本地缓存的Java对象字段写入PB构建器
   */
  private void mergeLocalToBuilder() {
    if (this.applicationId != null) {
      builder.setApplicationId(convertToProtoFormat(this.applicationId));
    }
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

  @Override
  public ApplicationId getApplicationId() {
    // 根据当前存储模式获取对应的协议对象/构建器
    GetApplicationHomeSubClusterRequestProtoOrBuilder p =
        viaProto ? proto : builder;
    // 已缓存直接返回
    if (applicationId != null) {
      return applicationId;
    }

    // PB中不存在该字段返回null
    if (!p.hasApplicationId()) {
      return null;
    }
    // 从PB反序列化为Java对象并缓存
    this.applicationId = convertFromProtoFormat(p.getApplicationId());
    return applicationId;
  }

  @Override
  public void setApplicationId(ApplicationId applicationId) {
    maybeInitBuilder();
    // 清空字段处理
    if (applicationId == null) {
      builder.clearApplicationId();
      return;
    }
    // 缓存Java对象，写入PB构建器
    this.applicationId = applicationId;
    builder.setApplicationId(convertToProtoFormat(applicationId));
  }

  @Override
  public boolean getContainsAppSubmissionContext() {
    GetApplicationHomeSubClusterRequestProtoOrBuilder p = viaProto ? proto : builder;
    // 获取请求是否需要包含应用提交上下文标记
    return p.getContainsAppSubmissionContext();
  }

  @Override
  public void setContainsAppSubmissionContext(boolean containsAppSubmissionContext) {
    maybeInitBuilder();
    // 设置是否需要包含应用提交上下文标记
    builder.setContainsAppSubmissionContext(containsAppSubmissionContext);
  }

  /**
   * 将PB格式的ApplicationId转换为Yarn API对象
   * @param appId PB格式应用ID
   * @return Yarn API应用ID对象
   */
  private ApplicationId convertFromProtoFormat(ApplicationIdProto appId) {
    return new ApplicationIdPBImpl(appId);
  }

  /**
   * 将Yarn API格式的ApplicationId转换为PB格式
   * @param appId Yarn API应用ID对象
   * @return PB格式应用ID
   */
  private ApplicationIdProto convertToProtoFormat(ApplicationId appId) {
    return ((ApplicationIdPBImpl) appId).getProto();
  }
}