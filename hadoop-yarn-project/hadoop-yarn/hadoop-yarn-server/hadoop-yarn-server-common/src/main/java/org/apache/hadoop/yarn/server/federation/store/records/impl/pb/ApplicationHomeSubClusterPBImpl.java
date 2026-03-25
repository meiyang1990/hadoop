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
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationSubmissionContextPBImpl;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.ApplicationHomeSubClusterProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.ApplicationHomeSubClusterProtoOrBuilder;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.SubClusterIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationSubmissionContextProto;
import org.apache.hadoop.yarn.server.federation.store.records.ApplicationHomeSubCluster;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;

import org.apache.hadoop.thirdparty.protobuf.TextFormat;

/**
 * 基于Protocol Buffer实现的{@link ApplicationHomeSubCluster}，用于在联邦状态存储中存储应用归属子集群信息
 */
@Private
@Unstable
public class ApplicationHomeSubClusterPBImpl extends ApplicationHomeSubCluster {

  // Protobuf消息对象，当通过proto读取时存储消息实例
  private ApplicationHomeSubClusterProto proto =
      ApplicationHomeSubClusterProto.getDefaultInstance();
  // Protobuf消息构造器，当需要修改消息时使用builder构造
  private ApplicationHomeSubClusterProto.Builder builder = null;
  // 标记当前数据是否已经同步到proto对象
  private boolean viaProto = false;

  // 缓存应用ID对象，避免重复从proto解析
  private ApplicationId applicationId = null;
  // 缓存应用归属子集群ID对象，避免重复从proto解析
  private SubClusterId homeSubCluster = null;
  // 缓存应用创建时间
  private long createTime = 0L;
  // 缓存应用提交上下文对象
  private ApplicationSubmissionContext applicationSubmissionContext;

  /**
   * 空构造函数，初始化Protobuf构造器
   */
  public ApplicationHomeSubClusterPBImpl() {
    builder = ApplicationHomeSubClusterProto.newBuilder();
  }

  /**
   * 基于已有Protobuf消息构造对象
   * @param proto 已有的ApplicationHomeSubClusterProto消息
   */
  public ApplicationHomeSubClusterPBImpl(ApplicationHomeSubClusterProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  /**
   * 获取当前对象对应的Protobuf消息，自动合并本地缓存修改
   * @return 合并后的完整Protobuf消息
   */
  public ApplicationHomeSubClusterProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  /**
   * 将本地缓存的修改合并到Protobuf消息对象
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
   * 如果当前是proto模式，初始化构造器以便修改
   */
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = ApplicationHomeSubClusterProto.newBuilder(proto);
    }
    viaProto = false;
  }

  /**
   * 将本地缓存的Java对象合并到Protobuf构造器
   */
  private void mergeLocalToBuilder() {
    if (this.applicationId != null) {
      builder.setApplicationId(convertToProtoFormat(this.applicationId));
    }
    if (this.homeSubCluster != null) {
      builder.setHomeSubCluster(convertToProtoFormat(this.homeSubCluster));
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
    ApplicationHomeSubClusterProtoOrBuilder p = viaProto ? proto : builder;
    if (this.applicationId != null) {
      return this.applicationId;
    }
    if (!p.hasApplicationId()) {
      return null;
    }
    this.applicationId = convertFromProtoFormat(p.getApplicationId());
    return applicationId;
  }

  @Override
  public void setApplicationId(ApplicationId applicationId) {
    maybeInitBuilder();
    if (applicationId == null) {
      builder.clearApplicationId();
      return;
    }
    this.applicationId = applicationId;
    builder.setApplicationId(convertToProtoFormat(applicationId));
  }

  @Override
  public SubClusterId getHomeSubCluster() {
    ApplicationHomeSubClusterProtoOrBuilder p = viaProto ? proto : builder;
    if (this.homeSubCluster != null) {
      return this.homeSubCluster;
    }
    if (!p.hasHomeSubCluster()) {
      return null;
    }
    this.homeSubCluster = convertFromProtoFormat(p.getHomeSubCluster());
    return this.homeSubCluster;
  }

  @Override
  public void setHomeSubCluster(SubClusterId paramHomeSubCluster) {
    maybeInitBuilder();
    if (paramHomeSubCluster == null) {
      builder.clearHomeSubCluster();
      return;
    }
    this.homeSubCluster = paramHomeSubCluster;
    builder.setHomeSubCluster(convertToProtoFormat(paramHomeSubCluster));
  }

  @Override
  public long getCreateTime() {
    ApplicationHomeSubClusterProtoOrBuilder p = viaProto ? proto : builder;
    if (this.createTime != 0) {
      return this.createTime;
    }
    if (!p.hasCreateTime()) {
      return 0;
    }
    this.createTime = p.getCreateTime();
    return this.createTime;
  }

  @Override
  public void setCreateTime(long time) {
    maybeInitBuilder();
    this.createTime = time;
    builder.setCreateTime(time);
  }

  @Override
  public void setApplicationSubmissionContext(ApplicationSubmissionContext context) {
    maybeInitBuilder();
    if (context == null) {
      builder.clearAppSubmitContext();
      return;
    }
    this.applicationSubmissionContext = context;
    builder.setAppSubmitContext(convertToProtoFormat(context));
  }

  @Override
  public ApplicationSubmissionContext getApplicationSubmissionContext() {
    ApplicationHomeSubClusterProtoOrBuilder p = viaProto ? proto : builder;
    if (this.applicationSubmissionContext != null) {
      return this.applicationSubmissionContext;
    }
    if (!p.hasAppSubmitContext()) {
      return null;
    }
    this.applicationSubmissionContext = convertFromProtoFormat(p.getAppSubmitContext());
    return applicationSubmissionContext;
  }

  /** 将Protobuf格式的SubClusterId转换为Java对象 */
  private SubClusterId convertFromProtoFormat(SubClusterIdProto subClusterId) {
    return new SubClusterIdPBImpl(subClusterId);
  }

  /** 将Java格式的SubClusterId转换为Protobuf对象 */
  private SubClusterIdProto convertToProtoFormat(SubClusterId subClusterId) {
    return ((SubClusterIdPBImpl) subClusterId).getProto();
  }

  /** 将Protobuf格式的ApplicationId转换为Java对象 */
  private ApplicationId convertFromProtoFormat(ApplicationIdProto appId) {
    return new ApplicationIdPBImpl(appId);
  }

  /** 将Java格式的ApplicationId转换为Protobuf对象 */
  private ApplicationIdProto convertToProtoFormat(ApplicationId appId) {
    return ((ApplicationIdPBImpl) appId).getProto();
  }

  /** 将Protobuf格式的ApplicationSubmissionContext转换为Java对象 */
  private ApplicationSubmissionContext convertFromProtoFormat(
      ApplicationSubmissionContextProto appSubmitContext) {
    return new ApplicationSubmissionContextPBImpl(appSubmitContext);
  }

  /** 将Java格式的ApplicationSubmissionContext转换为Protobuf对象 */
  private ApplicationSubmissionContextProto convertToProtoFormat(
      ApplicationSubmissionContext appContext) {
    return ((ApplicationSubmissionContextPBImpl) appContext).getProto();
  }
}