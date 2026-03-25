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
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.AddApplicationHomeSubClusterRequestProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.AddApplicationHomeSubClusterRequestProtoOrBuilder;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.ApplicationHomeSubClusterProto;
import org.apache.hadoop.yarn.server.federation.store.records.AddApplicationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.ApplicationHomeSubCluster;

import org.apache.hadoop.thirdparty.protobuf.TextFormat;

/**
 * 基于ProtocolBuffer实现的{@link AddApplicationHomeSubClusterRequest}，用于YARN联邦状态存储添加应用归属子集群请求的序列化
 */
@Private
@Unstable
public class AddApplicationHomeSubClusterRequestPBImpl
    extends AddApplicationHomeSubClusterRequest {

  // Proto缓存实例
  private AddApplicationHomeSubClusterRequestProto proto =
      AddApplicationHomeSubClusterRequestProto.getDefaultInstance();
  // Proto构建器，用于修改请求内容
  private AddApplicationHomeSubClusterRequestProto.Builder builder = null;
  // 标记当前是否通过Proto实例构建
  private boolean viaProto = false;

  /**
   * 空构造函数，初始化Proto构建器
   */
  public AddApplicationHomeSubClusterRequestPBImpl() {
    builder = AddApplicationHomeSubClusterRequestProto.newBuilder();
  }

  /**
   * 通过已有Proto实例构造请求对象
   * @param proto 已有的AddApplicationHomeSubClusterRequestProto实例
   */
  public AddApplicationHomeSubClusterRequestPBImpl(
      AddApplicationHomeSubClusterRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  /**
   * 获取当前请求对应的Proto实例，合并本地修改后返回
   * @return 合并后的Proto实例
   */
  public AddApplicationHomeSubClusterRequestProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  // 将本地修改合并到Proto实例
  private void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  // 延迟初始化Proto构建器，基于已有Proto创建
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = AddApplicationHomeSubClusterRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  // 合并本地字段到构建器，当前无额外本地字段，留空
  private void mergeLocalToBuilder() {
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
  public ApplicationHomeSubCluster getApplicationHomeSubCluster() {
    AddApplicationHomeSubClusterRequestProtoOrBuilder p =
        viaProto ? proto : builder;
    if (!p.hasAppSubclusterMap()) {
      return null;
    }
    // 将Proto格式转换为应用归属子集群对象
    return convertFromProtoFormat(p.getAppSubclusterMap());
  }

  @Override
  public void setApplicationHomeSubCluster(
      ApplicationHomeSubCluster applicationInfo) {
    maybeInitBuilder();
    if (applicationInfo == null) {
      builder.clearAppSubclusterMap();
      return;
    }
    // 将应用归属子集群对象转换为Proto格式存入构建器
    builder.setAppSubclusterMap(convertToProtoFormat(applicationInfo));
  }

  // Proto格式转业务对象
  private ApplicationHomeSubCluster convertFromProtoFormat(
      ApplicationHomeSubClusterProto sc) {
    return new ApplicationHomeSubClusterPBImpl(sc);
  }

  // 业务对象转Proto格式
  private ApplicationHomeSubClusterProto convertToProtoFormat(
      ApplicationHomeSubCluster sc) {
    return ((ApplicationHomeSubClusterPBImpl) sc).getProto();
  }

}