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
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.ApplicationHomeSubClusterProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.UpdateApplicationHomeSubClusterRequestProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.UpdateApplicationHomeSubClusterRequestProtoOrBuilder;
import org.apache.hadoop.yarn.server.federation.store.records.ApplicationHomeSubCluster;
import org.apache.hadoop.yarn.server.federation.store.records.UpdateApplicationHomeSubClusterRequest;

import org.apache.hadoop.thirdparty.protobuf.TextFormat;

/**
 * 基于Protocol Buffer实现的{@link UpdateApplicationHomeSubClusterRequest}，
 * 用于YARN联邦场景下更新应用归属子集群请求的PB序列化实现。
 */
@Private
@Unstable
public class UpdateApplicationHomeSubClusterRequestPBImpl
    extends UpdateApplicationHomeSubClusterRequest {

  // 存储请求的PB实例，通过viaProto标记当前使用哪种存储方式
  private UpdateApplicationHomeSubClusterRequestProto proto =
      UpdateApplicationHomeSubClusterRequestProto.getDefaultInstance();
  // PB构建器，当修改数据时使用builder构建
  private UpdateApplicationHomeSubClusterRequestProto.Builder builder = null;
  // 标记当前数据是否直接存储在proto中，false表示正在使用builder修改
  private boolean viaProto = false;

  /**
   * 无参构造，初始化PB构建器。
   */
  public UpdateApplicationHomeSubClusterRequestPBImpl() {
    builder = UpdateApplicationHomeSubClusterRequestProto.newBuilder();
  }

  /**
   * 基于已有PB实例构造请求对象。
   * @param proto 已有的PB请求实例
   */
  public UpdateApplicationHomeSubClusterRequestPBImpl(
      UpdateApplicationHomeSubClusterRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  /**
   * 获取当前请求的PB实例，合并本地修改后返回。
   * @return 序列化后的PB请求实例
   */
  public UpdateApplicationHomeSubClusterRequestProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  // 将本地builder中的修改合并到proto实例中
  private void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  // 如果当前数据在proto中，初始化builder准备修改
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = UpdateApplicationHomeSubClusterRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  // 合并本地修改到builder，当前无本地缓存字段，留空实现
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
    // 根据存储方式选择proto或builder读取数据
    UpdateApplicationHomeSubClusterRequestProtoOrBuilder p =
        viaProto ? proto : builder;
    if (!p.hasAppSubclusterMap()) {
      return null;
    }
    // 将PB格式转换为API对象返回
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
    // 将API对象转换为PB格式并设置到builder
    builder.setAppSubclusterMap(convertToProtoFormat(applicationInfo));
  }

  // 将PB格式转换为应用归属子集群API对象
  private ApplicationHomeSubCluster convertFromProtoFormat(
      ApplicationHomeSubClusterProto sc) {
    return new ApplicationHomeSubClusterPBImpl(sc);
  }

  // 将应用归属子集群API对象转换为PB格式
  private ApplicationHomeSubClusterProto convertToProtoFormat(
      ApplicationHomeSubCluster sc) {
    return ((ApplicationHomeSubClusterPBImpl) sc).getProto();
  }

}