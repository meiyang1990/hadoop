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
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.GetApplicationHomeSubClusterResponseProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.GetApplicationHomeSubClusterResponseProtoOrBuilder;
import org.apache.hadoop.yarn.server.federation.store.records.ApplicationHomeSubCluster;
import org.apache.hadoop.yarn.server.federation.store.records.GetApplicationHomeSubClusterResponse;

import org.apache.hadoop.thirdparty.protobuf.TextFormat;

/**
 * 文件功能：基于Protocol Buffer实现的获取应用归属子集群响应对象
 * 实现了{@link GetApplicationHomeSubClusterResponse}接口，用于联邦状态存储的RPC序列化
 */
@Private
@Unstable
public class GetApplicationHomeSubClusterResponsePBImpl
    extends GetApplicationHomeSubClusterResponse {

  // 底层存储的Protocol Buffer响应对象实例
  private GetApplicationHomeSubClusterResponseProto proto =
      GetApplicationHomeSubClusterResponseProto.getDefaultInstance();
  // Protocol Buffer构建器，用于构建修改对象
  private GetApplicationHomeSubClusterResponseProto.Builder builder = null;
  // 标识当前数据是否存储在proto对象中
  private boolean viaProto = false;

  /**
   * 构造函数，初始化空的构建器
   */
  public GetApplicationHomeSubClusterResponsePBImpl() {
    builder = GetApplicationHomeSubClusterResponseProto.newBuilder();
  }

  /**
   * 基于已有proto对象构造响应实例
   * @param proto 已构造好的Protocol Buffer响应对象
   */
  public GetApplicationHomeSubClusterResponsePBImpl(
      GetApplicationHomeSubClusterResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  /**
   * 获取当前对象对应的Protocol Buffer原型
   * @return 序列化后的proto对象
   */
  public GetApplicationHomeSubClusterResponseProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  // 将本地修改合并到proto对象
  private void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  // 延迟初始化builder，从现有proto对象拷贝数据
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = GetApplicationHomeSubClusterResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }

  // 合并本地修改到builder，本实现无额外本地字段，留空
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
    // 根据当前存储模式选择proto或builder
    GetApplicationHomeSubClusterResponseProtoOrBuilder p =
        viaProto ? proto : builder;
    // 如果不存在应用归属信息，返回null
    if (!p.hasAppSubclusterMap()) {
      return null;
    }
    // 将proto格式转换为业务对象
    return convertFromProtoFormat(p.getAppSubclusterMap());
  }

  @Override
  public void setApplicationHomeSubCluster(
      ApplicationHomeSubCluster applicationInfo) {
    maybeInitBuilder();
    // 如果传入null，清空原有字段
    if (applicationInfo == null) {
      builder.clearAppSubclusterMap();
      return;
    }
    // 将业务对象转换为proto格式并设置
    builder.setAppSubclusterMap(convertToProtoFormat(applicationInfo));
  }

  // 将proto格式转换为应用归属子集群业务对象
  private ApplicationHomeSubCluster convertFromProtoFormat(
      ApplicationHomeSubClusterProto sc) {
    return new ApplicationHomeSubClusterPBImpl(sc);
  }

  // 将应用归属子集群业务对象转换为proto格式
  private ApplicationHomeSubClusterProto convertToProtoFormat(
      ApplicationHomeSubCluster sc) {
    return ((ApplicationHomeSubClusterPBImpl) sc).getProto();
  }

}