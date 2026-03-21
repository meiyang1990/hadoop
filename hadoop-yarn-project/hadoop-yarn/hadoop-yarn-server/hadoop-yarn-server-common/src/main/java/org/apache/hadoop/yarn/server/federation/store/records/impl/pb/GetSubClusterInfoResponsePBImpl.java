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
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.GetSubClusterInfoResponseProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.GetSubClusterInfoResponseProtoOrBuilder;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.SubClusterInfoProto;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterInfoResponse;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;

import org.apache.hadoop.thirdparty.protobuf.TextFormat;

/**
 * 文件级注释：基于Protocol Buffer实现的获取子集群信息响应类，为YARN联邦元数据存储提供PB序列化实现
 * Protocol buffer based implementation of {@link GetSubClusterInfoResponse}.
 */
@Private
@Unstable
public class GetSubClusterInfoResponsePBImpl extends GetSubClusterInfoResponse {

  // PB协议对象，存储序列化后的响应数据
  private GetSubClusterInfoResponseProto proto =
      GetSubClusterInfoResponseProto.getDefaultInstance();
  // PB构建器，用于构建修改响应数据
  private GetSubClusterInfoResponseProto.Builder builder = null;
  // 标记当前是否通过proto对象存储数据
  private boolean viaProto = false;

  // 缓存子集群信息对象，避免重复反序列化
  private SubClusterInfo subClusterInfo = null;

  /**
   * 构造函数，初始化空构建器
   */
  public GetSubClusterInfoResponsePBImpl() {
    builder = GetSubClusterInfoResponseProto.newBuilder();
  }

  /**
   * 基于已有PB对象构造响应实例
   * @param proto 已有的PB格式响应对象
   */
  public GetSubClusterInfoResponsePBImpl(GetSubClusterInfoResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  /**
   * 获取当前响应的PB对象，合并本地修改后返回
   * @return 序列化后的PB对象
   */
  public GetSubClusterInfoResponseProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  // 将本地修改合并到PB对象
  private void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  // 初始化构建器，如果当前基于proto则从proto创建构建器
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = GetSubClusterInfoResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }

  // 将本地缓存的子集群信息合并到构建器
  private void mergeLocalToBuilder() {
    if (this.subClusterInfo != null) {
      builder.setSubClusterInfo(convertToProtoFormat(this.subClusterInfo));
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
  public SubClusterInfo getSubClusterInfo() {
    GetSubClusterInfoResponseProtoOrBuilder p = viaProto ? proto : builder;
    // 返回缓存如果已存在
    if (this.subClusterInfo != null) {
      return this.subClusterInfo;
    }
    // PB中不存在子集群信息则返回null
    if (!p.hasSubClusterInfo()) {
      return null;
    }
    // 从PB反序列化为API对象并缓存
    this.subClusterInfo = convertFromProtoFormat(p.getSubClusterInfo());
    return this.subClusterInfo;
  }

  @Override
  public void setSubClusterInfo(SubClusterInfo paramSubClusterInfo) {
    maybeInitBuilder();
    // 清空子集群信息如果传入null
    if (paramSubClusterInfo == null) {
      builder.clearSubClusterInfo();
      return;
    }
    // 缓存API对象，转换为PB格式存入构建器
    this.subClusterInfo = paramSubClusterInfo;
    builder.setSubClusterInfo(convertToProtoFormat(paramSubClusterInfo));
  }

  // 将PB格式子集群信息转换为API对象
  private SubClusterInfo convertFromProtoFormat(
      SubClusterInfoProto clusterInfo) {
    return new SubClusterInfoPBImpl(clusterInfo);
  }

  // 将API格式子集群信息转换为PB对象
  private SubClusterInfoProto convertToProtoFormat(SubClusterInfo clusterInfo) {
    return ((SubClusterInfoPBImpl) clusterInfo).getProto();
  }

}