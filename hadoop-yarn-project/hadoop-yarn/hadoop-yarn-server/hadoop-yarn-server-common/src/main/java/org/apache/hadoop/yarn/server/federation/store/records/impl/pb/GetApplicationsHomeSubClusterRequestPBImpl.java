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
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.GetApplicationsHomeSubClusterRequestProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.GetApplicationsHomeSubClusterRequestProtoOrBuilder;
import org.apache.hadoop.yarn.server.federation.store.records.GetApplicationsHomeSubClusterRequest;

import org.apache.hadoop.thirdparty.protobuf.TextFormat;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;

/**
 * GetApplicationsHomeSubClusterRequest 的 Protocol Buffer 实现，
 * 用于 federation 状态存储中查询指定子集群应用归属请求的序列化/反序列化。
 */
@Private
@Unstable
public class GetApplicationsHomeSubClusterRequestPBImpl
    extends GetApplicationsHomeSubClusterRequest {

  // PB 协议对象实例
  private GetApplicationsHomeSubClusterRequestProto proto =
      GetApplicationsHomeSubClusterRequestProto.getDefaultInstance();
  // PB 构建器实例
  private GetApplicationsHomeSubClusterRequestProto.Builder builder = null;
  // 是否直接使用 proto 模式，false 表示正在使用构建器修改
  private boolean viaProto = false;

  /**
   * 构造函数，初始化 PB 构建器。
   */
  public GetApplicationsHomeSubClusterRequestPBImpl() {
    builder = GetApplicationsHomeSubClusterRequestProto.newBuilder();
  }

  /**
   * 基于已有 PB 对象构造封装。
   * @param proto PB 协议对象
   */
  public GetApplicationsHomeSubClusterRequestPBImpl(
      GetApplicationsHomeSubClusterRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  /**
   * 获取当前请求的 PB 协议对象。
   * @return 构建完成的 PB 对象
   */
  public GetApplicationsHomeSubClusterRequestProto getProto() {
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

  // 初始化构建器，若当前使用只读 proto，则基于 proto 新建可写构建器
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = GetApplicationsHomeSubClusterRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public SubClusterId getSubClusterId() {
    GetApplicationsHomeSubClusterRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasSubClusterId()) {
      return null;
    }
    // 将 PB 格式子集群ID转换为 API 对象
    return convertFromProtoFormat(p.getSubClusterId());
  }

  @Override
  public void setSubClusterId(SubClusterId subClusterId) {
    maybeInitBuilder();
    if (subClusterId == null) {
      builder.clearSubClusterId();
      return;
    }
    // 将 API 对象转换为 PB 格式存入构建器
    builder.setSubClusterId(convertToProtoFormat(subClusterId));
  }

  // 将 PB 格式子集群ID转换为 API 实现对象
  private SubClusterId convertFromProtoFormat(YarnServerFederationProtos.SubClusterIdProto sc) {
    return new SubClusterIdPBImpl(sc);
  }

  // 将 API 对象转换为 PB 格式子集群ID
  private YarnServerFederationProtos.SubClusterIdProto convertToProtoFormat(SubClusterId sc) {
    return ((SubClusterIdPBImpl) sc).getProto();
  }
}