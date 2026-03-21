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
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.GetSubClusterInfoRequestProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.GetSubClusterInfoRequestProtoOrBuilder;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.SubClusterIdProto;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterInfoRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;

import org.apache.hadoop.thirdparty.protobuf.TextFormat;

/**
 * 文件说明：获取子集群信息请求的Protobuf实现类，基于ProtocolBuffer序列化实现
 * Protocol buffer based implementation of {@link GetSubClusterInfoRequest}.
 */
@Private
@Unstable
public class GetSubClusterInfoRequestPBImpl extends GetSubClusterInfoRequest {

  // Protobuf消息对象，当通过已有proto构建时使用
  private GetSubClusterInfoRequestProto proto =
      GetSubClusterInfoRequestProto.getDefaultInstance();
  // Protobuf构建器，当构建新消息时使用
  private GetSubClusterInfoRequestProto.Builder builder = null;
  // 标识当前是否通过proto对象存储数据
  private boolean viaProto = false;

  /**
   * 构造空的获取子集群信息请求对象
   */
  public GetSubClusterInfoRequestPBImpl() {
    builder = GetSubClusterInfoRequestProto.newBuilder();
  }

  /**
   * 通过已有Protobuf对象构造请求对象
   * @param proto Protobuf格式的请求对象
   */
  public GetSubClusterInfoRequestPBImpl(GetSubClusterInfoRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  /**
   * 获取当前请求的Protobuf对象，合并本地修改后返回
   * @return 构建完成的Protobuf请求对象
   */
  public GetSubClusterInfoRequestProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  // 将本地修改合并到Protobuf对象
  private void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  // 初始化Builder，如果当前是proto模式则基于现有proto构建
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = GetSubClusterInfoRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  // 合并本地字段到Builder，当前无额外本地字段，留空实现
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
  public SubClusterId getSubClusterId() {
    // 根据当前存储模式选择proto或builder
    GetSubClusterInfoRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasSubClusterId()) {
      return null;
    }
    // 将Protobuf格式转换为API对象返回
    return convertFromProtoFormat(p.getSubClusterId());
  }

  @Override
  public void setSubClusterId(SubClusterId subClusterId) {
    maybeInitBuilder();
    if (subClusterId == null) {
      builder.clearSubClusterId();
      return;
    }
    // 将API对象转换为Protobuf格式存入builder
    builder.setSubClusterId(convertToProtoFormat(subClusterId));
  }

  // Protobuf格式转换为SubClusterId API对象
  private SubClusterId convertFromProtoFormat(SubClusterIdProto sc) {
    return new SubClusterIdPBImpl(sc);
  }

  // SubClusterId API对象转换为Protobuf格式
  private SubClusterIdProto convertToProtoFormat(SubClusterId sc) {
    return ((SubClusterIdPBImpl) sc).getProto();
  }

}