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
 * distributed under the License is distributed on an "AS IS" BASIS WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.yarn.server.federation.store.records.impl.pb;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.AddApplicationHomeSubClusterResponseProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.AddApplicationHomeSubClusterResponseProtoOrBuilder;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.SubClusterIdProto;
import org.apache.hadoop.yarn.server.federation.store.records.AddApplicationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;

import org.apache.hadoop.thirdparty.protobuf.TextFormat;

/**
 * AddApplicationHomeSubClusterResponse 的 Protocol Buffer 实现类，
 * 用于联邦集群元数据存储中添加应用归属子集群响应的序列化/反序列化。
 */
@Private
@Unstable
public class AddApplicationHomeSubClusterResponsePBImpl
    extends AddApplicationHomeSubClusterResponse {

  // 存储proto对象，当通过proto构建时使用
  private AddApplicationHomeSubClusterResponseProto proto =
      AddApplicationHomeSubClusterResponseProto.getDefaultInstance();
  // proto构建器，修改数据时使用
  private AddApplicationHomeSubClusterResponseProto.Builder builder = null;
  // 当前是否直接使用proto对象标识，false表示使用builder构建
  private boolean viaProto = false;

  /**
   * 空构造函数，初始化构建器。
   */
  public AddApplicationHomeSubClusterResponsePBImpl() {
    builder = AddApplicationHomeSubClusterResponseProto.newBuilder();
  }

  /**
   * 通过已有proto对象构造响应实例。
   * @param proto 已构造完成的proto对象
   */
  public AddApplicationHomeSubClusterResponsePBImpl(
      AddApplicationHomeSubClusterResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  /**
   * 初始化构建器，若当前使用proto则基于现有proto创建构建器。
   */
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = AddApplicationHomeSubClusterResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }

  /**
   * 获取当前响应对应的proto对象。
   * @return 构建完成的proto对象
   */
  public AddApplicationHomeSubClusterResponseProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  @Override
  public void setHomeSubCluster(SubClusterId homeSubCluster) {
    maybeInitBuilder();
    if (homeSubCluster == null) {
      builder.clearHomeSubCluster();
      return;
    }
    // 将业务对象转换为proto格式存入构建器
    builder.setHomeSubCluster(convertToProtoFormat(homeSubCluster));
  }

  @Override
  public SubClusterId getHomeSubCluster() {
    // 根据当前状态选择proto或builder
    AddApplicationHomeSubClusterResponseProtoOrBuilder p =
        viaProto ? proto : builder;

    if (!p.hasHomeSubCluster()) {
      return null;
    }
    // 将proto格式转换为业务对象返回
    return convertFromProtoFormat(p.getHomeSubCluster());
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

  /**
   * 将proto格式的子集群ID转换为业务对象。
   * @param sc proto格式子集群ID
   * @return PB实现的业务子集群ID对象
   */
  private SubClusterId convertFromProtoFormat(SubClusterIdProto sc) {
    return new SubClusterIdPBImpl(sc);
  }

  /**
   * 将业务子集群ID对象转换为proto格式。
   * @param sc 业务子集群ID对象
   * @return proto格式子集群ID
   */
  private SubClusterIdProto convertToProtoFormat(SubClusterId sc) {
    return ((SubClusterIdPBImpl) sc).getProto();
  }

}