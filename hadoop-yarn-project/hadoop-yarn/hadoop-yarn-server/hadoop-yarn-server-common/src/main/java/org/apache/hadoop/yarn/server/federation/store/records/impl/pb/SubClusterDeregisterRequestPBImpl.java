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
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.SubClusterDeregisterRequestProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.SubClusterDeregisterRequestProtoOrBuilder;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.SubClusterIdProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.SubClusterStateProto;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterDeregisterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterState;

import org.apache.hadoop.thirdparty.protobuf.TextFormat;

/**
 * 文件级注释：基于Protocol Buffer实现的子集群注销请求，用于YARN联邦模式下向联邦状态存储注册子集群下线请求
 * Protocol buffer based implementation of {@link SubClusterDeregisterRequest}.
 */
@Private
@Unstable
public class SubClusterDeregisterRequestPBImpl
    extends SubClusterDeregisterRequest {

  // 缓存的PB协议对象，当通过已构建的proto实例构造时使用
  private SubClusterDeregisterRequestProto proto =
      SubClusterDeregisterRequestProto.getDefaultInstance();
  // PB协议构建器，当新建或修改对象时使用
  private SubClusterDeregisterRequestProto.Builder builder = null;
  // 标记当前是否通过已构建的proto实例使用，false表示当前正在通过builder修改
  private boolean viaProto = false;

  /**
   * 新建空的子集群注销请求对象，初始化PB构建器
   */
  public SubClusterDeregisterRequestPBImpl() {
    builder = SubClusterDeregisterRequestProto.newBuilder();
  }

  /**
   * 基于已有PB proto实例构造子集群注销请求对象
   * @param proto 已构建的PB协议实例
   */
  public SubClusterDeregisterRequestPBImpl(
      SubClusterDeregisterRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  /**
   * 获取当前对象对应的PB协议实例，合并本地修改后返回
   * @return 构建完成的PB协议实例
   */
  public SubClusterDeregisterRequestProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  // 将本地修改合并到proto实例中
  private void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  // 延迟初始化PB构建器：如果当前通过proto实例，基于现有proto创建构建器
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = SubClusterDeregisterRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  // 合并本地对象修改到PB构建器，本实现无额外本地字段，留空
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
    // 根据当前状态选择proto或builder
    SubClusterDeregisterRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasSubClusterId()) {
      return null;
    }
    // 将PB格式转换为业务对象格式返回
    return convertFromProtoFormat(p.getSubClusterId());
  }

  @Override
  public void setSubClusterId(SubClusterId subClusterId) {
    // 确保builder已初始化
    maybeInitBuilder();
    if (subClusterId == null) {
      builder.clearSubClusterId();
      return;
    }
    // 将业务对象转换为PB格式设置到构建器
    builder.setSubClusterId(convertToProtoFormat(subClusterId));
  }

  @Override
  public SubClusterState getState() {
    // 根据当前状态选择proto或builder
    SubClusterDeregisterRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasState()) {
      return null;
    }
    // 将PB格式转换为业务枚举返回
    return convertFromProtoFormat(p.getState());
  }

  @Override
  public void setState(SubClusterState state) {
    // 确保builder已初始化
    maybeInitBuilder();
    if (state == null) {
      builder.clearState();
      return;
    }
    // 将业务枚举转换为PB格式设置到构建器
    builder.setState(convertToProtoFormat(state));
  }

  // 将PB格式的子集群ID转换为业务对象
  private SubClusterId convertFromProtoFormat(SubClusterIdProto sc) {
    return new SubClusterIdPBImpl(sc);
  }

  // 将业务格式的子集群ID转换为PB协议对象
  private SubClusterIdProto convertToProtoFormat(SubClusterId sc) {
    return ((SubClusterIdPBImpl) sc).getProto();
  }

  // 将PB格式的子集群状态转换为业务枚举
  private SubClusterState convertFromProtoFormat(SubClusterStateProto state) {
    return SubClusterState.valueOf(state.name());
  }

  // 将业务格式的子集群状态转换为PB枚举
  private SubClusterStateProto convertToProtoFormat(SubClusterState state) {
    return SubClusterStateProto.valueOf(state.name());
  }

}