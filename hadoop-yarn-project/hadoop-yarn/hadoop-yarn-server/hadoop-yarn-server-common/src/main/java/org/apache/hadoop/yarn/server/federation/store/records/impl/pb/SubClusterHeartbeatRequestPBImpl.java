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
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.SubClusterHeartbeatRequestProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.SubClusterHeartbeatRequestProtoOrBuilder;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.SubClusterIdProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.SubClusterStateProto;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterHeartbeatRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterState;

import org.apache.hadoop.thirdparty.protobuf.TextFormat;

/**
 * 基于Protocol Buffer实现的子集群心跳请求类，用于联邦存储服务中子集群上报心跳
 * Protocol buffer based implementation of {@link SubClusterHeartbeatRequest}.
 */
@Private
@Unstable
public class SubClusterHeartbeatRequestPBImpl
    extends SubClusterHeartbeatRequest {

  // Proto对象实例，当通过已有proto构建时使用
  private SubClusterHeartbeatRequestProto proto =
      SubClusterHeartbeatRequestProto.getDefaultInstance();
  // Proto构建器，当修改对象状态时使用
  private SubClusterHeartbeatRequestProto.Builder builder = null;
  // 标记当前是否直接使用proto对象存储数据
  private boolean viaProto = false;

  // 缓存的子集群ID对象，避免重复转换
  private SubClusterId subClusterId = null;

  /**
   * 构造空的子集群心跳请求对象，初始化构建器
   */
  public SubClusterHeartbeatRequestPBImpl() {
    builder = SubClusterHeartbeatRequestProto.newBuilder();
  }

  /**
   * 基于已有proto对象构造心跳请求
   * @param proto proto格式的心跳请求对象
   */
  public SubClusterHeartbeatRequestPBImpl(
      SubClusterHeartbeatRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  /**
   * 获取当前对象对应的proto对象，合并本地修改后返回
   * @return proto格式的心跳请求
   */
  public SubClusterHeartbeatRequestProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  /**
   * 将本地修改合并到proto对象中
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
   * 如果当前使用proto存储，初始化构建器以便修改
   */
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = SubClusterHeartbeatRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  /**
   * 将本地缓存的域对象合并到构建器中
   */
  private void mergeLocalToBuilder() {
    if (this.subClusterId != null) {
      builder.setSubClusterId(convertToProtoFormat(this.subClusterId));
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
  public SubClusterId getSubClusterId() {
    // 根据当前存储方式获取proto或构建器
    SubClusterHeartbeatRequestProtoOrBuilder p = viaProto ? proto : builder;
    // 优先返回缓存的对象
    if (this.subClusterId != null) {
      return this.subClusterId;
    }
    // 检查是否存在子集群ID字段
    if (!p.hasSubClusterId()) {
      return null;
    }
    // 将proto格式转换为域对象并缓存
    this.subClusterId = convertFromProtoFormat(p.getSubClusterId());
    return this.subClusterId;
  }

  @Override
  public void setSubClusterId(SubClusterId subClusterId) {
    // 确保构建器已初始化
    maybeInitBuilder();
    // 清空字段处理
    if (subClusterId == null) {
      builder.clearSubClusterId();
      return;
    }
    // 缓存域对象并更新构建器
    this.subClusterId = subClusterId;
    builder.setSubClusterId(convertToProtoFormat(subClusterId));
  }

  @Override
  public long getLastHeartBeat() {
    SubClusterHeartbeatRequestProtoOrBuilder p = viaProto ? proto : builder;
    return p.getLastHeartBeat();
  }

  @Override
  public void setLastHeartBeat(long time) {
    maybeInitBuilder();
    builder.setLastHeartBeat(time);
  }

  @Override
  public SubClusterState getState() {
    SubClusterHeartbeatRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasState()) {
      return null;
    }
    return convertFromProtoFormat(p.getState());
  }

  @Override
  public void setState(SubClusterState state) {
    maybeInitBuilder();
    if (state == null) {
      builder.clearState();
      return;
    }
    builder.setState(convertToProtoFormat(state));
  }

  @Override
  public String getCapability() {
    SubClusterHeartbeatRequestProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasCapability()) ? p.getCapability() : null;
  }

  @Override
  public void setCapability(String capability) {
    maybeInitBuilder();
    if (capability == null) {
      builder.clearCapability();
      return;
    }
    builder.setCapability(capability);
  }

  /**
   * 将proto格式的子集群ID转换为域对象
   * @param clusterId proto格式子集群ID
   * @return 域对象子集群ID
   */
  private SubClusterId convertFromProtoFormat(SubClusterIdProto clusterId) {
    return new SubClusterIdPBImpl(clusterId);
  }

  /**
   * 将域对象子集群ID转换为proto格式
   * @param clusterId 域对象子集群ID
   * @return proto格式子集群ID
   */
  private SubClusterIdProto convertToProtoFormat(SubClusterId clusterId) {
    return ((SubClusterIdPBImpl) clusterId).getProto();
  }

  /**
   * 将proto格式的子集群状态转换为域对象
   * @param state proto格式子集群状态
   * @return 域对象子集群状态
   */
  private SubClusterState convertFromProtoFormat(SubClusterStateProto state) {
    return SubClusterState.valueOf(state.name());
  }

  /**
   * 将域对象子集群状态转换为proto格式
   * @param state 域对象子集群状态
   * @return proto格式子集群状态
   */
  private SubClusterStateProto convertToProtoFormat(SubClusterState state) {
    return SubClusterStateProto.valueOf(state.name());
  }

}