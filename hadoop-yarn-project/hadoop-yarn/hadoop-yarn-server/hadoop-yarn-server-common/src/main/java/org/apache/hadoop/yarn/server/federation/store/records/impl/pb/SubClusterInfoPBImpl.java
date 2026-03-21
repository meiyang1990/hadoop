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
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.SubClusterIdProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.SubClusterInfoProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.SubClusterInfoProtoOrBuilder;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.SubClusterStateProto;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterState;

import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.thirdparty.protobuf.TextFormat;

/**
 * 基于 Protocol Buffer 实现的 {@link SubClusterInfo}，用于序列化存储子集群信息。
 */
@Private
@Unstable
public class SubClusterInfoPBImpl extends SubClusterInfo {

  // 存储子集群信息的Proto对象，当通过Proto读取时使用
  private SubClusterInfoProto proto = SubClusterInfoProto.getDefaultInstance();
  // 用于构建Proto对象的Builder，当修改数据时使用
  private SubClusterInfoProto.Builder builder = null;
  // 当前是否通过Proto对象持有数据的标记
  private boolean viaProto = false;

  // 缓存的子集群ID对象，延迟从Proto反序列化
  private SubClusterId subClusterId = null;

  /**
   * 构造空的子集群信息对象，初始化Builder用于构建新数据。
   */
  public SubClusterInfoPBImpl() {
    builder = SubClusterInfoProto.newBuilder();
  }

  /**
   * 基于已有Proto对象构造子集群信息对象。
   * @param proto 已有的子集群信息Proto对象
   */
  public SubClusterInfoPBImpl(SubClusterInfoProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  /**
   * 获取当前子集群信息对应的Proto对象，合并本地修改后返回。
   * @return 序列化后的Proto对象
   */
  public SubClusterInfoProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  // 将本地缓存的数据合并到Proto对象
  private void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  // 如果当前使用Proto存储，则初始化Builder用于修改
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = SubClusterInfoProto.newBuilder(proto);
    }
    viaProto = false;
  }

  // 将本地缓存的对象合并到Builder中
  private void mergeLocalToBuilder() {
    if (this.subClusterId != null) {
      builder.setSubClusterId(convertToProtoFormat(this.subClusterId));
    }
  }

  @Override
  public String toString() {
    return TextFormat.shortDebugString(getProto());
  }

  @Override
  public SubClusterId getSubClusterId() {
    // 根据当前存储方式选择Proto或Builder
    SubClusterInfoProtoOrBuilder p = viaProto ? proto : builder;
    // 已缓存则直接返回
    if (this.subClusterId != null) {
      return this.subClusterId;
    }
    // Proto中不存在则返回null
    if (!p.hasSubClusterId()) {
      return null;
    }
    // 从Proto反序列化为对象并缓存
    this.subClusterId = convertFromProtoFormat(p.getSubClusterId());
    return this.subClusterId;
  }

  @Override
  public void setSubClusterId(SubClusterId subClusterId) {
    maybeInitBuilder();
    // 清空操作
    if (subClusterId == null) {
      builder.clearSubClusterId();
      return;
    }
    // 缓存并写入Builder
    this.subClusterId = subClusterId;
    builder.setSubClusterId(convertToProtoFormat(subClusterId));
  }

  @Override
  public String getAMRMServiceAddress() {
    SubClusterInfoProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasAMRMServiceAddress()) ? p.getAMRMServiceAddress() : null;
  }

  @Override
  public void setAMRMServiceAddress(String amRMServiceAddress) {
    maybeInitBuilder();
    if (amRMServiceAddress == null) {
      builder.clearAMRMServiceAddress();
      return;
    }
    builder.setAMRMServiceAddress(amRMServiceAddress);
  }

  @Override
  public String getClientRMServiceAddress() {
    SubClusterInfoProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasClientRMServiceAddress()) ? p.getClientRMServiceAddress()
        : null;
  }

  @Override
  public void setClientRMServiceAddress(String clientRMServiceAddress) {
    maybeInitBuilder();
    if (clientRMServiceAddress == null) {
      builder.clearClientRMServiceAddress();
      return;
    }
    builder.setClientRMServiceAddress(clientRMServiceAddress);
  }

  @Override
  public String getRMAdminServiceAddress() {
    SubClusterInfoProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasRMAdminServiceAddress()) ? p.getRMAdminServiceAddress() : null;
  }

  @Override
  public void setRMAdminServiceAddress(String rmAdminServiceAddress) {
    maybeInitBuilder();
    if (rmAdminServiceAddress == null) {
      builder.clearRMAdminServiceAddress();
      return;
    }
    builder.setRMAdminServiceAddress(rmAdminServiceAddress);
  }

  @Override
  public String getRMWebServiceAddress() {
    SubClusterInfoProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasRMWebServiceAddress()) ? p.getRMWebServiceAddress() : null;
  }

  @Override
  public void setRMWebServiceAddress(String rmWebServiceAddress) {
    maybeInitBuilder();
    if (rmWebServiceAddress == null) {
      builder.clearRMWebServiceAddress();
      return;
    }
    builder.setRMWebServiceAddress(rmWebServiceAddress);
  }

  @Override
  public long getLastHeartBeat() {
    SubClusterInfoProtoOrBuilder p = viaProto ? proto : builder;
    return p.getLastHeartBeat();
  }

  @Override
  public void setLastHeartBeat(long time) {
    maybeInitBuilder();
    builder.setLastHeartBeat(time);
  }

  @Override
  public SubClusterState getState() {
    SubClusterInfoProtoOrBuilder p = viaProto ? proto : builder;
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
  public long getLastStartTime() {
    SubClusterInfoProtoOrBuilder p = viaProto ? proto : builder;
    return (p.hasLastStartTime()) ? p.getLastStartTime() : 0;
  }

  @Override
  public void setLastStartTime(long lastStartTime) {
    Preconditions.checkNotNull(builder);
    builder.setLastStartTime(lastStartTime);
  }

  @Override
  public String getCapability() {
    SubClusterInfoProtoOrBuilder p = viaProto ? proto : builder;
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

  // 将Proto格式的子集群ID转换为业务对象
  private SubClusterId convertFromProtoFormat(SubClusterIdProto clusterId) {
    return new SubClusterIdPBImpl(clusterId);
  }

  // 将业务格式的子集群ID转换为Proto格式
  private SubClusterIdProto convertToProtoFormat(SubClusterId clusterId) {
    return ((SubClusterIdPBImpl) clusterId).getProto();
  }

  // 将Proto格式的子集群状态转换为业务枚举
  private SubClusterState convertFromProtoFormat(SubClusterStateProto state) {
    return SubClusterState.valueOf(state.name());
  }

  // 将业务格式的子集群状态转换为Proto枚举
  private SubClusterStateProto convertToProtoFormat(SubClusterState state) {
    return SubClusterStateProto.valueOf(state.name());
  }

}