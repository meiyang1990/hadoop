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
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.SubClusterInfoProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.SubClusterRegisterRequestProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.SubClusterRegisterRequestProtoOrBuilder;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterRegisterRequest;

import org.apache.hadoop.thirdparty.protobuf.TextFormat;

/**
 * 基于ProtocolBuffer实现的{@link SubClusterRegisterRequest}，YARN联邦子集群注册请求PB实现
 */
@Private
@Unstable
public class SubClusterRegisterRequestPBImpl extends SubClusterRegisterRequest {

  // PB协议对象，已构建完成的只读实例
  private SubClusterRegisterRequestProto proto =
      SubClusterRegisterRequestProto.getDefaultInstance();
  // PB构建器，用于动态构建请求对象
  private SubClusterRegisterRequestProto.Builder builder = null;
  // 标记当前是否通过只读proto实例访问数据
  private boolean viaProto = false;

  // 缓存子集群信息对象，避免重复转换
  private SubClusterInfo subClusterInfo = null;

  /**
   * 构造空的子集群注册请求对象，初始化PB构建器
   */
  public SubClusterRegisterRequestPBImpl() {
    builder = SubClusterRegisterRequestProto.newBuilder();
  }

  /**
   * 通过已有PB proto构造子集群注册请求对象
   * @param proto PB协议的子集群注册请求实例
   */
  public SubClusterRegisterRequestPBImpl(SubClusterRegisterRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  /**
   * 获取当前请求对应的PB协议实例，合并本地缓存数据到proto
   * @return PB协议的子集群注册请求实例
   */
  public SubClusterRegisterRequestProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  /**
   * 将本地缓存的业务对象合并到PB proto实例
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
   * 如果当前是只读proto模式，初始化构建器以便修改
   */
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = SubClusterRegisterRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  /**
   * 将本地缓存的子集群信息合并到PB构建器
   */
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
    // 根据当前模式选择proto或builder
    SubClusterRegisterRequestProtoOrBuilder p = viaProto ? proto : builder;
    // 已有缓存直接返回
    if (this.subClusterInfo != null) {
      return this.subClusterInfo;
    }
    // proto中没有该字段返回null
    if (!p.hasSubClusterInfo()) {
      return null;
    }
    // 从PB转换得到业务对象并缓存
    this.subClusterInfo = convertFromProtoFormat(p.getSubClusterInfo());
    return this.subClusterInfo;
  }

  @Override
  public void setSubClusterInfo(SubClusterInfo subClusterInfo) {
    maybeInitBuilder();
    // 如果传入null清空字段
    if (subClusterInfo == null) {
      builder.clearSubClusterInfo();
      return;
    }
    // 缓存业务对象，转换后设置到PB构建器
    this.subClusterInfo = subClusterInfo;
    builder.setSubClusterInfo(convertToProtoFormat(subClusterInfo));
  }

  /**
   * 将PB格式的子集群信息转换为业务对象
   * @param clusterInfo PB格式子集群信息
   * @return 业务层子集群信息对象
   */
  private SubClusterInfo convertFromProtoFormat(
      SubClusterInfoProto clusterInfo) {
    return new SubClusterInfoPBImpl(clusterInfo);
  }

  /**
   * 将业务层子集群信息转换为PB格式
   * @param clusterInfo 业务层子集群信息对象
   * @return PB格式子集群信息
   */
  private SubClusterInfoProto convertToProtoFormat(SubClusterInfo clusterInfo) {
    return ((SubClusterInfoPBImpl) clusterInfo).getProto();
  }

}