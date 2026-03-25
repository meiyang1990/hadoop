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
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.GetSubClusterPolicyConfigurationRequestProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.GetSubClusterPolicyConfigurationRequestProtoOrBuilder;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterPolicyConfigurationRequest;

import org.apache.hadoop.thirdparty.protobuf.TextFormat;

/**
 * 文件说明: YARN联邦环境下，获取子集群策略配置请求的Protobuf序列化实现类
 * 基于Protocol Buffer实现{@link GetSubClusterPolicyConfigurationRequest}接口。
 */
@Private
@Unstable
public class GetSubClusterPolicyConfigurationRequestPBImpl
    extends GetSubClusterPolicyConfigurationRequest {

  // Protobuf默认实例对象
  private GetSubClusterPolicyConfigurationRequestProto proto =
      GetSubClusterPolicyConfigurationRequestProto.getDefaultInstance();
  // Protobuf构建器，用于修改请求内容
  private GetSubClusterPolicyConfigurationRequestProto.Builder builder = null;
  // 当前是否使用已有proto实例标记
  private boolean viaProto = false;

  /**
   * 无参构造函数，初始化Protobuf构建器。
   */
  public GetSubClusterPolicyConfigurationRequestPBImpl() {
    builder = GetSubClusterPolicyConfigurationRequestProto.newBuilder();
  }

  /**
   * 通过已有Protobuf对象构造请求实例。
   * @param proto 已有的Protobuf请求对象
   */
  public GetSubClusterPolicyConfigurationRequestPBImpl(
      GetSubClusterPolicyConfigurationRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  /**
   * 获取当前请求对应的Protobuf对象，转换完成后切换为只读模式。
   * @return 序列化完成的Protobuf请求对象
   */
  public GetSubClusterPolicyConfigurationRequestProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  /**
   * 初始化构建器，如果当前基于已有proto则复制proto内容到构建器，切换为可写模式。
   */
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = GetSubClusterPolicyConfigurationRequestProto.newBuilder(proto);
    }
    viaProto = false;
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
  public String getQueue() {
    GetSubClusterPolicyConfigurationRequestProtoOrBuilder p =
        viaProto ? proto : builder;
    return p.getQueue();
  }

  @Override
  public void setQueue(String queueName) {
    maybeInitBuilder();
    if (queueName == null) {
      builder.clearQueue();
      return;
    }
    builder.setQueue(queueName);
  }

}