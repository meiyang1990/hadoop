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

import java.nio.ByteBuffer;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoUtils;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.SubClusterPolicyConfigurationProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.SubClusterPolicyConfigurationProtoOrBuilder;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterPolicyConfiguration;

import org.apache.hadoop.thirdparty.protobuf.TextFormat;

/**
 * 文件说明：子集群路由策略配置基于Protobuf的实现类，存储YARN联邦环境中队列级别的路由策略配置
 * Protobuf based implementation of {@link SubClusterPolicyConfiguration}.
 *
 */
@Private
@Unstable
public class SubClusterPolicyConfigurationPBImpl
    extends SubClusterPolicyConfiguration {

  // Protobuf消息对象，存储序列化后的配置数据
  private SubClusterPolicyConfigurationProto proto =
      SubClusterPolicyConfigurationProto.getDefaultInstance();
  // Protobuf构建器，用于构建修改配置
  private SubClusterPolicyConfigurationProto.Builder builder = null;
  // 标记当前是否直接使用proto对象，false表示正在通过builder修改
  private boolean viaProto = false;

  /**
   * 构造函数，初始化空的构建器
   */
  public SubClusterPolicyConfigurationPBImpl() {
    builder = SubClusterPolicyConfigurationProto.newBuilder();
  }

  /**
   * 基于已有Protobuf对象构造配置实例
   * @param proto 已有的Protobuf配置对象
   */
  public SubClusterPolicyConfigurationPBImpl(
      SubClusterPolicyConfigurationProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  /**
   * 获取当前配置对应的Protobuf对象，构建并缓存最终结果
   * @return 序列化后的Protobuf配置对象
   */
  public SubClusterPolicyConfigurationProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  /**
   * 初始化构建器，确保可以修改配置，从现有proto拷贝数据到builder
   */
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = SubClusterPolicyConfigurationProto.newBuilder(proto);
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
    SubClusterPolicyConfigurationProtoOrBuilder p = viaProto ? proto : builder;
    return p.getQueue();
  }

  @Override
  public void setQueue(String queueName) {
    maybeInitBuilder();
    if (queueName == null) {
      builder.clearType();
      return;
    }
    builder.setQueue(queueName);

  }

  @Override
  public String getType() {
    SubClusterPolicyConfigurationProtoOrBuilder p = viaProto ? proto : builder;
    return p.getType();
  }

  @Override
  public void setType(String policyType) {
    maybeInitBuilder();
    if (policyType == null) {
      builder.clearType();
      return;
    }
    builder.setType(policyType);
  }

  @Override
  public ByteBuffer getParams() {
    SubClusterPolicyConfigurationProtoOrBuilder p = viaProto ? proto : builder;
    return ProtoUtils.convertFromProtoFormat(p.getParams());
  }

  @Override
  public void setParams(ByteBuffer policyParams) {
    maybeInitBuilder();
    if (policyParams == null) {
      builder.clearParams();
      return;
    }
    builder.setParams(ProtoUtils.convertToProtoFormat(policyParams));
  }

}