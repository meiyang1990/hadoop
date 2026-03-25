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
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.SetSubClusterPolicyConfigurationRequestProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.SetSubClusterPolicyConfigurationRequestProtoOrBuilder;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.SubClusterPolicyConfigurationProto;
import org.apache.hadoop.yarn.server.federation.store.records.SetSubClusterPolicyConfigurationRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterPolicyConfiguration;

import org.apache.hadoop.thirdparty.protobuf.TextFormat;

/**
 * 基于Protocol Buffer实现的{@link SetSubClusterPolicyConfigurationRequest}，
 * 用于在YARN联邦集群中设置子集群路由策略配置的请求序列化与反序列化。
 */
@Private
@Unstable
public class SetSubClusterPolicyConfigurationRequestPBImpl
    extends SetSubClusterPolicyConfigurationRequest {

  // Proto缓冲实例，当通过Proto构造时使用
  private SetSubClusterPolicyConfigurationRequestProto proto =
      SetSubClusterPolicyConfigurationRequestProto.getDefaultInstance();
  // Proto构建器，当需要修改对象时使用
  private SetSubClusterPolicyConfigurationRequestProto.Builder builder = null;
  // 当前对象是否直接使用proto实例（未修改）标记
  private boolean viaProto = false;

  // 缓存子集群策略配置对象
  private SubClusterPolicyConfiguration subClusterPolicy = null;

  /**
   * 构造函数，初始化空构建器。
   */
  public SetSubClusterPolicyConfigurationRequestPBImpl() {
    builder = SetSubClusterPolicyConfigurationRequestProto.newBuilder();
  }

  /**
   * 基于已有Proto实例构造请求对象。
   * @param proto 已有Proto请求实例
   */
  public SetSubClusterPolicyConfigurationRequestPBImpl(
      SetSubClusterPolicyConfigurationRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  /**
   * 获取当前请求的Proto实例，合并本地修改后返回。
   * @return 请求的Proto实例
   */
  public SetSubClusterPolicyConfigurationRequestProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  /**
   * 将本地缓存的对象合并到Proto实例中。
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
   * 如果当前使用的是只读Proto实例，初始化可写Builder。
   */
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = SetSubClusterPolicyConfigurationRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  /**
   * 将本地缓存的策略配置合并到Builder中。
   */
  private void mergeLocalToBuilder() {
    if (this.subClusterPolicy != null
        && !((SubClusterPolicyConfigurationPBImpl) this.subClusterPolicy)
            .getProto().equals(builder.getPolicyConfiguration())) {
      builder
          .setPolicyConfiguration(convertToProtoFormat(this.subClusterPolicy));
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
  public SubClusterPolicyConfiguration getPolicyConfiguration() {
    SetSubClusterPolicyConfigurationRequestProtoOrBuilder p =
        viaProto ? proto : builder;
    // 返回缓存的策略配置，如果已经加载
    if (this.subClusterPolicy != null) {
      return this.subClusterPolicy;
    }
    // Proto中不存在策略配置，返回null
    if (!p.hasPolicyConfiguration()) {
      return null;
    }
    // 从Proto反序列化为策略配置对象并缓存
    this.subClusterPolicy = convertFromProtoFormat(p.getPolicyConfiguration());
    return this.subClusterPolicy;
  }

  @Override
  public void setPolicyConfiguration(
      SubClusterPolicyConfiguration policyConfiguration) {
    maybeInitBuilder();
    // 清空策略配置如果入参为null
    if (policyConfiguration == null) {
      builder.clearPolicyConfiguration();
      return;
    }
    // 缓存策略配置，更新到Builder中
    this.subClusterPolicy = policyConfiguration;
    builder.setPolicyConfiguration(convertToProtoFormat(policyConfiguration));
  }

  /**
   * 将Proto格式的策略配置转换为API对象。
   * @param policy Proto格式策略配置
   * @return API层策略配置对象
   */
  private SubClusterPolicyConfiguration convertFromProtoFormat(
      SubClusterPolicyConfigurationProto policy) {
    return new SubClusterPolicyConfigurationPBImpl(policy);
  }

  /**
   * 将API层策略配置转换为Proto格式。
   * @param policy API层策略配置对象
   * @return Proto格式策略配置
   */
  private SubClusterPolicyConfigurationProto convertToProtoFormat(
      SubClusterPolicyConfiguration policy) {
    return ((SubClusterPolicyConfigurationPBImpl) policy).getProto();
  }
}