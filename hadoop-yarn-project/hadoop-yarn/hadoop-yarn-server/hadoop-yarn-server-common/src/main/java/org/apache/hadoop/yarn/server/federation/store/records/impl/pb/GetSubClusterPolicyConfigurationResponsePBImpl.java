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
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.GetSubClusterPolicyConfigurationResponseProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.GetSubClusterPolicyConfigurationResponseProtoOrBuilder;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.SubClusterPolicyConfigurationProto;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterPolicyConfigurationResponse;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterPolicyConfiguration;

import org.apache.hadoop.thirdparty.protobuf.TextFormat;

/**
 * 文件说明：YARN联邦环境下，获取子集群路由策略配置响应的Protobuf实现类
 * 基于Protocol Buffer序列化实现{@link GetSubClusterPolicyConfigurationResponse}接口。
 * 用于在联邦状态存储服务中传递获取到的子集群路由策略配置信息。
 */
@Private
@Unstable
public class GetSubClusterPolicyConfigurationResponsePBImpl
    extends GetSubClusterPolicyConfigurationResponse {

  // Protobuf对象实例，通过proto方式存储时使用
  private GetSubClusterPolicyConfigurationResponseProto proto =
      GetSubClusterPolicyConfigurationResponseProto.getDefaultInstance();
  // Protobuf构建器，通过builder方式修改时使用
  private GetSubClusterPolicyConfigurationResponseProto.Builder builder = null;
  // 标识当前数据是否通过proto实例存储
  private boolean viaProto = false;

  // 缓存的子集群策略配置对象
  private SubClusterPolicyConfiguration subClusterPolicy = null;

  /**
   * 构造函数，初始化空的Protobuf构建器。
   */
  public GetSubClusterPolicyConfigurationResponsePBImpl() {
    builder = GetSubClusterPolicyConfigurationResponseProto.newBuilder();
  }

  /**
   * 构造函数，基于已有的Protobuf对象封装。
   * @param proto 已构造完成的Protobuf响应对象
   */
  public GetSubClusterPolicyConfigurationResponsePBImpl(
      GetSubClusterPolicyConfigurationResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  /**
   * 获取当前响应对象的Protobuf表示，合并本地修改后返回。
   * @return 标准Protobuf响应对象
   */
  public GetSubClusterPolicyConfigurationResponseProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  /**
   * 将本地缓存的对象合并到Protobuf实例中。
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
   * 如果当前是proto存储模式，初始化构建器以便修改。
   */
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = GetSubClusterPolicyConfigurationResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }

  /**
   * 将本地缓存的策略配置合并到Protobuf构建器中。
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
    // 根据存储模式选择proto或builder
    GetSubClusterPolicyConfigurationResponseProtoOrBuilder p =
        viaProto ? proto : builder;
    // 直接返回缓存对象
    if (this.subClusterPolicy != null) {
      return this.subClusterPolicy;
    }
    // Protobuf中不存在该配置，返回null
    if (!p.hasPolicyConfiguration()) {
      return null;
    }
    // 从Protobuf转换为业务对象并缓存
    this.subClusterPolicy = convertFromProtoFormat(p.getPolicyConfiguration());
    return this.subClusterPolicy;
  }

  @Override
  public void setPolicyConfiguration(
      SubClusterPolicyConfiguration policyConfiguration) {
    maybeInitBuilder();
    // 清空配置
    if (policyConfiguration == null) {
      builder.clearPolicyConfiguration();
      return;
    }
    // 缓存业务对象，写入Protobuf构建器
    this.subClusterPolicy = policyConfiguration;
    this.builder.setPolicyConfiguration(convertToProtoFormat(policyConfiguration));
  }

  /**
   * 将Protobuf格式的策略配置转换为业务对象。
   * @param policy Protobuf格式策略配置
   * @return PB实现的业务策略配置对象
   */
  private SubClusterPolicyConfiguration convertFromProtoFormat(
      SubClusterPolicyConfigurationProto policy) {
    return new SubClusterPolicyConfigurationPBImpl(policy);
  }

  /**
   * 将业务策略配置对象转换为Protobuf格式。
   * @param policy 业务策略配置对象
   * @return Protobuf格式策略配置
   */
  private SubClusterPolicyConfigurationProto convertToProtoFormat(
      SubClusterPolicyConfiguration policy) {
    return ((SubClusterPolicyConfigurationPBImpl) policy).getProto();
  }

}