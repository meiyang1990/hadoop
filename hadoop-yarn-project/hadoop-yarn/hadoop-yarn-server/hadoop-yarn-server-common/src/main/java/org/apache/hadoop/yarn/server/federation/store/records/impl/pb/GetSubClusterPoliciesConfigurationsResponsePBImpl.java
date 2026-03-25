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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.GetSubClusterPoliciesConfigurationsResponseProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.GetSubClusterPoliciesConfigurationsResponseProtoOrBuilder;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.SubClusterPolicyConfigurationProto;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterPoliciesConfigurationsResponse;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterPolicyConfiguration;

import org.apache.hadoop.thirdparty.protobuf.TextFormat;

/**
 * 文件级注释：获取子集群策略配置响应的Protocol Buffer实现类，用于YARN联邦模式下，
 * 序列化/反序列化从联邦状态存储查询子集群路由策略配置的响应数据。
 * Protocol buffer based implementation of
 * {@link GetSubClusterPoliciesConfigurationsResponse}.
 */
@Private
@Unstable
public class GetSubClusterPoliciesConfigurationsResponsePBImpl
    extends GetSubClusterPoliciesConfigurationsResponse {

  // PB协议只读实例，通过viaProto标识当前数据存储位置
  private GetSubClusterPoliciesConfigurationsResponseProto proto =
      GetSubClusterPoliciesConfigurationsResponseProto.getDefaultInstance();
  // PB协议构建器，用于修改数据
  private GetSubClusterPoliciesConfigurationsResponseProto.Builder builder =
      null;
  // 标识数据是否存储在proto实例中，false表示数据在builder或本地缓存中
  private boolean viaProto = false;

  // 本地缓存的子集群策略配置列表，避免重复PB转换
  private List<SubClusterPolicyConfiguration> subClusterPolicies = null;

  /**
   * 构造函数，初始化空的PB构建器。
   */
  public GetSubClusterPoliciesConfigurationsResponsePBImpl() {
    builder = GetSubClusterPoliciesConfigurationsResponseProto.newBuilder();
  }

  /**
   * 基于已有PB对象构造响应实例，数据存储在proto中。
   * @param proto 已序列化的PB响应对象
   */
  public GetSubClusterPoliciesConfigurationsResponsePBImpl(
      GetSubClusterPoliciesConfigurationsResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  /**
   * 获取当前响应对应的PB协议对象，合并本地修改后生成最终对象。
   * @return 序列化后的PB响应对象
   */
  public GetSubClusterPoliciesConfigurationsResponseProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  // 将本地缓存数据合并到proto对象
  private void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  // 延迟初始化PB构建器，基于现有proto创建
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder =
          GetSubClusterPoliciesConfigurationsResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }

  // 将本地缓存的策略列表合并到PB构建器中
  private void mergeLocalToBuilder() {
    if (this.subClusterPolicies != null) {
      addSubClusterPoliciesConfigurationsToProto();
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
  public List<SubClusterPolicyConfiguration> getPoliciesConfigs() {
    initSubClusterPoliciesConfigurationsList();
    return this.subClusterPolicies;
  }

  @Override
  public void setPoliciesConfigs(
      List<SubClusterPolicyConfiguration> policyConfigurations) {
    maybeInitBuilder();
    if (policyConfigurations == null) {
      builder.clearPoliciesConfigurations();
    }
    this.subClusterPolicies = policyConfigurations;
    addSubClusterPoliciesConfigurationsToProto();
  }

  // 从PB对象初始化本地缓存的策略配置列表
  private void initSubClusterPoliciesConfigurationsList() {
    if (this.subClusterPolicies != null) {
      return;
    }
    GetSubClusterPoliciesConfigurationsResponseProtoOrBuilder p =
        viaProto ? proto : builder;
    List<SubClusterPolicyConfigurationProto> subClusterPoliciesList =
        p.getPoliciesConfigurationsList();
    subClusterPolicies = new ArrayList<SubClusterPolicyConfiguration>();

    // 将每个PB格式的策略配置转换为业务对象添加到本地列表
    for (SubClusterPolicyConfigurationProto r : subClusterPoliciesList) {
      subClusterPolicies.add(convertFromProtoFormat(r));
    }
  }

  // 将本地缓存的策略配置列表转换为PB格式写入构建器
  private void addSubClusterPoliciesConfigurationsToProto() {
    maybeInitBuilder();
    builder.clearPoliciesConfigurations();
    if (subClusterPolicies == null) {
      return;
    }
    // 自定义可迭代对象，实现从业务对象到PB对象的流式转换
    Iterable<SubClusterPolicyConfigurationProto> iterable =
        new Iterable<SubClusterPolicyConfigurationProto>() {
          @Override
          public Iterator<SubClusterPolicyConfigurationProto> iterator() {
            return new Iterator<SubClusterPolicyConfigurationProto>() {

              private Iterator<SubClusterPolicyConfiguration> iter =
                  subClusterPolicies.iterator();

              @Override
              public boolean hasNext() {
                return iter.hasNext();
              }

              @Override
              public SubClusterPolicyConfigurationProto next() {
                return convertToProtoFormat(iter.next());
              }

              @Override
              public void remove() {
                throw new UnsupportedOperationException();
              }

            };

          }

        };
    builder.addAllPoliciesConfigurations(iterable);
  }

  // 将PB格式策略配置转换为业务对象
  private SubClusterPolicyConfiguration convertFromProtoFormat(
      SubClusterPolicyConfigurationProto policy) {
    return new SubClusterPolicyConfigurationPBImpl(policy);
  }

  // 将业务对象策略配置转换为PB格式
  private SubClusterPolicyConfigurationProto convertToProtoFormat(
      SubClusterPolicyConfiguration policy) {
    return ((SubClusterPolicyConfigurationPBImpl) policy).getProto();
  }

}