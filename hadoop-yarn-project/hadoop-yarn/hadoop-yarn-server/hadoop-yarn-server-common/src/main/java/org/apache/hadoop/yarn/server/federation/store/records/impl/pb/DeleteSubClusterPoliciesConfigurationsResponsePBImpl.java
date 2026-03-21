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
import org.apache.hadoop.thirdparty.protobuf.TextFormat;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.DeleteSubClusterPoliciesConfigurationsResponseProto;
import org.apache.hadoop.yarn.server.federation.store.records.DeleteSubClusterPoliciesConfigurationsResponse;

/**
 * 基于ProtocolBuffer实现的删除子集群策略配置响应类，用于YARN联邦元数据存储响应的序列化
 * 实现 {@link DeleteSubClusterPoliciesConfigurationsResponse} 接口
 */
@Private
@Unstable
public class DeleteSubClusterPoliciesConfigurationsResponsePBImpl
    extends DeleteSubClusterPoliciesConfigurationsResponse {

  // 存储已构建完成的ProtocolBuffer原型对象
  private DeleteSubClusterPoliciesConfigurationsResponseProto proto =
      DeleteSubClusterPoliciesConfigurationsResponseProto.getDefaultInstance();
  // ProtocolBuffer构建器，用于构建新对象
  private DeleteSubClusterPoliciesConfigurationsResponseProto.Builder builder = null;
  // 标记当前是否通过已有的原型对象使用该实例
  private boolean viaProto = false;

  /**
   * 构造函数，初始化构建器用于创建新响应对象
   */
  public DeleteSubClusterPoliciesConfigurationsResponsePBImpl() {
    builder = DeleteSubClusterPoliciesConfigurationsResponseProto.newBuilder();
  }

  /**
   * 基于已有原型对象构造响应实例
   * @param proto 已有的ProtocolBuffer原型对象
   */
  public DeleteSubClusterPoliciesConfigurationsResponsePBImpl(
      DeleteSubClusterPoliciesConfigurationsResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  /**
   * 获取当前响应对应的ProtocolBuffer原型对象
   * @return 构建完成的ProtocolBuffer原型对象
   */
  public DeleteSubClusterPoliciesConfigurationsResponseProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
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
}