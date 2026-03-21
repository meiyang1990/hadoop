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
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.DeletePoliciesConfigurationsRequestProto;
import org.apache.hadoop.yarn.server.federation.store.records.DeletePoliciesConfigurationsRequest;

/**
 * 删除路由策略配置请求的Protobuf序列化实现类，
 * 用于YARN联邦元数据存储层的请求数据序列化。
 */
@Private
@Unstable
public class DeletePoliciesConfigurationsRequestPBImpl
    extends DeletePoliciesConfigurationsRequest {

  // Protobuf消息对象，构建完成后不可变
  private DeletePoliciesConfigurationsRequestProto proto =
      DeletePoliciesConfigurationsRequestProto.getDefaultInstance();

  // Protobuf构建器，用于构造消息对象
  private DeletePoliciesConfigurationsRequestProto.Builder builder = null;

  // 当前是否通过只读proto对象提供数据
  private boolean viaProto = false;

  /**
   * 构造空的删除请求对象，初始化Protobuf构建器。
   */
  public DeletePoliciesConfigurationsRequestPBImpl() {
    builder = DeletePoliciesConfigurationsRequestProto.newBuilder();
  }

  /**
   * 基于已有的Protobuf对象构造删除请求对象。
   * @param proto 已构造完成的Protobuf请求对象
   */
  public DeletePoliciesConfigurationsRequestPBImpl(
      DeletePoliciesConfigurationsRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  /**
   * 获取当前请求对应的不可变Protobuf对象，自动完成构建。
   * @return 不可变Protobuf请求对象
   */
  public DeletePoliciesConfigurationsRequestProto getProto() {
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