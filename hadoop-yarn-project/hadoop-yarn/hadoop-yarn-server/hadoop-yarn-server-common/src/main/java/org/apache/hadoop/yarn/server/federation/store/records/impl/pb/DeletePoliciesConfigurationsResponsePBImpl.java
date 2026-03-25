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
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.DeletePoliciesConfigurationsResponseProto;
import org.apache.hadoop.yarn.server.federation.store.records.DeletePoliciesConfigurationsResponse;

/**
 * YARN联邦存储删除路由策略配置响应的Protobuf实现类，基于Protobuf序列化协议实现数据存储。
 */
@Private
@Unstable
public class DeletePoliciesConfigurationsResponsePBImpl
    extends DeletePoliciesConfigurationsResponse {

  // Protobuf消息对象，当通过构建器模式构建完成后保存最终消息
  private DeletePoliciesConfigurationsResponseProto proto =
      DeletePoliciesConfigurationsResponseProto.getDefaultInstance();

  // Protobuf消息构建器，当需要修改消息内容时使用构建器模式
  private DeletePoliciesConfigurationsResponseProto.Builder builder = null;

  // 标记当前是否已经通过构建器生成了最终Proto消息
  private boolean viaProto = false;

  /**
   * 无参构造函数，初始化Protobuf构建器。
   */
  public DeletePoliciesConfigurationsResponsePBImpl() {
    builder = DeletePoliciesConfigurationsResponseProto.newBuilder();
  }

  /**
   * 基于已有的Protobuf消息构造响应对象。
   * @param proto 已构造完成的Protobuf删除策略配置响应消息
   */
  public DeletePoliciesConfigurationsResponsePBImpl(
      DeletePoliciesConfigurationsResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  /**
   * 获取当前响应对应的Protobuf消息实例，若存在未构建的修改则完成构建。
   * @return 最终序列化好的Protobuf消息
   */
  public DeletePoliciesConfigurationsResponseProto getProto() {
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