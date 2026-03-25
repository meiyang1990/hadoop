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
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.GetSubClusterPoliciesConfigurationsRequestProto;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterPoliciesConfigurationsRequest;

import org.apache.hadoop.thirdparty.protobuf.TextFormat;

/**
 * 获取子集群策略配置请求的Protocol Buffer实现，基于Protobuf序列化框架实现。
 * 对应{@link GetSubClusterPoliciesConfigurationsRequest}接口。
 */
@Private
@Unstable
public class GetSubClusterPoliciesConfigurationsRequestPBImpl
    extends GetSubClusterPoliciesConfigurationsRequest {

  // 存储ProtoBuf实例，viaProto为true时直接使用该实例
  private GetSubClusterPoliciesConfigurationsRequestProto proto =
      GetSubClusterPoliciesConfigurationsRequestProto.getDefaultInstance();
  // ProtoBuf构建器，修改请求内容时使用
  private GetSubClusterPoliciesConfigurationsRequestProto.Builder builder =
      null;
  // 标记当前是否直接使用proto实例，false表示正在通过builder修改
  private boolean viaProto = false;

  /**
   * 构造空的请求对象，初始化Builder用于构建请求。
   */
  public GetSubClusterPoliciesConfigurationsRequestPBImpl() {
    builder = GetSubClusterPoliciesConfigurationsRequestProto.newBuilder();
  }

  /**
   * 基于已有Proto实例构造请求对象。
   * @param proto 已构造完成的Proto请求实例
   */
  public GetSubClusterPoliciesConfigurationsRequestPBImpl(
      GetSubClusterPoliciesConfigurationsRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  /**
   * 获取当前请求对应的Proto实例，合并本地修改后返回。
   * @return 序列化完成的Proto请求实例
   */
  public GetSubClusterPoliciesConfigurationsRequestProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  // 将本地builder的修改合并到proto实例中
  private void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    proto = builder.build();
    viaProto = true;
  }

  // 按需初始化builder，若当前基于现有proto则从proto创建builder
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder =
          GetSubClusterPoliciesConfigurationsRequestProto.newBuilder(proto);
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
}