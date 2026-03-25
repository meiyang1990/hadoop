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
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.GetSubClustersInfoRequestProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.GetSubClustersInfoRequestProtoOrBuilder;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClustersInfoRequest;

import org.apache.hadoop.thirdparty.protobuf.TextFormat;

/**
 * 文件级：YARN联邦存储层获取子集群信息请求的Protobuf实现类，基于ProtocolBuffer序列化实现
 * Protocol buffer based implementation of {@link GetSubClustersInfoRequest}.
 */
@Private
@Unstable
public class GetSubClustersInfoRequestPBImpl extends GetSubClustersInfoRequest {

  // Protobuf消息对象，当通过已有proto构建时使用
  private GetSubClustersInfoRequestProto proto =
      GetSubClustersInfoRequestProto.getDefaultInstance();
  // Protobuf构建器，当构建新消息或修改消息时使用
  private GetSubClustersInfoRequestProto.Builder builder = null;
  // 标识当前是否直接使用proto对象，false表示当前正在通过builder构建
  private boolean viaProto = false;

  /**
   * 构造空的获取子集群信息请求对象，初始化builder
   */
  public GetSubClustersInfoRequestPBImpl() {
    builder = GetSubClustersInfoRequestProto.newBuilder();
  }

  /**
   * 基于已有Protobuf对象构造获取子集群信息请求
   * @param proto 已序列化的Protobuf请求对象
   */
  public GetSubClustersInfoRequestPBImpl(GetSubClustersInfoRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  /**
   * 获取当前请求对应的Protobuf对象，合并本地修改后返回
   * @return 序列化完成的Protobuf请求对象
   */
  public GetSubClustersInfoRequestProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  // 将本地修改合并到proto对象
  private void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  // 如果当前基于proto，初始化builder用于修改
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = GetSubClustersInfoRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  // 合并本地自定义字段到builder，当前无自定义字段，留空
  private void mergeLocalToBuilder() {
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
  public boolean getFilterInactiveSubClusters() {
    // 根据当前状态选择使用proto还是builder读取属性
    GetSubClustersInfoRequestProtoOrBuilder p = viaProto ? proto : builder;
    return p.getFilterInactiveSubclusters();
  }

  @Override
  public void setFilterInactiveSubClusters(boolean filterInactiveSubClusters) {
    // 确保builder已初始化，设置过滤非激活子集群选项
    maybeInitBuilder();
    builder.setFilterInactiveSubclusters(filterInactiveSubClusters);
  }

}