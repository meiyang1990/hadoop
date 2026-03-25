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
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationIdPBImpl;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.DeleteApplicationHomeSubClusterRequestProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.DeleteApplicationHomeSubClusterRequestProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationIdProto;
import org.apache.hadoop.yarn.server.federation.store.records.DeleteApplicationHomeSubClusterRequest;

import org.apache.hadoop.thirdparty.protobuf.TextFormat;

/**
 * 文件级注释：删除应用归属子集群请求的Protobuf实现，用于YARN联邦集群状态存储的RPC序列化
 * Protocol buffer based implementation of
 * {@link DeleteApplicationHomeSubClusterRequest}.
 */
@Private
@Unstable
public class DeleteApplicationHomeSubClusterRequestPBImpl
    extends DeleteApplicationHomeSubClusterRequest {

  // 底层存储的Protobuf对象实例
  private DeleteApplicationHomeSubClusterRequestProto proto =
      DeleteApplicationHomeSubClusterRequestProto.getDefaultInstance();
  // Protobuf构建器，用于修改请求内容
  private DeleteApplicationHomeSubClusterRequestProto.Builder builder = null;
  // 标记当前是否直接使用proto存储数据，false表示使用builder构建
  private boolean viaProto = false;

  /**
   * 构造空的请求对象，初始化Protobuf构建器
   */
  public DeleteApplicationHomeSubClusterRequestPBImpl() {
    builder = DeleteApplicationHomeSubClusterRequestProto.newBuilder();
  }

  /**
   * 基于已有Protobuf对象构造请求实例
   * @param proto 已序列化的Protobuf请求对象
   */
  public DeleteApplicationHomeSubClusterRequestPBImpl(
      DeleteApplicationHomeSubClusterRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  /**
   * 获取当前请求的Protobuf对象，合并本地修改后返回
   * @return 序列化后的Protobuf请求对象
   */
  public DeleteApplicationHomeSubClusterRequestProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  // 将本地修改合并到Protobuf对象
  private void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  // 如果当前是只读模式，初始化构建器用于修改
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = DeleteApplicationHomeSubClusterRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  // 合并本地字段到构建器，当前无额外本地字段，留空
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
  public ApplicationId getApplicationId() {
    // 根据当前存储模式选择proto或builder
    DeleteApplicationHomeSubClusterRequestProtoOrBuilder p =
        viaProto ? proto : builder;
    if (!p.hasApplicationId()) {
      return null;
    }
    // 将Protobuf格式转换为API层ApplicationId对象
    return convertFromProtoFormat(p.getApplicationId());
  }

  @Override
  public void setApplicationId(ApplicationId applicationId) {
    // 确保构建器已初始化
    maybeInitBuilder();
    if (applicationId == null) {
      builder.clearApplicationId();
      return;
    }
    // 将API层对象转换为Protobuf格式并设置
    builder.setApplicationId(convertToProtoFormat(applicationId));
  }

  // Protobuf格式转API层ApplicationId
  private ApplicationId convertFromProtoFormat(ApplicationIdProto appId) {
    return new ApplicationIdPBImpl(appId);
  }

  // API层ApplicationId转Protobuf格式
  private ApplicationIdProto convertToProtoFormat(ApplicationId appId) {
    return ((ApplicationIdPBImpl) appId).getProto();
  }

}