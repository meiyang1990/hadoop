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
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.AddReservationHomeSubClusterResponseProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.AddReservationHomeSubClusterResponseProtoOrBuilder;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.SubClusterIdProto;
import org.apache.hadoop.yarn.server.federation.store.records.AddReservationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;

import org.apache.hadoop.thirdparty.protobuf.TextFormat;

/**
 * 文件说明：添加预约归属子集群响应的Protobuf实现，基于Protocol Buffer序列化格式
 * 实现了{@link AddReservationHomeSubClusterResponse}接口，用于YARN联邦元数据存储的RPC通信
 */
@Private
@Unstable
public class AddReservationHomeSubClusterResponsePBImpl
    extends AddReservationHomeSubClusterResponse {

  // Protobuf对象实例，只读模式下使用
  private AddReservationHomeSubClusterResponseProto proto =
      AddReservationHomeSubClusterResponseProto.getDefaultInstance();
  // Protobuf构建器，可写模式下使用
  private AddReservationHomeSubClusterResponseProto.Builder builder = null;
  // 当前是否通过proto实例持有数据
  private boolean viaProto = false;

  /**
   * 无参构造函数，初始化Builder用于构建响应对象
   */
  public AddReservationHomeSubClusterResponsePBImpl() {
    builder = AddReservationHomeSubClusterResponseProto.newBuilder();
  }

  /**
   * 基于已有Protobuf对象构造响应对象
   * @param proto 已序列化的Protobuf响应对象
   */
  public AddReservationHomeSubClusterResponsePBImpl(
      AddReservationHomeSubClusterResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  /**
   * 初始化Builder，确保修改数据前处于可写状态
   */
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = AddReservationHomeSubClusterResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }

  /**
   * 获取当前响应对应的Protobuf对象，序列化前构建最终对象
   * @return 构建完成的Protobuf响应对象
   */
  public AddReservationHomeSubClusterResponseProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  @Override
  public void setHomeSubCluster(SubClusterId homeSubCluster) {
    // 确保Builder已初始化
    maybeInitBuilder();
    // 处理空值，清空已有字段
    if (homeSubCluster == null) {
      builder.clearHomeSubCluster();
      return;
    }
    // 转换Domain对象为Protobuf格式后设置
    builder.setHomeSubCluster(convertToProtoFormat(homeSubCluster));
  }

  @Override
  public SubClusterId getHomeSubCluster() {
    // 根据当前模式选择proto或builder
    AddReservationHomeSubClusterResponseProtoOrBuilder p = viaProto ? proto : builder;

    // 如果字段不存在返回null
    if (!p.hasHomeSubCluster()) {
      return null;
    }
    // 转换Protobuf格式为Domain对象后返回
    return convertFromProtoFormat(p.getHomeSubCluster());
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

  /**
   * 将Protobuf格式的子集群ID转换为领域对象
   * @param sc Protobuf格式子集群ID
   * @return 领域对象子集群ID
   */
  private SubClusterId convertFromProtoFormat(SubClusterIdProto sc) {
    return new SubClusterIdPBImpl(sc);
  }

  /**
   * 将领域对象格式的子集群ID转换为Protobuf格式
   * @param sc 领域对象子集群ID
   * @return Protobuf格式子集群ID
   */
  private SubClusterIdProto convertToProtoFormat(SubClusterId sc) {
    return ((SubClusterIdPBImpl) sc).getProto();
  }
}