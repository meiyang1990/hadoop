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
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.impl.pb.ReservationIdPBImpl;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.ReservationHomeSubClusterProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.ReservationHomeSubClusterProtoOrBuilder;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.SubClusterIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ReservationIdProto;
import org.apache.hadoop.yarn.server.federation.store.records.ReservationHomeSubCluster;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;

import org.apache.hadoop.thirdparty.protobuf.TextFormat;

/**
 * 文件说明：YARN联邦存储中预约归属子集群信息的Protocol Buffer实现类
 * 基于Protocol Buffer实现{@link ReservationHomeSubCluster}接口，负责序列化存储预约与归属子集群映射关系
 */
@Private
@Unstable
public class ReservationHomeSubClusterPBImpl extends ReservationHomeSubCluster {

  // Protocol Buffer消息实例，当通过proto构建时使用
  private ReservationHomeSubClusterProto proto =
      ReservationHomeSubClusterProto.getDefaultInstance();
  // Protocol Buffer构建器，当通过Java对象构建时使用
  private ReservationHomeSubClusterProto.Builder builder = null;
  // 标识当前数据是否来自proto对象
  private boolean viaProto = false;

  // 缓存预约ID对象
  private ReservationId reservationId = null;
  // 缓存归属子集群ID对象
  private SubClusterId homeSubCluster = null;

  /**
   * 空构造函数，初始化Builder用于构建新对象
   */
  public ReservationHomeSubClusterPBImpl() {
    builder = ReservationHomeSubClusterProto.newBuilder();
  }

  /**
   * 通过已有proto对象构造实现类
   * @param proto 已有的ReservationHomeSubClusterProto对象
   */
  public ReservationHomeSubClusterPBImpl(ReservationHomeSubClusterProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  /**
   * 获取当前对象对应的proto消息，合并本地缓存数据后返回
   * @return 合并后的ReservationHomeSubClusterProto对象
   */
  public ReservationHomeSubClusterProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  // 将本地缓存的Java对象合并到proto中
  private void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  // 按需初始化Builder，如果当前是proto模式则基于现有proto创建Builder
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = ReservationHomeSubClusterProto.newBuilder(proto);
    }
    viaProto = false;
  }

  // 将本地缓存的Java对象写入Builder
  private void mergeLocalToBuilder() {
    if (this.reservationId != null) {
      builder.setReservationId(convertToProtoFormat(this.reservationId));
    }
    if (this.homeSubCluster != null) {
      builder.setHomeSubCluster(convertToProtoFormat(this.homeSubCluster));
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
  public ReservationId getReservationId() {
    ReservationHomeSubClusterProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasReservationId()) {
      return null;
    }
    this.reservationId = convertFromProtoFormat(p.getReservationId());
    return this.reservationId;
  }

  @Override
  public void setReservationId(ReservationId resId) {
    maybeInitBuilder();
    if (resId == null) {
      builder.clearReservationId();
      return;
    }
    builder.setReservationId(convertToProtoFormat(resId));
    this.reservationId = resId;
  }

  @Override
  public SubClusterId getHomeSubCluster() {
    ReservationHomeSubClusterProtoOrBuilder p = viaProto ? proto : builder;
    if (this.homeSubCluster != null) {
      return this.homeSubCluster;
    }
    if (!p.hasHomeSubCluster()) {
      return null;
    }
    this.homeSubCluster = convertFromProtoFormat(p.getHomeSubCluster());
    return this.homeSubCluster;
  }

  @Override
  public void setHomeSubCluster(SubClusterId subClusterId) {
    maybeInitBuilder();
    if (subClusterId == null) {
      builder.clearHomeSubCluster();
      return;
    }
    this.homeSubCluster = subClusterId;
  }

  // 将proto格式的子集群ID转换为Java对象
  private SubClusterId convertFromProtoFormat(SubClusterIdProto subClusterId) {
    return new SubClusterIdPBImpl(subClusterId);
  }

  // 将Java格式的子集群ID转换为proto格式
  private SubClusterIdProto convertToProtoFormat(SubClusterId subClusterId) {
    return ((SubClusterIdPBImpl) subClusterId).getProto();
  }

  // 将proto格式的预约ID转换为Java对象
  private ReservationId convertFromProtoFormat(ReservationIdProto appId) {
    return new ReservationIdPBImpl(appId);
  }

  // 将Java格式的预约ID转换为proto格式
  private ReservationIdProto convertToProtoFormat(ReservationId appId) {
    return ((ReservationIdPBImpl) appId).getProto();
  }
}