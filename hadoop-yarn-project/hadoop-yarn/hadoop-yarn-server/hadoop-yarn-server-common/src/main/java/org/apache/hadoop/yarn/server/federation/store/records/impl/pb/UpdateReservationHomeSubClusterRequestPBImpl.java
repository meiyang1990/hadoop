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
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.ReservationHomeSubClusterProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.UpdateReservationHomeSubClusterRequestProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.UpdateReservationHomeSubClusterRequestProtoOrBuilder;
import org.apache.hadoop.yarn.server.federation.store.records.ReservationHomeSubCluster;
import org.apache.hadoop.yarn.server.federation.store.records.UpdateReservationHomeSubClusterRequest;

import org.apache.hadoop.thirdparty.protobuf.TextFormat;

/**
 * 文件说明：YARN联邦环境下更新预约归属子集群请求的Protocol Buffer实现类
 * Protocol buffer based implementation of
 * {@link UpdateReservationHomeSubClusterRequest} .
 */
@Private
@Unstable
public class UpdateReservationHomeSubClusterRequestPBImpl
    extends UpdateReservationHomeSubClusterRequest {

  // PB协议对象实例
  private UpdateReservationHomeSubClusterRequestProto proto =
      UpdateReservationHomeSubClusterRequestProto.getDefaultInstance();
  // PB构建器对象
  private UpdateReservationHomeSubClusterRequestProto.Builder builder = null;
  // 标识当前是否通过proto对象存储数据
  private boolean viaProto = false;

  /**
   * 构造函数，初始化PB构建器
   */
  public UpdateReservationHomeSubClusterRequestPBImpl() {
    builder = UpdateReservationHomeSubClusterRequestProto.newBuilder();
  }

  /**
   * 构造函数，基于已有的PB对象构造请求实例
   * @param proto 已构造好的PB请求对象
   */
  public UpdateReservationHomeSubClusterRequestPBImpl(
      UpdateReservationHomeSubClusterRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  /**
   * 获取当前请求的PB对象，会自动合并本地修改
   * @return 完整的PB请求对象
   */
  public UpdateReservationHomeSubClusterRequestProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  // 合并本地修改到proto对象
  private void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  // 延迟初始化PB构建器，如果当前是proto模式则基于现有proto创建构建器
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = UpdateReservationHomeSubClusterRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  // 合并本地字段到构建器，当前无本地缓存字段，此方法留空
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
  public ReservationHomeSubCluster getReservationHomeSubCluster() {
    // 根据当前存储模式选择proto或builder
    UpdateReservationHomeSubClusterRequestProtoOrBuilder p =
        viaProto ? proto : builder;
    if (!p.hasAppSubclusterMap()) {
      return null;
    }
    // 将PB格式转换为业务对象
    return convertFromProtoFormat(p.getAppSubclusterMap());
  }

  @Override
  public void setReservationHomeSubCluster(
      ReservationHomeSubCluster reservationInfo) {
    maybeInitBuilder();
    // 如果传入为空，清除原有字段
    if (reservationInfo == null) {
      builder.clearAppSubclusterMap();
      return;
    }
    // 将业务对象转换为PB格式并设置
    builder.setAppSubclusterMap(convertToProtoFormat(reservationInfo));
  }

  // 将PB对象转换为业务对象实现
  private ReservationHomeSubCluster convertFromProtoFormat(
      ReservationHomeSubClusterProto sc) {
    return new ReservationHomeSubClusterPBImpl(sc);
  }

  // 将业务对象转换为PB对象
  private ReservationHomeSubClusterProto convertToProtoFormat(
      ReservationHomeSubCluster sc) {
    return ((ReservationHomeSubClusterPBImpl) sc).getProto();
  }
}