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
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.GetReservationHomeSubClusterResponseProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.GetReservationHomeSubClusterResponseProtoOrBuilder;
import org.apache.hadoop.yarn.server.federation.store.records.ReservationHomeSubCluster;
import org.apache.hadoop.yarn.server.federation.store.records.GetReservationHomeSubClusterResponse;

import org.apache.hadoop.thirdparty.protobuf.TextFormat;

/**
 * 文件级注释：基于ProtocolBuffer实现的获取预约归属子集群响应类，为联邦存储系统提供PB序列化能力
 * Protocol buffer based implementation of
 * {@link GetReservationHomeSubClusterResponse}.
 */
@Private
@Unstable
public class GetReservationHomeSubClusterResponsePBImpl
    extends GetReservationHomeSubClusterResponse {

  // PB协议对象实例，只读模式下使用默认实例
  private GetReservationHomeSubClusterResponseProto proto =
      GetReservationHomeSubClusterResponseProto.getDefaultInstance();
  // PB构建器，修改模式下使用
  private GetReservationHomeSubClusterResponseProto.Builder builder = null;
  // 标识当前是否通过已有Proto构造对象
  private boolean viaProto = false;

  // 无参构造，初始化构建器
  public GetReservationHomeSubClusterResponsePBImpl() {
    builder = GetReservationHomeSubClusterResponseProto.newBuilder();
  }

  // 基于已有Proto构造响应对象
  public GetReservationHomeSubClusterResponsePBImpl(
      GetReservationHomeSubClusterResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  // 获取当前对象的Proto表示，合并本地修改后返回
  public GetReservationHomeSubClusterResponseProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  // 将本地修改合并到Proto中
  private void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  // 延迟初始化Builder，如果当前是Proto只读模式则基于已有Proto创建Builder
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = GetReservationHomeSubClusterResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }

  // 合并本地变量到Builder，当前无本地额外字段所以留空
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
  // 获取预约归属子集群信息
  public ReservationHomeSubCluster getReservationHomeSubCluster() {
    GetReservationHomeSubClusterResponseProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasAppSubclusterMap()) {
      return null;
    }
    // 将PB格式转换为业务对象格式
    return convertFromProtoFormat(p.getAppSubclusterMap());
  }

  @Override
  // 设置预约归属子集群信息
  public void setReservationHomeSubCluster(
      ReservationHomeSubCluster reservationInfo) {
    maybeInitBuilder();
    if (reservationInfo == null) {
      builder.clearAppSubclusterMap();
      return;
    }
    // 将业务对象转换为PB格式存入Builder
    builder.setAppSubclusterMap(convertToProtoFormat(reservationInfo));
  }

  // PB格式转换为业务对象
  private ReservationHomeSubCluster convertFromProtoFormat(
      ReservationHomeSubClusterProto sc) {
    return new ReservationHomeSubClusterPBImpl(sc);
  }

  // 业务对象转换为PB格式
  private ReservationHomeSubClusterProto convertToProtoFormat(
      ReservationHomeSubCluster sc) {
    return ((ReservationHomeSubClusterPBImpl) sc).getProto();
  }
}