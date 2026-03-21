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
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.AddReservationHomeSubClusterRequestProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.AddReservationHomeSubClusterRequestProtoOrBuilder;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.ReservationHomeSubClusterProto;
import org.apache.hadoop.yarn.server.federation.store.records.AddReservationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.ReservationHomeSubCluster;

import org.apache.hadoop.thirdparty.protobuf.TextFormat;

/**
 * 添加预约归属子集群请求的Protocol Buffer实现类。
 * 基于Protobuf序列化协议实现{@link AddReservationHomeSubClusterRequest}接口。
 */
@Private
@Unstable
public class AddReservationHomeSubClusterRequestPBImpl
    extends AddReservationHomeSubClusterRequest {

  // 存储Protobuf消息对象
  private AddReservationHomeSubClusterRequestProto proto =
      AddReservationHomeSubClusterRequestProto.getDefaultInstance();
  // Protobuf消息构造器
  private AddReservationHomeSubClusterRequestProto.Builder builder = null;
  // 当前是否通过proto对象访问数据的标记
  private boolean viaProto = false;

  /**
   * 构造空请求对象，初始化Builder。
   */
  public AddReservationHomeSubClusterRequestPBImpl() {
    builder = AddReservationHomeSubClusterRequestProto.newBuilder();
  }

  /**
   * 通过已有的Protobuf对象构造请求。
   * @param proto 已构造完成的Protobuf请求对象
   */
  public AddReservationHomeSubClusterRequestPBImpl(
      AddReservationHomeSubClusterRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  /**
   * 获取当前请求对应的Protobuf对象，合并本地修改。
   * @return 序列化完成的Protobuf请求对象
   */
  public AddReservationHomeSubClusterRequestProto getProto() {
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

  // 延迟初始化Builder，从现有proto对象拷贝数据
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = AddReservationHomeSubClusterRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  // 合并本地修改到Builder，当前无本地字段需要合并
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
    AddReservationHomeSubClusterRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasAppSubclusterMap()) {
      return null;
    }
    return convertFromProtoFormat(p.getAppSubclusterMap());
  }

  @Override
  public void setReservationHomeSubCluster(
      ReservationHomeSubCluster reservationInfo) {
    maybeInitBuilder();
    if (reservationInfo == null) {
      builder.clearAppSubclusterMap();
      return;
    }
    builder.setAppSubclusterMap(convertToProtoFormat(reservationInfo));
  }

  // 将Protobuf格式预约信息转换为业务对象
  private ReservationHomeSubCluster convertFromProtoFormat(
      ReservationHomeSubClusterProto sc) {
    return new ReservationHomeSubClusterPBImpl(sc);
  }

  // 将业务对象预约信息转换为Protobuf格式
  private ReservationHomeSubClusterProto convertToProtoFormat(
      ReservationHomeSubCluster sc) {
    return ((ReservationHomeSubClusterPBImpl) sc).getProto();
  }
}