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
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.GetReservationHomeSubClusterRequestProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.GetReservationHomeSubClusterRequestProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnProtos.ReservationIdProto;
import org.apache.hadoop.yarn.server.federation.store.records.GetReservationHomeSubClusterRequest;

import org.apache.hadoop.thirdparty.protobuf.TextFormat;

/**
 * 文件级说明：YARN联邦存储模块，基于Protocol Buffer实现的获取预约归属子集群请求
 * Protocol buffer based implementation of
 * {@link GetReservationHomeSubClusterRequest}.
 */
@Private
@Unstable
public class GetReservationHomeSubClusterRequestPBImpl
    extends GetReservationHomeSubClusterRequest {

  // 缓存的PB协议对象实例
  private GetReservationHomeSubClusterRequestProto proto =
      GetReservationHomeSubClusterRequestProto.getDefaultInstance();
  // PB协议构建器，用于构造修改请求对象
  private GetReservationHomeSubClusterRequestProto.Builder builder = null;
  // 当前数据是否已经从构建器合并到proto实例的标志位
  private boolean viaProto = false;

  // 缓存的预约ID对象
  private ReservationId reservationId = null;

  /**
   * 构造方法，初始化PB构建器用于创建新请求
   */
  public GetReservationHomeSubClusterRequestPBImpl() {
    builder = GetReservationHomeSubClusterRequestProto.newBuilder();
  }

  /**
   * 基于已有PB proto构造请求对象
   * @param proto 已有的PB协议对象
   */
  public GetReservationHomeSubClusterRequestPBImpl(
      GetReservationHomeSubClusterRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  /**
   * 获取当前请求对应的完整PB协议对象，自动合并本地修改
   * @return 构造完成的PB协议对象
   */
  public GetReservationHomeSubClusterRequestProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  // 将本地缓存的字段合并到PB proto对象
  private void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  // 延迟初始化PB构建器，如果当前使用proto则基于现有proto创建构建器
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = GetReservationHomeSubClusterRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  // 将本地缓存的预约ID合并到PB构建器中
  private void mergeLocalToBuilder() {
    if (this.reservationId != null) {
      builder.setReservationId(convertToProtoFormat(this.reservationId));
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
    // 根据当前状态选择使用proto还是builder
    GetReservationHomeSubClusterRequestProtoOrBuilder p = viaProto ? proto : builder;
    // 本地已缓存直接返回
    if (reservationId != null) {
      return reservationId;
    }

    // proto中不存在该字段返回空
    if (!p.hasReservationId()) {
      return null;
    }
    // 从PB反序列化为API对象并缓存到本地
    this.reservationId = convertFromProtoFormat(p.getReservationId());
    return reservationId;
  }

  @Override
  public void setReservationId(ReservationId paramReservationId) {
    maybeInitBuilder();
    // 输入为空则清空字段
    if (paramReservationId == null) {
      builder.clearReservationId();
      return;
    }
    // 缓存到本地并写入PB构建器
    this.reservationId = paramReservationId;
    builder.setReservationId(convertToProtoFormat(paramReservationId));
  }

  // 将PB格式的ReservationId转换为API对象
  private ReservationId convertFromProtoFormat(ReservationIdProto appId) {
    return new ReservationIdPBImpl(appId);
  }

  // 将API格式的ReservationId转换为PB协议对象
  private ReservationIdProto convertToProtoFormat(ReservationId appId) {
    return ((ReservationIdPBImpl) appId).getProto();
  }
}