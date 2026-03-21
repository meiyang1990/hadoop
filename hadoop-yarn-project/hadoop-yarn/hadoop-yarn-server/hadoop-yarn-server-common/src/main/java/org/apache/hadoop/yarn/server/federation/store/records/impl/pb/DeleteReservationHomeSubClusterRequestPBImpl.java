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
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.DeleteReservationHomeSubClusterRequestProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.DeleteReservationHomeSubClusterRequestProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnProtos.ReservationIdProto;
import org.apache.hadoop.yarn.server.federation.store.records.DeleteReservationHomeSubClusterRequest;

import org.apache.hadoop.thirdparty.protobuf.TextFormat;

/**
 * 文件说明：删除预约归属子集群请求的Protocol Buffer实现类
 * 基于Protocol Buffer实现{@link DeleteReservationHomeSubClusterRequest}接口
 */
@Private
@Unstable
public class DeleteReservationHomeSubClusterRequestPBImpl
    extends DeleteReservationHomeSubClusterRequest {

  // Protocol Buffer proto对象，存储序列化后的数据
  private DeleteReservationHomeSubClusterRequestProto proto =
      DeleteReservationHomeSubClusterRequestProto.getDefaultInstance();
  // Protocol Buffer构建器，用于构造对象
  private DeleteReservationHomeSubClusterRequestProto.Builder builder = null;
  // 标记当前是否通过proto方式存储数据，false表示使用builder构建
  private boolean viaProto = false;

  /**
   * 构造方法，初始化builder用于构建请求对象
   */
  public DeleteReservationHomeSubClusterRequestPBImpl() {
    builder = DeleteReservationHomeSubClusterRequestProto.newBuilder();
  }

  /**
   * 构造方法，基于已有proto对象封装请求
   * @param proto 已序列化的proto对象
   */
  public DeleteReservationHomeSubClusterRequestPBImpl(
      DeleteReservationHomeSubClusterRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  /**
   * 获取当前请求的proto对象，处理本地数据合并
   * @return 序列化后的proto对象
   */
  public DeleteReservationHomeSubClusterRequestProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  /**
   * 将本地builder数据合并到proto对象
   */
  private void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  /**
   * 如果当前是proto存储模式，初始化builder以便修改
   */
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = DeleteReservationHomeSubClusterRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  /**
   * 将本地字段合并到builder，本类无额外本地字段，因此为空实现
   */
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
  public ReservationId getReservationId() {
    // 根据存储模式获取proto或builder
    DeleteReservationHomeSubClusterRequestProtoOrBuilder p =
        viaProto ? proto : builder;
    // 如果没有预约ID，返回空
    if (!p.hasReservationId()) {
      return null;
    }
    // 将proto格式转换为ReservationId对象返回
    return convertFromProtoFormat(p.getReservationId());
  }

  @Override
  public void setReservationId(ReservationId reservationId) {
    // 初始化builder以便修改
    maybeInitBuilder();
    // 如果传入为空，清空字段
    if (reservationId == null) {
      builder.clearReservationId();
      return;
    }
    // 将ReservationId转换为proto格式并设置
    builder.setReservationId(convertToProtoFormat(reservationId));
  }

  /**
   * 将ReservationIdProto转换为ReservationId业务对象
   * @param appId proto格式预约ID
   * @return 业务层ReservationId对象
   */
  private ReservationId convertFromProtoFormat(ReservationIdProto appId) {
    return new ReservationIdPBImpl(appId);
  }

  /**
   * 将ReservationId业务对象转换为proto格式
   * @param appId 业务层ReservationId对象
   * @return proto格式预约ID
   */
  private ReservationIdProto convertToProtoFormat(ReservationId appId) {
    return ((ReservationIdPBImpl) appId).getProto();
  }
}