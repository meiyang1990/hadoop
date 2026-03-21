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
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.UpdateReservationHomeSubClusterResponseProto;
import org.apache.hadoop.yarn.server.federation.store.records.UpdateReservationHomeSubClusterResponse;

import org.apache.hadoop.thirdparty.protobuf.TextFormat;

/**
 * 文件说明：YARN联邦存储更新预留主集群响应的Protocol Buffer实现类
 * 基于Protocol Buffer序列化实现{@link UpdateReservationHomeSubClusterResponse}接口。
 */
@Private
@Unstable
public class UpdateReservationHomeSubClusterResponsePBImpl
    extends UpdateReservationHomeSubClusterResponse {

  // 存储Protocol Buffer消息实例
  private UpdateReservationHomeSubClusterResponseProto proto =
      UpdateReservationHomeSubClusterResponseProto.getDefaultInstance();
  // Protocol Buffer构建器，用于构建消息
  private UpdateReservationHomeSubClusterResponseProto.Builder builder = null;
  // 标识当前是否直接使用proto实例，而非通过builder构建
  private boolean viaProto = false;

  /**
   * 构造函数，初始化构建器准备创建响应对象。
   */
  public UpdateReservationHomeSubClusterResponsePBImpl() {
    builder = UpdateReservationHomeSubClusterResponseProto.newBuilder();
  }

  /**
   * 构造函数，基于已有proto实例包装响应对象。
   * @param proto 已有的Protocol Buffer响应实例
   */
  public UpdateReservationHomeSubClusterResponsePBImpl(
      UpdateReservationHomeSubClusterResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  /**
   * 获取当前对象对应的Protocol Buffer proto实例。
   * @return 序列化后的proto实例
   */
  public UpdateReservationHomeSubClusterResponseProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
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