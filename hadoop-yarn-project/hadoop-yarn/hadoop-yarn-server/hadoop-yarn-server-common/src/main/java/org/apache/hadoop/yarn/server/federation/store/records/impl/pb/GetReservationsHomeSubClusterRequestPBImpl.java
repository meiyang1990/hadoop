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
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.GetReservationsHomeSubClusterRequestProto;
import org.apache.hadoop.yarn.server.federation.store.records.GetReservationsHomeSubClusterRequest;

import org.apache.hadoop.thirdparty.protobuf.TextFormat;

/**
 * 文件级注释：基于Protocol Buffer实现的获取预留资源归属子集群请求实现类，
 * 属于YARN联邦存储层的PB序列化实现，用于联邦状态存储服务的RPC通信。
 * <p>
 * Protocol buffer based implementation of
 * {@link GetReservationsHomeSubClusterRequest}.
 */
@Private
@Unstable
public class GetReservationsHomeSubClusterRequestPBImpl
    extends GetReservationsHomeSubClusterRequest {

  // 存储已构建完成的Protocol Buffer消息实例
  private GetReservationsHomeSubClusterRequestProto proto =
      GetReservationsHomeSubClusterRequestProto.getDefaultInstance();
  // Protocol Buffer构建器，用于构建消息实例
  private GetReservationsHomeSubClusterRequestProto.Builder builder = null;
  // 标识当前是否通过已有proto实例访问
  private boolean viaProto = false;

  /**
   * 无参构造函数，初始化PB构建器用于构造新请求。
   */
  public GetReservationsHomeSubClusterRequestPBImpl() {
    builder = GetReservationsHomeSubClusterRequestProto.newBuilder();
  }

  /**
   * 基于已有PB proto实例构造请求对象。
   * @param proto 已有的PB请求proto实例
   */
  public GetReservationsHomeSubClusterRequestPBImpl(
      GetReservationsHomeSubClusterRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  /**
   * 获取当前请求对应的proto实例，若通过构建器则先构建再返回。
   * @return 构造完成的proto实例
   */
  public GetReservationsHomeSubClusterRequestProto getProto() {
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