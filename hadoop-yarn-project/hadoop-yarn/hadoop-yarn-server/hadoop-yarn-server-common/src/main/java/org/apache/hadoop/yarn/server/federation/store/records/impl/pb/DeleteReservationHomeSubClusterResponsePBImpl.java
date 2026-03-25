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
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.DeleteReservationHomeSubClusterResponseProto;
import org.apache.hadoop.yarn.server.federation.store.records.DeleteReservationHomeSubClusterResponse;

import org.apache.hadoop.thirdparty.protobuf.TextFormat;

/**
 * 文件说明：基于Protocol Buffer实现的{@link DeleteReservationHomeSubClusterResponse}，
 * 用于YARN联邦状态存储中删除预约宿主子集群操作响应的PB序列化实现
 */
@Private
@Unstable
public class DeleteReservationHomeSubClusterResponsePBImpl
    extends DeleteReservationHomeSubClusterResponse {
  // 存储Protocol Buffer消息实例，通过默认实例初始化
  private DeleteReservationHomeSubClusterResponseProto proto =
      DeleteReservationHomeSubClusterResponseProto.getDefaultInstance();
  // Protocol Buffer构建器，用于构造消息对象
  private DeleteReservationHomeSubClusterResponseProto.Builder builder = null;
  // 标识当前是否通过proto实例存储数据
  private boolean viaProto = false;

  /**
   * 构造函数，初始化PB构建器
   */
  public DeleteReservationHomeSubClusterResponsePBImpl() {
    builder = DeleteReservationHomeSubClusterResponseProto.newBuilder();
  }

  /**
   * 基于已有PB实例构造响应对象
   * @param proto 已有的PB响应proto实例
   */
  public DeleteReservationHomeSubClusterResponsePBImpl(
      DeleteReservationHomeSubClusterResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  /**
   * 获取当前响应对应的PB实例，自动完成构建转换
   * @return 序列化可用的PB响应实例
   */
  public DeleteReservationHomeSubClusterResponseProto getProto() {
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