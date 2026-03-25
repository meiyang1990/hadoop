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
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.SubClusterHeartbeatResponseProto;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterHeartbeatResponse;

import org.apache.hadoop.thirdparty.protobuf.TextFormat;

/**
 * 文件功能：基于Protocol Buffer实现的子集群心跳响应记录类
 * 子集群心跳响应是YARN联邦架构中，子集群向联邦状态存储上报心跳后，存储返回给子集群的响应结果
 * Protocol buffer based implementation of {@link SubClusterHeartbeatResponse}.
 */
@Private
@Unstable
public class SubClusterHeartbeatResponsePBImpl
    extends SubClusterHeartbeatResponse {

  // 存储Proto缓冲实例，默认初始化为默认实例
  private SubClusterHeartbeatResponseProto proto =
      SubClusterHeartbeatResponseProto.getDefaultInstance();
  // Proto构建器，通过builder模式构造新的Proto实例
  private SubClusterHeartbeatResponseProto.Builder builder = null;
  // 标识当前是否通过已有的Proto对象实例化
  private boolean viaProto = false;

  /**
   * 无参构造函数，初始化Builder用于构造新对象
   */
  public SubClusterHeartbeatResponsePBImpl() {
    builder = SubClusterHeartbeatResponseProto.newBuilder();
  }

  /**
   * 基于已有Proto对象构造响应实例
   * @param proto 已有的Proto心跳响应对象
   */
  public SubClusterHeartbeatResponsePBImpl(
      SubClusterHeartbeatResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  /**
   * 获取当前对象对应的Proto实例，完成对象到Proto的转换
   * @return 序列化后的Proto心跳响应对象
   */
  public SubClusterHeartbeatResponseProto getProto() {
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