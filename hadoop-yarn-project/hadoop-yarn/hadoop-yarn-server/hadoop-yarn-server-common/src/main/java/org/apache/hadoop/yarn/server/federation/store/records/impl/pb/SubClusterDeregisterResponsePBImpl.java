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
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.SubClusterDeregisterResponseProto;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterDeregisterResponse;

import org.apache.hadoop.thirdparty.protobuf.TextFormat;

/**
 * 文件级注释：YARN联邦子集群注销响应的Protocol Buffer实现类，基于PB序列化存储
 * Protocol buffer based implementation of {@link SubClusterDeregisterResponse}.
 */
@Private
@Unstable
public class SubClusterDeregisterResponsePBImpl
    extends SubClusterDeregisterResponse {

  // 缓存的PB协议对象实例
  private SubClusterDeregisterResponseProto proto =
      SubClusterDeregisterResponseProto.getDefaultInstance();
  // PB对象构建器，构建阶段非空
  private SubClusterDeregisterResponseProto.Builder builder = null;
  // 当前是否通过proto方式存储数据
  private boolean viaProto = false;

  /**
   * 构造函数，初始化空的PB构建器
   */
  public SubClusterDeregisterResponsePBImpl() {
    builder = SubClusterDeregisterResponseProto.newBuilder();
  }

  /**
   * 基于已有PB对象构造响应实例
   * @param proto 已有的子集群注销响应PB对象
   */
  public SubClusterDeregisterResponsePBImpl(
      SubClusterDeregisterResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  /**
   * 获取当前响应对应的PB协议对象
   * @return 序列化后的PB对象
   */
  public SubClusterDeregisterResponseProto getProto() {
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