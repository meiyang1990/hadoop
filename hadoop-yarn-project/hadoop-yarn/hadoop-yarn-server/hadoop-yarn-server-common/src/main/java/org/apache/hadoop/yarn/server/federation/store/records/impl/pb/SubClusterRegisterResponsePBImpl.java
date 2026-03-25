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
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.SubClusterRegisterResponseProto;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterRegisterResponse;

import org.apache.hadoop.thirdparty.protobuf.TextFormat;

/**
 * 文件级注释：YARN联邦子集群注册响应的Protobuf实现类，基于ProtocolBuffer序列化实现，
 * 用于联邦注册后返回子集群注册结果的存储与传输。
 * Protocol buffer based implementation of {@link SubClusterRegisterResponse}.
 */
@Private
@Unstable
public class SubClusterRegisterResponsePBImpl
    extends SubClusterRegisterResponse {

  // 存储已构建完成的Protobuf对象实例
  private SubClusterRegisterResponseProto proto =
      SubClusterRegisterResponseProto.getDefaultInstance();
  // Protobuf构建器，用于动态构建对象
  private SubClusterRegisterResponseProto.Builder builder = null;
  // 标识当前是否使用已构建完成的proto对象，false表示正在通过builder构建
  private boolean viaProto = false;

  /**
   * 构造空的响应对象，初始化builder。
   */
  public SubClusterRegisterResponsePBImpl() {
    builder = SubClusterRegisterResponseProto.newBuilder();
  }

  /**
   * 基于已有Protobuf对象构造响应对象。
   * @param proto 已有的SubClusterRegisterResponseProto对象
   */
  public SubClusterRegisterResponsePBImpl(
      SubClusterRegisterResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  /**
   * 获取当前对象对应的Protobuf实例，若通过builder构建则先完成构建。
   * @return 构建完成的SubClusterRegisterResponseProto对象
   */
  public SubClusterRegisterResponseProto getProto() {
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