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
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.DeleteApplicationHomeSubClusterResponseProto;
import org.apache.hadoop.yarn.server.federation.store.records.DeleteApplicationHomeSubClusterResponse;

import org.apache.hadoop.thirdparty.protobuf.TextFormat;

/**
 * 文件说明：YARN联邦存储删除应用归属子集群响应的Protocol Buffer实现类，基于PB序列化协议实现响应对象
 * 实现了 {@link DeleteApplicationHomeSubClusterResponse} 接口。
 */
@Private
@Unstable
public class DeleteApplicationHomeSubClusterResponsePBImpl
    extends DeleteApplicationHomeSubClusterResponse {

  // PB协议的响应对象实例
  private DeleteApplicationHomeSubClusterResponseProto proto =
      DeleteApplicationHomeSubClusterResponseProto.getDefaultInstance();
  // PB构建器，用于构造响应对象
  private DeleteApplicationHomeSubClusterResponseProto.Builder builder = null;
  // 标识当前是否已经构建为完成的proto对象
  private boolean viaProto = false;

  /**
   * 构造函数，初始化PB构建器用于创建响应对象。
   */
  public DeleteApplicationHomeSubClusterResponsePBImpl() {
    builder = DeleteApplicationHomeSubClusterResponseProto.newBuilder();
  }

  /**
   * 构造函数，基于已有的proto对象包装实现。
   * @param proto 已构建完成的PB响应proto对象
   */
  public DeleteApplicationHomeSubClusterResponsePBImpl(
      DeleteApplicationHomeSubClusterResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  /**
   * 获取当前响应对应的PB proto对象，懒加载构建。
   * @return 构建完成的PB proto对象
   */
  public DeleteApplicationHomeSubClusterResponseProto getProto() {
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