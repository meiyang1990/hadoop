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
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.UpdateApplicationHomeSubClusterResponseProto;
import org.apache.hadoop.yarn.server.federation.store.records.UpdateApplicationHomeSubClusterResponse;

import org.apache.hadoop.thirdparty.protobuf.TextFormat;

/**
 * 文件说明：基于Protocol Buffer实现的{@link UpdateApplicationHomeSubClusterResponse}，
 * 用于YARN联邦环境中更新应用归属子集群操作的响应结果序列化与反序列化
 */
@Private
@Unstable
public class UpdateApplicationHomeSubClusterResponsePBImpl
    extends UpdateApplicationHomeSubClusterResponse {

  // PB协议对象，存储响应数据
  private UpdateApplicationHomeSubClusterResponseProto proto =
      UpdateApplicationHomeSubClusterResponseProto.getDefaultInstance();
  // PB构建器，用于构造响应对象
  private UpdateApplicationHomeSubClusterResponseProto.Builder builder = null;
  // 标记当前是否通过现有proto对象构造
  private boolean viaProto = false;

  /**
   * 空构造函数，初始化PB构建器用于构造新响应对象
   */
  public UpdateApplicationHomeSubClusterResponsePBImpl() {
    builder = UpdateApplicationHomeSubClusterResponseProto.newBuilder();
  }

  /**
   * 基于已有PB proto对象构造响应实现
   * @param proto 已序列化完成的PB proto对象
   */
  public UpdateApplicationHomeSubClusterResponsePBImpl(
      UpdateApplicationHomeSubClusterResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  /**
   * 获取当前响应对应的PB proto对象，处理构建状态转换
   * @return 序列化完成的PB proto对象
   */
  public UpdateApplicationHomeSubClusterResponseProto getProto() {
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