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
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.SetSubClusterPolicyConfigurationResponseProto;
import org.apache.hadoop.yarn.server.federation.store.records.SetSubClusterPolicyConfigurationResponse;

import org.apache.hadoop.thirdparty.protobuf.TextFormat;

/**
 * 文件说明：基于ProtocolBuffer实现的设置子集群策略配置响应实体，属于YARN联邦存储层的PB实现类
 * Protocol buffer based implementation of
 * {@link SetSubClusterPolicyConfigurationResponse}.
 */
@Private
@Unstable
public class SetSubClusterPolicyConfigurationResponsePBImpl
    extends SetSubClusterPolicyConfigurationResponse {

  // PB协议对象实例，默认使用默认实例
  private SetSubClusterPolicyConfigurationResponseProto proto =
      SetSubClusterPolicyConfigurationResponseProto.getDefaultInstance();
  // PB构建器，用于构造修改对象
  private SetSubClusterPolicyConfigurationResponseProto.Builder builder = null;
  // 标识当前是否通过proto对象访问数据
  private boolean viaProto = false;

  /**
   * 无参构造函数，初始化PB构建器
   */
  public SetSubClusterPolicyConfigurationResponsePBImpl() {
    builder = SetSubClusterPolicyConfigurationResponseProto.newBuilder();
  }

  /**
   * 基于已有proto对象的构造函数
   * @param proto 已构造完成的PB协议对象
   */
  public SetSubClusterPolicyConfigurationResponsePBImpl(
      SetSubClusterPolicyConfigurationResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  /**
   * 获取当前对象对应的PB协议对象，合并本地修改后返回
   * @return 序列化用的PB协议对象
   */
  public SetSubClusterPolicyConfigurationResponseProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  // 将本地构建器修改合并到proto对象
  private void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    proto = builder.build();
    viaProto = true;
  }

  // 延迟初始化构建器，如果当前是proto模式则基于现有proto创建构建器
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = SetSubClusterPolicyConfigurationResponseProto.newBuilder(proto);
    }
    viaProto = false;
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