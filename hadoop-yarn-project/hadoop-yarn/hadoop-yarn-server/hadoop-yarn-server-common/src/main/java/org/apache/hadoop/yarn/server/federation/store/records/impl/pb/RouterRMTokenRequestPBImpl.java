// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.federation.store.records.impl.pb;

import org.apache.hadoop.thirdparty.protobuf.TextFormat;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.RouterRMTokenRequestProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.RouterRMTokenRequestProtoOrBuilder;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.RouterStoreTokenProto;
import org.apache.hadoop.yarn.server.federation.store.records.RouterRMTokenRequest;
import org.apache.hadoop.yarn.server.federation.store.records.RouterStoreToken;

/**
 * YARN联邦RouterRMTokenRequest的Protobuf序列化实现类，
 * 负责封装向联邦状态存储获取Router RM令牌请求的PB格式转换。
 */
public class RouterRMTokenRequestPBImpl extends RouterRMTokenRequest {

  // 存储Proto对象实例
  private RouterRMTokenRequestProto proto = RouterRMTokenRequestProto.getDefaultInstance();
  // Proto构建器，用于构造修改请求对象
  private RouterRMTokenRequestProto.Builder builder = null;
  // 标记当前是否通过已有的Proto对象初始化
  private boolean viaProto = false;
  // 缓存Router存储令牌对象
  private RouterStoreToken routerStoreToken = null;

  /**
   * 构造函数，初始化空的Proto构建器。
   */
  public RouterRMTokenRequestPBImpl() {
    builder = RouterRMTokenRequestProto.newBuilder();
  }

  /**
   * 基于已有的Proto对象构造请求实例。
   * @param requestProto 已构造完成的RouterRMTokenRequestProto
   */
  public RouterRMTokenRequestPBImpl(RouterRMTokenRequestProto requestProto) {
    this.proto = requestProto;
    viaProto = true;
  }

  /**
   * 获取当前请求对应的Proto对象，合并本地修改后返回。
   * @return 序列化后的Proto对象
   */
  public RouterRMTokenRequestProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  // 将本地修改合并到Proto对象中
  private void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  // 初始化Proto构建器，若当前基于Proto则基于现有Proto创建构建器
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = RouterRMTokenRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  // 将本地缓存的令牌对象合并到Proto构建器中
  private void mergeLocalToBuilder() {
    if (this.routerStoreToken != null) {
      RouterStoreTokenPBImpl routerStoreTokenPBImpl =
          (RouterStoreTokenPBImpl) this.routerStoreToken;
      RouterStoreTokenProto storeTokenProto = routerStoreTokenPBImpl.getProto();
      if (!storeTokenProto.equals(builder.getRouterStoreToken())) {
        builder.setRouterStoreToken(convertToProtoFormat(this.routerStoreToken));
      }
    }
  }

  @Override
  public RouterStoreToken getRouterStoreToken() {
    RouterRMTokenRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (this.routerStoreToken != null) {
      return this.routerStoreToken;
    }
    if (!p.hasRouterStoreToken()) {
      return null;
    }
    this.routerStoreToken = convertFromProtoFormat(p.getRouterStoreToken());
    return this.routerStoreToken;
  }

  @Override
  public void setRouterStoreToken(RouterStoreToken storeToken) {
    maybeInitBuilder();
    if (storeToken == null) {
      builder.clearRouterStoreToken();
      return;
    }
    this.routerStoreToken = storeToken;
    this.builder.setRouterStoreToken(convertToProtoFormat(storeToken));
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

  // 将RouterStoreToken领域对象转换为Proto格式
  private RouterStoreTokenProto convertToProtoFormat(RouterStoreToken storeToken) {
    return ((RouterStoreTokenPBImpl) storeToken).getProto();
  }

  // 将Proto格式转换为RouterStoreToken领域对象
  private RouterStoreToken convertFromProtoFormat(RouterStoreTokenProto storeTokenProto) {
    return new RouterStoreTokenPBImpl(storeTokenProto);
  }
}