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

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.thirdparty.protobuf.TextFormat;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.RouterRMTokenResponseProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.RouterRMTokenResponseProtoOrBuilder;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.RouterStoreTokenProto;
import org.apache.hadoop.yarn.server.federation.store.records.RouterRMTokenResponse;
import org.apache.hadoop.yarn.server.federation.store.records.RouterStoreToken;

/**
 * 联邦场景下获取Router RM令牌响应的Protobuf实现类，
 * 基于Protobuf序列化框架实现响应数据的存储与转换。
 */
@Private
@Unstable
public class RouterRMTokenResponsePBImpl extends RouterRMTokenResponse {

  // Protobuf消息对象，存储只读实例
  private RouterRMTokenResponseProto proto = RouterRMTokenResponseProto.getDefaultInstance();
  // Protobuf消息构造器，用于构建可变实例
  private RouterRMTokenResponseProto.Builder builder = null;
  // 标记当前是否通过只读proto实例持有数据
  private boolean viaProto = false;
  // 缓存的Router存储令牌对象
  private RouterStoreToken routerStoreToken = null;

  /**
   * 无参构造函数，初始化Protobuf构建器。
   */
  public RouterRMTokenResponsePBImpl() {
    builder = RouterRMTokenResponseProto.newBuilder();
  }

  /**
   * 基于已有Protobuf实例构造响应对象。
   * @param requestProto 已构建完成的Protobuf响应实例
   */
  public RouterRMTokenResponsePBImpl(RouterRMTokenResponseProto requestProto) {
    this.proto = requestProto;
    viaProto = true;
  }

  /**
   * 获取当前对象对应的Protobuf实例，合并本地修改并生成最终proto。
   * @return 序列化完成的Protobuf响应对象
   */
  public RouterRMTokenResponseProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  // 将本地缓存对象合并到proto实例中
  private void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  // 初始化Protobuf构建器，若当前持有只读proto则基于它创建构建器
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = RouterRMTokenResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }

  // 将本地缓存的令牌对象合并到Protobuf构建器中
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
    RouterRMTokenResponseProtoOrBuilder p = viaProto ? proto : builder;
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
    }
    this.routerStoreToken = storeToken;
  }

  // 将业务层令牌对象转换为Protobuf格式
  private RouterStoreTokenProto convertToProtoFormat(RouterStoreToken storeToken) {
    return ((RouterStoreTokenPBImpl) storeToken).getProto();
  }

  // 将Protobuf格式转换为业务层令牌对象
  private RouterStoreToken convertFromProtoFormat(RouterStoreTokenProto storeTokenProto) {
    return new RouterStoreTokenPBImpl(storeTokenProto);
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