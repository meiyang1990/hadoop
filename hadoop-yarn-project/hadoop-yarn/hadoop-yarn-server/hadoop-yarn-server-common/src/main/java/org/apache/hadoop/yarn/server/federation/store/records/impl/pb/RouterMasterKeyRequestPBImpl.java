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
import org.apache.hadoop.yarn.server.federation.store.records.RouterMasterKey;
import org.apache.hadoop.yarn.server.federation.store.records.RouterMasterKeyRequest;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.RouterMasterKeyProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.RouterMasterKeyRequestProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.RouterMasterKeyRequestProtoOrBuilder;
import org.apache.hadoop.thirdparty.protobuf.TextFormat;

/**
 * YARN联邦Router主密钥请求的Protobuf实现类，基于PB序列化协议实现数据存储与转换。
 * 负责在联邦状态存储中传递获取/更新Router主密钥的请求信息。
 */
@Private
@Unstable
public class RouterMasterKeyRequestPBImpl extends RouterMasterKeyRequest {

  // Protobuf消息实例，当通过已有proto构建时使用
  private RouterMasterKeyRequestProto proto = RouterMasterKeyRequestProto.getDefaultInstance();
  // Protobuf消息构建器，当构建新消息时使用
  private RouterMasterKeyRequestProto.Builder builder = null;
  // 当前是否直接使用proto实例标识，false表示正在通过builder构建
  private boolean viaProto = false;
  // 缓存的Router主密钥对象
  private RouterMasterKey routerMasterKey = null;

  /**
   * 无参构造函数，初始化Protobuf构建器。
   */
  public RouterMasterKeyRequestPBImpl() {
    builder = RouterMasterKeyRequestProto.newBuilder();
  }

  /**
   * 通过已有Protobuf消息构造请求对象。
   * @param proto 已有的RouterMasterKeyRequestProto实例
   */
  public RouterMasterKeyRequestPBImpl(RouterMasterKeyRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  /**
   * 获取当前请求对应的Protobuf消息，合并本地缓存后生成最终proto。
   * @return 序列化后的Protobuf消息实例
   */
  public RouterMasterKeyRequestProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  // 将本地缓存数据合并到Protobuf消息
  private void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  // 初始化Protobuf构建器，如果当前使用proto实例则基于它创建builder
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = RouterMasterKeyRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  // 将本地缓存的主密钥对象合并到Protobuf构建器
  private void mergeLocalToBuilder() {
    RouterMasterKeyPBImpl masterKeyRequest = (RouterMasterKeyPBImpl) this.routerMasterKey;
    RouterMasterKeyProto routerMasterKeyProto = builder.getRouterMasterKey();
    if (this.routerMasterKey != null && !masterKeyRequest.getProto().equals(routerMasterKeyProto)) {
      builder.setRouterMasterKey(convertToProtoFormat(this.routerMasterKey));
    }
  }

  @Override
  public RouterMasterKey getRouterMasterKey() {
    RouterMasterKeyRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (this.routerMasterKey != null) {
      return this.routerMasterKey;
    }
    if (!p.hasRouterMasterKey()) {
      return null;
    }
    this.routerMasterKey = convertFromProtoFormat(p.getRouterMasterKey());
    return this.routerMasterKey;
  }

  @Override
  public void setRouterMasterKey(RouterMasterKey masterKey) {
    maybeInitBuilder();
    if (masterKey == null) {
      builder.clearRouterMasterKey();
      return;
    }
    this.routerMasterKey = masterKey;
    builder.setRouterMasterKey(convertToProtoFormat(masterKey));
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

  /**
   * 将Protobuf格式的主密钥转换为业务对象。
   * @param masterKeyProto Protobuf格式主密钥
   * @return 业务层RouterMasterKey对象
   */
  private RouterMasterKey convertFromProtoFormat(RouterMasterKeyProto masterKeyProto) {
    return new RouterMasterKeyPBImpl(masterKeyProto);
  }

  /**
   * 将业务层主密钥对象转换为Protobuf格式。
   * @param masterKey 业务层RouterMasterKey对象
   * @return Protobuf格式主密钥
   */
  private RouterMasterKeyProto convertToProtoFormat(RouterMasterKey masterKey) {
    return ((RouterMasterKeyPBImpl) masterKey).getProto();
  }
}