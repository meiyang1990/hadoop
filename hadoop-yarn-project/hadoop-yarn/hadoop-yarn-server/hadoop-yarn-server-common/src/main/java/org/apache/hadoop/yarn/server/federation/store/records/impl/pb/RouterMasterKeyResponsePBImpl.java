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
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.RouterMasterKeyProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.RouterMasterKeyResponseProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.RouterMasterKeyResponseProtoOrBuilder;
import org.apache.hadoop.yarn.server.federation.store.records.RouterMasterKey;
import org.apache.hadoop.yarn.server.federation.store.records.RouterMasterKeyResponse;

/**
 * YARN联邦状态存储获取路由器密钥响应的Protobuf实现类。
 * 基于Protobuf序列化框架，实现响应对象的构造与转换，用于联邦元数据存储数据交互。
 */
@Private
@Unstable
public class RouterMasterKeyResponsePBImpl extends RouterMasterKeyResponse {

  // Protobuf消息对象，表示当前响应数据
  private RouterMasterKeyResponseProto proto = RouterMasterKeyResponseProto.getDefaultInstance();
  // Protobuf消息构建器，用于构造修改响应数据
  private RouterMasterKeyResponseProto.Builder builder = null;
  // 标识当前是否通过现成的Proto对象使用
  private boolean viaProto = false;
  // 缓存本地组装的路由器密钥对象
  private RouterMasterKey routerMasterKey = null;

  /**
   * 构造函数，初始化一个空的响应构建器。
   */
  public RouterMasterKeyResponsePBImpl() {
    builder = RouterMasterKeyResponseProto.newBuilder();
  }

  /**
   * 基于已有的Protobuf对象构造响应实例。
   * @param proto 已构造好的响应Protobuf对象
   */
  public RouterMasterKeyResponsePBImpl(RouterMasterKeyResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  /**
   * 获取当前响应的Protobuf对象，合并本地修改后返回。
   * @return 序列化后的Protobuf响应对象
   */
  public RouterMasterKeyResponseProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  // 将本地缓存的对象合并到Protobuf对象中
  private void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  // 延迟初始化Protobuf构建器，从现有proto复制数据
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = RouterMasterKeyResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }

  // 将本地缓存的路由器密钥对象合并到Protobuf构建器中
  private void mergeLocalToBuilder() {
    RouterMasterKeyPBImpl masterKeyRequest = (RouterMasterKeyPBImpl) this.routerMasterKey;
    RouterMasterKeyProto routerMasterKeyProto = builder.getRouterMasterKey();
    if (this.routerMasterKey != null && !masterKeyRequest.getProto().equals(routerMasterKeyProto)) {
      builder.setRouterMasterKey(convertToProtoFormat(this.routerMasterKey));
    }
  }

  @Override
  public RouterMasterKey getRouterMasterKey() {
    RouterMasterKeyResponseProtoOrBuilder p = viaProto ? proto : builder;
    // 直接返回本地缓存
    if (this.routerMasterKey != null) {
      return this.routerMasterKey;
    }
    // proto中不存在则返回null
    if (!p.hasRouterMasterKey()) {
      return null;
    }
    // 从proto转换并缓存
    this.routerMasterKey = convertFromProtoFormat(p.getRouterMasterKey());
    return this.routerMasterKey;
  }

  @Override
  public void setRouterMasterKey(RouterMasterKey masterKey) {
    maybeInitBuilder();
    // 清空字段
    if (masterKey == null) {
      builder.clearRouterMasterKey();
      return;
    }
    // 缓存到本地并更新到proto构建器
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

  // 将Protobuf格式的密钥对象转换为API层对象
  private RouterMasterKey convertFromProtoFormat(RouterMasterKeyProto masterKeyProto) {
    return new RouterMasterKeyPBImpl(masterKeyProto);
  }

  // 将API层密钥对象转换为Protobuf格式
  private RouterMasterKeyProto convertToProtoFormat(RouterMasterKey masterKey) {
    return ((RouterMasterKeyPBImpl) masterKey).getProto();
  }
}