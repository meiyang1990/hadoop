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
import org.apache.hadoop.yarn.proto.YarnSecurityTokenProtos.YARNDelegationTokenIdentifierProto;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.security.client.YARNDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.federation.store.records.RouterStoreToken;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.RouterStoreTokenProto;
import org.apache.hadoop.yarn.federation.proto.YarnServerFederationProtos.RouterStoreTokenProtoOrBuilder;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;

/**
 * 文件说明：YARN联邦存储中Router存储令牌的Protobuf实现类，基于ProtocolBuffer序列化存储令牌信息
 * 实现{@link RouterStoreToken}接口，封装联邦环境下 delegation token 的存储与序列化
 */
@Private
@Unstable
public class RouterStoreTokenPBImpl extends RouterStoreToken {

  // Protobuf消息对象，缓存序列化后的实例
  private RouterStoreTokenProto proto = RouterStoreTokenProto.getDefaultInstance();

  // Protobuf构建器，用于构造消息
  private RouterStoreTokenProto.Builder builder = null;

  // 标识当前数据是否已通过Protobuf对象存储
  private boolean viaProto = false;

  // 缓存RM委派令牌标识对象
  private YARNDelegationTokenIdentifier rMDelegationTokenIdentifier = null;
  // 令牌更新日期
  private Long renewDate;
  // 令牌额外信息字符串
  private String tokenInfo;

  /**
   * 空构造函数，初始化Protobuf构建器
   */
  public RouterStoreTokenPBImpl() {
    builder = RouterStoreTokenProto.newBuilder();
  }

  /**
   * 基于已有的Protobuf对象构造实例
   * @param storeTokenProto 预构建的RouterStoreTokenProto对象
   */
  public RouterStoreTokenPBImpl(RouterStoreTokenProto storeTokenProto) {
    this.proto = storeTokenProto;
    viaProto = true;
  }

  /**
   * 获取当前对象对应的Protobuf实例，合并本地字段到Proto后返回
   * @return 序列化后的RouterStoreTokenProto对象
   */
  public RouterStoreTokenProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  // 将本地缓存字段合并到Protobuf对象
  private void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  // 将本地缓存的所有字段写入Protobuf构建器
  private void mergeLocalToBuilder() {
    if (this.rMDelegationTokenIdentifier != null) {
      YARNDelegationTokenIdentifierProto idProto = this.rMDelegationTokenIdentifier.getProto();
      if (!idProto.equals(builder.getTokenIdentifier())) {
        builder.setTokenIdentifier(convertToProtoFormat(this.rMDelegationTokenIdentifier));
      }
    }

    if (this.renewDate != null) {
      builder.setRenewDate(this.renewDate);
    }

    if (this.tokenInfo != null) {
      builder.setTokenInfo(this.tokenInfo);
    }
  }

  // 初始化Protobuf构建器，如果当前通过proto存储则从proto构造builder
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = RouterStoreTokenProto.newBuilder(proto);
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

  @Override
  public YARNDelegationTokenIdentifier getTokenIdentifier() throws IOException {
    // 根据存储模式选择proto或builder
    RouterStoreTokenProtoOrBuilder p = viaProto ? proto : builder;
    // 已缓存直接返回缓存对象
    if (rMDelegationTokenIdentifier != null) {
      return rMDelegationTokenIdentifier;
    }
    // proto中没有该字段返回空
    if(!p.hasTokenIdentifier()){
      return null;
    }
    // 从proto反序列化出令牌标识对象
    YARNDelegationTokenIdentifierProto identifierProto = p.getTokenIdentifier();
    ByteArrayInputStream in = new ByteArrayInputStream(identifierProto.toByteArray());
    RMDelegationTokenIdentifier identifier = new RMDelegationTokenIdentifier();
    identifier.readFields(new DataInputStream(in));
    // 缓存到本地
    this.rMDelegationTokenIdentifier = identifier;
    return identifier;
  }

  @Override
  public Long getRenewDate() {
    RouterStoreTokenProtoOrBuilder p = viaProto ? proto : builder;
    if (this.renewDate != null) {
      return this.renewDate;
    }
    if (!p.hasRenewDate()) {
      return null;
    }
    this.renewDate = p.getRenewDate();
    return this.renewDate;
  }

  @Override
  public void setIdentifier(YARNDelegationTokenIdentifier identifier) {
    maybeInitBuilder();
    if(identifier == null) {
      builder.clearTokenIdentifier();
      return;
    }
    this.rMDelegationTokenIdentifier = identifier;
    this.builder.setTokenIdentifier(identifier.getProto());
  }

  @Override
  public void setRenewDate(Long renewDate) {
    maybeInitBuilder();
    if(renewDate == null) {
      builder.clearRenewDate();
      return;
    }
    this.renewDate = renewDate;
    this.builder.setRenewDate(renewDate);
  }

  @Override
  public String getTokenInfo() {
    RouterStoreTokenProtoOrBuilder p = viaProto ? proto : builder;
    if (this.tokenInfo != null) {
      return this.tokenInfo;
    }
    if (!p.hasTokenInfo()) {
      return null;
    }
    this.tokenInfo = p.getTokenInfo();
    return this.tokenInfo;
  }

  @Override
  public void setTokenInfo(String tokenInfo) {
    maybeInitBuilder();
    if (tokenInfo == null) {
      builder.clearTokenInfo();
      return;
    }
    this.tokenInfo = tokenInfo;
    this.builder.setTokenInfo(tokenInfo);
  }

  // 将YARN令牌标识转换为Protobuf格式
  private YARNDelegationTokenIdentifierProto convertToProtoFormat(
      YARNDelegationTokenIdentifier delegationTokenIdentifier) {
    return delegationTokenIdentifier.getProto();
  }

  /**
   * 将当前对象序列化为字节数组
   * @return 序列化后的字节数组
   * @throws IOException 序列化异常
   */
  public byte[] toByteArray() throws IOException {
    return builder.build().toByteArray();
  }

  /**
   * 从输入流反序列化读取对象
   * @param in 输入流
   * @throws IOException 反序列化异常
   */
  public void readFields(DataInput in) throws IOException {
    builder.mergeFrom((DataInputStream) in);
  }
}