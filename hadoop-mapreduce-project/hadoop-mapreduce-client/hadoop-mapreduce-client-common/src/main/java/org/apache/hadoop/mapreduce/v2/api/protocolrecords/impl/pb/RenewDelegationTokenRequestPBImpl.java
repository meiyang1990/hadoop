// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.mapreduce.v2.api.protocolrecords.impl.pb;

import org.apache.hadoop.mapreduce.v2.api.protocolrecords.RenewDelegationTokenRequest;
import org.apache.hadoop.security.proto.SecurityProtos.RenewDelegationTokenRequestProto;
import org.apache.hadoop.security.proto.SecurityProtos.RenewDelegationTokenRequestProtoOrBuilder;
import org.apache.hadoop.security.proto.SecurityProtos.TokenProto;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoBase;
import org.apache.hadoop.yarn.api.records.impl.pb.TokenPBImpl;

/**
 * 续期委托令牌请求的Protobuf实现类，将MapReduce协议请求对象封装为PB格式，用于RPC序列化传输
 * 继承ProtoBase实现PB协议转换规范，实现RenewDelegationTokenRequest接口
 */
public class RenewDelegationTokenRequestPBImpl extends
    ProtoBase<RenewDelegationTokenRequestProto> implements
    RenewDelegationTokenRequest {

  // 存储默认的PB协议对象实例
  RenewDelegationTokenRequestProto proto = RenewDelegationTokenRequestProto
      .getDefaultInstance();
  // PB对象构建器，当需要修改请求内容时使用
  RenewDelegationTokenRequestProto.Builder builder = null;
  // 当前是否通过proto对象存储数据的标记
  boolean viaProto = false;

  /**
   * 空构造函数，初始化PB构建器用于构造新请求
   */
  public RenewDelegationTokenRequestPBImpl() {
    this.builder = RenewDelegationTokenRequestProto.newBuilder();
  }

  /**
   * 通过已有PB对象构造请求包装类，用于反序列化
   * @param proto 已有的续期委托令牌请求PB对象
   */
  public RenewDelegationTokenRequestPBImpl(
      RenewDelegationTokenRequestProto proto) {
    this.proto = proto;
    this.viaProto = true;
  }

  // 缓存委托令牌对象
  Token token;

  @Override
  /**
   * 获取要续期的委托令牌
   * @return 委托令牌对象
   */
  public Token getDelegationToken() {
    // 根据当前存储方式选择proto或builder
    RenewDelegationTokenRequestProtoOrBuilder p = viaProto ? proto : builder;
    // 如果已经缓存了令牌直接返回
    if (this.token != null) {
      return this.token;
    }
    // 从PB对象转换得到令牌并缓存
    this.token = convertFromProtoFormat(p.getToken());
    return this.token;
  }

  @Override
  /**
   * 设置需要续期的委托令牌
   * @param token 待续期的委托令牌对象
   */
  public void setDelegationToken(Token token) {
    // 若当前使用proto存储，初始化builder准备修改
    maybeInitBuilder();
    // 清空令牌字段如果传入为null
    if (token == null)
      builder.clearToken();
    // 缓存设置的令牌对象
    this.token = token;
  }

  @Override
  /**
   * 获取当前请求对应的PB协议对象，用于RPC序列化
   * @return 转换完成的RenewDelegationTokenRequestProto对象
   */
  public RenewDelegationTokenRequestProto getProto() {
    // 将本地缓存的令牌数据合并到PB对象中
    mergeLocalToProto();
    // 构建最终proto对象并标记通过proto存储
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  /**
   * 将本地缓存的令牌数据合并到PB构建器中
   */
  private void mergeLocalToBuilder() {
    if (token != null) {
      builder.setToken(convertToProtoFormat(this.token));
    }
  }

  /**
   * 将本地缓存数据合并到最终PB对象
   */
  private void mergeLocalToProto() {
    // 如果当前用proto存储，初始化builder准备合并
    if (viaProto)
      maybeInitBuilder();
    // 合并本地数据到builder
    mergeLocalToBuilder();
    // 重新构建proto并标记通过proto存储
    proto = builder.build();
    viaProto = true;
  }

  /**
   * 初始化PB构建器，在需要修改现有proto时调用
   */
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = RenewDelegationTokenRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  /**
   * 将Protobuf格式的令牌转换为Yarn API令牌对象
   * @param p Protobuf格式的令牌
   * @return Yarn API令牌对象
   */
  private TokenPBImpl convertFromProtoFormat(TokenProto p) {
    return new TokenPBImpl(p);
  }

  /**
   * 将Yarn API令牌对象转换为Protobuf格式令牌
   * @param t Yarn API令牌对象
   * @return Protobuf格式令牌
   */
  private TokenProto convertToProtoFormat(Token t) {
    return ((TokenPBImpl) t).getProto();
  }
}