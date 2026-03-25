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

import org.apache.hadoop.mapreduce.v2.api.protocolrecords.CancelDelegationTokenRequest;
import org.apache.hadoop.security.proto.SecurityProtos.CancelDelegationTokenRequestProto;
import org.apache.hadoop.security.proto.SecurityProtos.CancelDelegationTokenRequestProtoOrBuilder;
import org.apache.hadoop.security.proto.SecurityProtos.TokenProto;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoBase;
import org.apache.hadoop.yarn.api.records.impl.pb.TokenPBImpl;

/**
 * 取消委托令牌请求的Protobuf实现类，负责将MapReduce取消委托令牌请求与Protobuf消息格式互相转换，
 * 用于MapReduce客户端与服务端之间的RPC通信序列化。
 */
public class CancelDelegationTokenRequestPBImpl extends
    ProtoBase<CancelDelegationTokenRequestProto> implements
    CancelDelegationTokenRequest {

  // 缓存的Protobuf默认实例
  CancelDelegationTokenRequestProto proto = 
      CancelDelegationTokenRequestProto.getDefaultInstance();
  // Protobuf构建器，用于构造请求消息
  CancelDelegationTokenRequestProto.Builder builder = null;
  // 标识当前是否通过已有Protobuf实例构建本对象
  boolean viaProto = false;
  
  public CancelDelegationTokenRequestPBImpl() {
    this.builder = CancelDelegationTokenRequestProto.newBuilder();
  }

  public CancelDelegationTokenRequestPBImpl (
      CancelDelegationTokenRequestProto proto) {
    this.proto = proto;
    this.viaProto = true;
  }
    
  // 缓存的待取消委托令牌对象
  Token token;

  @Override
  /**
   * 获取待取消的委托令牌
   * @return 待取消的委托令牌
   */
  public Token getDelegationToken() {
    CancelDelegationTokenRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (this.token != null) {
      return this.token;
    }
    // 从Protobuf消息转换出令牌对象
    this.token = convertFromProtoFormat(p.getToken());
    return this.token;
  }

  @Override
  /**
   * 设置待取消的委托令牌
   * @param token 待取消的委托令牌
   */
  public void setDelegationToken(Token token) {
    maybeInitBuilder();
    if (token == null) 
      builder.clearToken();
    this.token = token;
  }

  @Override
  /**
   * 获取当前请求对应的Protobuf消息对象
   * @return 序列化后的Protobuf取消委托令牌请求消息
   */
  public CancelDelegationTokenRequestProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }


  /**
   * 将本地缓存的令牌对象合并到Protobuf构建器中
   */
  private void mergeLocalToBuilder() {
    if (token != null) {
      builder.setToken(convertToProtoFormat(this.token));
    }
  }

  /**
   * 将本地缓存的变更合并到Protobuf消息中
   */
  private void mergeLocalToProto() {
    if (viaProto) 
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  /**
   * 初始化Protobuf构建器，若当前基于已有Protobuf实例则基于它创建构建器
   */
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = CancelDelegationTokenRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }


  /**
   * 将Protobuf格式的令牌转换为Yarn Token对象
   * @param p Protobuf格式的令牌
   * @return 转换后的Yarn Token对象
   */
  private TokenPBImpl convertFromProtoFormat(TokenProto p) {
    return new TokenPBImpl(p);
  }

  /**
   * 将Yarn Token对象转换为Protobuf格式的令牌
   * @param t Yarn Token对象
   * @return 转换后的Protobuf格式令牌
   */
  private TokenProto convertToProtoFormat(Token t) {
    return ((TokenPBImpl)t).getProto();
  }
}