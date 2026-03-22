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
package org.apache.hadoop.mapreduce.v2.api.protocolrecords.impl.pb;

import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetDelegationTokenResponse;
import org.apache.hadoop.security.proto.SecurityProtos.GetDelegationTokenResponseProto;
import org.apache.hadoop.security.proto.SecurityProtos.GetDelegationTokenResponseProtoOrBuilder;
import org.apache.hadoop.security.proto.SecurityProtos.TokenProto;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoBase;
import org.apache.hadoop.yarn.api.records.impl.pb.TokenPBImpl;

/**
 * 获取MapReduce委托令牌响应的Protobuf实现类，负责协议数据的序列化与反序列化
 * 基于Protobuf实现GetDelegationTokenResponse接口，封装RPC响应数据转换逻辑
 */
public class GetDelegationTokenResponsePBImpl extends
      ProtoBase<GetDelegationTokenResponseProto> implements GetDelegationTokenResponse {
  
  // MapReduce委托令牌对象
  Token mrToken;
  

  GetDelegationTokenResponseProto proto = 
      GetDelegationTokenResponseProto.getDefaultInstance();
  // Protobuf构建器，用于构造响应对象
  GetDelegationTokenResponseProto.Builder builder = null;
  // 标识当前是否通过Protobuf对象构造，控制数据访问方式
  boolean viaProto = false;
  
  /**
   * 构造空响应对象，初始化Protobuf构建器
   */
  public GetDelegationTokenResponsePBImpl() {
    builder = GetDelegationTokenResponseProto.newBuilder();
  }
  
  /**
   * 基于已有Protobuf对象构造响应对象
   * @param proto 已序列化的GetDelegationTokenResponseProto对象
   */
  public GetDelegationTokenResponsePBImpl (
      GetDelegationTokenResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  @Override
  public Token getDelegationToken() {
    // 根据当前数据存储方式选择Protobuf或构建器
    GetDelegationTokenResponseProtoOrBuilder p = viaProto ? proto : builder;
    // 本地已缓存直接返回
    if (this.mrToken != null) {
      return this.mrToken;
    }
    // Protobuf中不存在令牌，返回null
    if (!p.hasToken()) {
      return null;
    }
    // 从Protobuf转换令牌并缓存到本地
    this.mrToken = convertFromProtoFormat(p.getToken());
    return this.mrToken;  
  }
  
  @Override
  public void setDelegationToken(Token mrToken) {
    // 确保构建器已初始化
    maybeInitBuilder();
    // 设置空令牌时清空构建器
    if (mrToken == null) 
      builder.getToken();
    // 缓存令牌到本地
    this.mrToken = mrToken;
  }

  @Override
  public GetDelegationTokenResponseProto getProto() {
    // 合并本地修改到Protobuf
    mergeLocalToProto();
    // 生成最终Protobuf对象并标记为通过Protobuf访问
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }
  

  /**
   * 将本地修改的令牌合并到Protobuf构建器中
   */
  private void mergeLocalToBuilder() {
    if (mrToken != null) {
      builder.setToken(convertToProtoFormat(this.mrToken));
    }
  }

  /**
   * 合并本地修改并生成最终Protobuf对象
   */
  private void mergeLocalToProto() {
    if (viaProto) 
      // 如果当前是Protobuf模式，先初始化构建器以便修改
      maybeInitBuilder();
    // 合并本地修改到构建器
    mergeLocalToBuilder();
    // 构建最终Protobuf对象
    proto = builder.build();
    viaProto = true;
  }

  /**
   * 按需初始化Protobuf构建器，从现有Protobuf对象拷贝数据
   */
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = GetDelegationTokenResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }

  /**
   * 将Protobuf格式的令牌转换为YARN API令牌对象
   * @param p Protobuf格式的TokenProto
   * @return YARN API Token对象
   */
  private TokenPBImpl convertFromProtoFormat(TokenProto p) {
    return new TokenPBImpl(p);
  }

  /**
   * 将YARN API令牌对象转换为Protobuf格式
   * @param t YARN API Token对象
   * @return Protobuf格式的TokenProto
   */
  private TokenProto convertToProtoFormat(Token t) {
    return ((TokenPBImpl)t).getProto();
  }
}