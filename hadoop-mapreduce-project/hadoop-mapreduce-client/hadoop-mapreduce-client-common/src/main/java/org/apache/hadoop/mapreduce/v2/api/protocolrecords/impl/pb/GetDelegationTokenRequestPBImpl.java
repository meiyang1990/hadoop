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

import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetDelegationTokenRequest;
import org.apache.hadoop.security.proto.SecurityProtos.GetDelegationTokenRequestProto;
import org.apache.hadoop.security.proto.SecurityProtos.GetDelegationTokenRequestProtoOrBuilder;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoBase;

/**
 * 获取委托令牌请求的Protobuf实现类，基于ProtoBase实现PB序列化转换
 * 负责MapReduce中获取委托令牌请求参数的存储与Protobuf格式互转，用于安全认证通信
 */
public class GetDelegationTokenRequestPBImpl extends
      ProtoBase<GetDelegationTokenRequestProto> implements GetDelegationTokenRequest {
  
  String renewer;
  

  GetDelegationTokenRequestProto proto = 
      GetDelegationTokenRequestProto.getDefaultInstance();
  GetDelegationTokenRequestProto.Builder builder = null;
  boolean viaProto = false;
  
  /**
   * 构造空的获取委托令牌请求对象，初始化PB构建器
   */
  public GetDelegationTokenRequestPBImpl() {
    builder = GetDelegationTokenRequestProto.newBuilder();
  }
  
  /**
   * 基于已有的PB消息构造获取委托令牌请求对象
   * @param proto 已序列化的PB请求消息
   */
  public GetDelegationTokenRequestPBImpl (
      GetDelegationTokenRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  @Override
  /**
   * 获取委托令牌的更新者标识
   * @return 更新者用户标识
   */
  public String getRenewer(){
    // 根据当前状态选择使用已构建的proto还是构建器
    GetDelegationTokenRequestProtoOrBuilder p = viaProto ? proto : builder;
    // 本地缓存已加载直接返回
    if (this.renewer != null) {
      return this.renewer;
    }
    // 从PB消息中解析获取并缓存到本地
    this.renewer = p.getRenewer();
    return this.renewer;
  }
  
  @Override
  /**
   * 设置委托令牌的更新者标识
   * @param renewer 更新者用户标识
   */
  public void setRenewer(String renewer) {
    maybeInitBuilder();
    // 值为空时清空PB中的对应字段
    if (renewer == null) 
      builder.clearRenewer();
    this.renewer = renewer;
  }

  @Override
  /**
   * 获取序列化后的PB请求消息
   * @return 序列化完成的GetDelegationTokenRequestProto对象
   */
  public GetDelegationTokenRequestProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }
  

  /**
   * 将本地缓存的更新者字段合并到PB构建器中
   */
  private void mergeLocalToBuilder() {
    if (renewer != null) {
      builder.setRenewer(this.renewer);
    }
  }

  /**
   * 将本地修改合并到最终的PB消息中
   */
  private void mergeLocalToProto() {
    if (viaProto) 
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  /**
   * 延迟初始化PB构建器：如果当前基于现有proto，从proto初始化构建器
   */
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = GetDelegationTokenRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }   
}