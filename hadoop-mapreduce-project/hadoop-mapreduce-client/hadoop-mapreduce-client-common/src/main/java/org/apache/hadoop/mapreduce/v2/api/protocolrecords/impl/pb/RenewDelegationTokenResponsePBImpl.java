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


import org.apache.hadoop.mapreduce.v2.api.protocolrecords.RenewDelegationTokenResponse;
import org.apache.hadoop.security.proto.SecurityProtos.RenewDelegationTokenResponseProto;
import org.apache.hadoop.security.proto.SecurityProtos.RenewDelegationTokenResponseProtoOrBuilder;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoBase;

/**
 * 刷新委托令牌响应的Protobuf序列化实现类
 * 实现了MapReduce协议中刷新委托令牌响应接口，基于Protobuf进行数据序列化
 */
public class RenewDelegationTokenResponsePBImpl extends
    ProtoBase<RenewDelegationTokenResponseProto> implements
    RenewDelegationTokenResponse {
  
  // Protobuf消息实例，当通过已构建的proto构造时使用
  RenewDelegationTokenResponseProto proto =
      RenewDelegationTokenResponseProto.getDefaultInstance();
  // Protobuf消息构建器，当需要修改消息内容时使用
  RenewDelegationTokenResponseProto.Builder builder = null;
  // 标记当前是否通过现成proto实例构建，控制构建流程
  boolean viaProto = false;

  /**
   * 无参构造函数，初始化空的Protobuf构建器
   */
  public RenewDelegationTokenResponsePBImpl() {
    this.builder = RenewDelegationTokenResponseProto.newBuilder();
  }

  /**
   * 基于现有Protobuf消息的构造函数
   * @param proto 已构建好的刷新委托令牌响应Protobuf消息
   */
  public RenewDelegationTokenResponsePBImpl (
      RenewDelegationTokenResponseProto proto) {
    this.proto = proto;
    this.viaProto = true;
  }

  @Override
  public RenewDelegationTokenResponseProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }
  
  /**
   * 延迟初始化Protobuf构建器，确保修改消息前构建器可用
   */
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = RenewDelegationTokenResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }
  
  @Override
  public long getNextExpirationTime() {
    RenewDelegationTokenResponseProtoOrBuilder p = viaProto ? proto : builder;
    return p.getNewExpiryTime();
  }

  @Override
  public void setNextExpirationTime(long expTime) {
    maybeInitBuilder();
    builder.setNewExpiryTime(expTime);
  }
}