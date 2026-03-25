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

import org.apache.hadoop.mapreduce.v2.api.protocolrecords.CancelDelegationTokenResponse;
import org.apache.hadoop.security.proto.SecurityProtos.CancelDelegationTokenResponseProto;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoBase;

/**
 * 取消代理令牌响应的Protobuf实现类
 * 基于Protobuf序列化框架，实现了CancelDelegationTokenResponse接口，
 * 负责封装MapReduce客户端取消代理令牌请求的响应数据
 */
public class CancelDelegationTokenResponsePBImpl extends
    ProtoBase<CancelDelegationTokenResponseProto> implements
    CancelDelegationTokenResponse {

  // 存储Protobuf消息对象，默认使用默认实例初始化
  CancelDelegationTokenResponseProto proto = CancelDelegationTokenResponseProto
      .getDefaultInstance();

  /**
   * 无参构造函数，创建空的取消代理令牌响应对象
   */
  public CancelDelegationTokenResponsePBImpl() {
  }

  /**
   * 构造函数，基于已有Protobuf消息对象创建响应对象
   * @param proto 已序列化的取消代理令牌响应Protobuf消息
   */
  public CancelDelegationTokenResponsePBImpl(
      CancelDelegationTokenResponseProto proto) {
    this.proto = proto;
  }

  @Override
  public CancelDelegationTokenResponseProto getProto() {
    return proto;
  }

}