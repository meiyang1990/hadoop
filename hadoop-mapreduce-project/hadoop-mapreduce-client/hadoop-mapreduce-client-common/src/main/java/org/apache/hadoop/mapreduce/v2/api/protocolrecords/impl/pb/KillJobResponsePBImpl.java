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


import org.apache.hadoop.mapreduce.v2.api.protocolrecords.KillJobResponse;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.KillJobResponseProto;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoBase;

/**
 * 基于Protobuf实现的KillJobResponse协议实现类，负责封装杀死作业响应的序列化与反序列化
 * 实现了MapReduce服务协议中KillJob响应的PB格式转换逻辑，供客户端与服务端RPC通信使用
 */    
public class KillJobResponsePBImpl extends ProtoBase<KillJobResponseProto> implements KillJobResponse {
  // Protobuf协议的默认响应实例
  KillJobResponseProto proto = KillJobResponseProto.getDefaultInstance();
  // Protobuf构建器，用于构建响应对象
  KillJobResponseProto.Builder builder = null;
  // 标记当前是否通过已有proto实例构建
  boolean viaProto = false;
  
  /**
   * 无参构造函数，初始化Protobuf构建器，用于构造新的响应对象
   */
  public KillJobResponsePBImpl() {
    builder = KillJobResponseProto.newBuilder();
  }

  /**
   * 基于已有Protobuf对象构造KillJobResponse实现类，用于反序列化解析响应
   * @param proto 已反序列化完成的KillJobResponseProto对象
   */
  public KillJobResponsePBImpl(KillJobResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  @Override
  public KillJobResponseProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  /**
   * 按需初始化Protobuf构建器，确保在修改响应前构建器已正确初始化
   */
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = KillJobResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  



}