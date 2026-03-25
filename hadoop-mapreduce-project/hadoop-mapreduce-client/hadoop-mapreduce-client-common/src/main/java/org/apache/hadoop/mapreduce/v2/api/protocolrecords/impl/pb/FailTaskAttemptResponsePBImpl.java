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


import org.apache.hadoop.mapreduce.v2.api.protocolrecords.FailTaskAttemptResponse;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.FailTaskAttemptResponseProto;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoBase;

/**
 * 失败任务尝试请求响应的Protobuf实现类
 * 基于Protobuf序列化框架，实现FailTaskAttemptResponse接口，用于MapReduce服务端与客户端之间的通信
 */    
public class FailTaskAttemptResponsePBImpl extends ProtoBase<FailTaskAttemptResponseProto> implements FailTaskAttemptResponse {
  // 保存默认的Protobuf对象实例
  FailTaskAttemptResponseProto proto = FailTaskAttemptResponseProto.getDefaultInstance();
  // Protobuf构建器，用于构建和修改响应对象
  FailTaskAttemptResponseProto.Builder builder = null;
  // 标记当前是否通过已有Protobuf对象构造
  boolean viaProto = false;
  
  /**
   * 无参构造函数，初始化Protobuf构建器
   */
  public FailTaskAttemptResponsePBImpl() {
    builder = FailTaskAttemptResponseProto.newBuilder();
  }

  /**
   * 基于已有Protobuf对象构造响应实例
   * @param proto 已构造完成的FailTaskAttemptResponseProto对象
   */
  public FailTaskAttemptResponsePBImpl(FailTaskAttemptResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  @Override
  public FailTaskAttemptResponseProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  /**
   * 延迟初始化Protobuf构建器，确保修改对象时构建器可用
   */
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = FailTaskAttemptResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }

}