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


import org.apache.hadoop.mapreduce.v2.api.protocolrecords.KillTaskAttemptResponse;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.KillTaskAttemptResponseProto;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoBase;

/**
 * 终止任务尝试请求响应的Protobuf实现类，基于ProtoBase实现协议缓冲序列化
 * 封装MapReduce服务端对KillTaskAttempt请求的返回结果，负责PB对象与接口的转换
 */
public class KillTaskAttemptResponsePBImpl extends ProtoBase<KillTaskAttemptResponseProto> implements KillTaskAttemptResponse {
  // 持有的Protobuf协议对象实例
  KillTaskAttemptResponseProto proto = KillTaskAttemptResponseProto.getDefaultInstance();
  // Protobuf构建器，用于构造修改响应对象
  KillTaskAttemptResponseProto.Builder builder = null;
  // 标记当前是否直接通过proto实例存储数据
  boolean viaProto = false;
  
  /**
   * 构造空的终止任务尝试响应对象，初始化Protobuf构建器
   */
  public KillTaskAttemptResponsePBImpl() {
    builder = KillTaskAttemptResponseProto.newBuilder();
  }

  /**
   * 基于已有Protobuf对象构造终止任务尝试响应
   * @param proto 已构造完成的KillTaskAttemptResponseProto对象
   */
  public KillTaskAttemptResponsePBImpl(KillTaskAttemptResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  /**
   * 获取当前响应对应的Protobuf协议对象，用于RPC通信序列化
   * @return 构造完成的KillTaskAttemptResponseProto实例
   */
  public KillTaskAttemptResponseProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  /**
   * 延迟初始化Protobuf构建器，确保修改操作可执行
   * 当当前通过proto存储数据时，转换为通过builder进行修改
   */
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = KillTaskAttemptResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }
    

}