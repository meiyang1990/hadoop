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


import org.apache.hadoop.mapreduce.v2.api.protocolrecords.KillTaskResponse;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.KillTaskResponseProto;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoBase;

/**
 * 杀死任务响应的Protobuf实现类，基于Protobuf序列化协议实现KillTaskResponse接口
 * 负责处理MapReduce服务端与客户端之间杀死任务响应消息的序列化与反序列化
 */
public class KillTaskResponsePBImpl extends ProtoBase<KillTaskResponseProto> implements KillTaskResponse {
  // 存储已构建完成的Protobuf消息对象
  KillTaskResponseProto proto = KillTaskResponseProto.getDefaultInstance();
  // 用于构建Protobuf消息的Builder
  KillTaskResponseProto.Builder builder = null;
  // 标识当前是否使用已构建完成的Proto对象
  boolean viaProto = false;
  
  /**
   * 构造空的杀死任务响应对象，初始化Builder用于构建响应
   */
  public KillTaskResponsePBImpl() {
    builder = KillTaskResponseProto.newBuilder();
  }

  /**
   * 基于已有的Protobuf对象构造杀死任务响应
   * @param proto 已序列化完成的KillTaskResponseProto对象
   */
  public KillTaskResponsePBImpl(KillTaskResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  /**
   * 获取当前响应对应的Protobuf对象，用于网络传输序列化
   * @return 构建完成的KillTaskResponseProto对象
   */
  public KillTaskResponseProto getProto() {
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  /**
   * 初始化Builder，确保修改操作可以正确进行
   * 如果当前基于已构建的Proto对象，需要将内容合并到Builder中
   */
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = KillTaskResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  



}