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


import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetCountersResponse;
import org.apache.hadoop.mapreduce.v2.api.records.Counters;
import org.apache.hadoop.mapreduce.v2.api.records.impl.pb.CountersPBImpl;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.CountersProto;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.GetCountersResponseProto;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.GetCountersResponseProtoOrBuilder;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoBase;

/**
 * 获取计数器响应的Protobuf序列化实现类，负责将MapReduce服务获取计数器响应与Protobuf格式互相转换
 * 实现了GetCountersResponse接口，继承ProtoBase提供通用Proto序列化能力
 */    
public class GetCountersResponsePBImpl extends ProtoBase<GetCountersResponseProto> implements GetCountersResponse {
  // Protobuf消息对象，通过protobuf格式存储响应数据
  GetCountersResponseProto proto = GetCountersResponseProto.getDefaultInstance();
  // Protobuf消息构造器，用于构造响应消息
  GetCountersResponseProto.Builder builder = null;
  // 标记当前是否通过protobuf对象存储数据
  boolean viaProto = false;
  
  // 本地缓存的计数器对象
  private Counters counters = null;
  
  /**
   * 无参构造函数，初始化Protobuf构造器
   */
  public GetCountersResponsePBImpl() {
    builder = GetCountersResponseProto.newBuilder();
  }

  /**
   * 基于已有Protobuf对象构造响应对象
   * @param proto 已构造完成的GetCountersResponseProto对象
   */
  public GetCountersResponsePBImpl(GetCountersResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  /**
   * 获取当前响应对应的Protobuf对象，将本地修改合并后返回
   * @return 序列化后的GetCountersResponseProto对象
   */
  public GetCountersResponseProto getProto() {
      // 将本地缓存的计数器合并到Protobuf构造器中
      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  /**
   * 将本地缓存的计数器对象合并到Protobuf构造器
   */
  private void mergeLocalToBuilder() {
    if (this.counters != null) {
      builder.setCounters(convertToProtoFormat(this.counters));
    }
  }

  /**
   * 将本地修改合并到Protobuf对象
   */
  private void mergeLocalToProto() {
    // 如果当前是protobuf格式，先初始化构造器
    if (viaProto) 
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  /**
   * 初始化Protobuf构造器，如果当前是protobuf格式则基于现有proto创建
   */
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = GetCountersResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public Counters getCounters() {
    GetCountersResponseProtoOrBuilder p = viaProto ? proto : builder;
    // 本地已有缓存直接返回
    if (this.counters != null) {
      return this.counters;
    }
    // Protobuf中不存在计数器，返回null
    if (!p.hasCounters()) {
      return null;
    }
    // 从Protobuf格式转换为API对象并缓存
    this.counters = convertFromProtoFormat(p.getCounters());
    return this.counters;
  }

  @Override
  public void setCounters(Counters counters) {
    maybeInitBuilder();
    // 清空计数器字段
    if (counters == null) 
      builder.clearCounters();
    // 缓存计数器对象到本地
    this.counters = counters;
  }

  /**
   * 将Protobuf格式的计数器转换为API对象
   * @param p Protobuf格式计数器
   * @return API层计数器对象
   */
  private CountersPBImpl convertFromProtoFormat(CountersProto p) {
    return new CountersPBImpl(p);
  }

  /**
   * 将API层计数器对象转换为Protobuf格式
   * @param t API层计数器对象
   * @return Protobuf格式计数器
   */
  private CountersProto convertToProtoFormat(Counters t) {
    return ((CountersPBImpl)t).getProto();
  }

}