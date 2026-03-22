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

package org.apache.hadoop.mapreduce.v2.api.records.impl.pb;


import org.apache.hadoop.mapreduce.v2.api.records.Counter;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.CounterProto;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.CounterProtoOrBuilder;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoBase;

/**
 * 基于Protobuf实现的MapReduce计数器PB实现类
 * 负责存储MapReduce作业/任务运行过程中的计量指标数据，封装Protobuf序列化逻辑
 */    
public class CounterPBImpl extends ProtoBase<CounterProto> implements Counter {
  // 已构建完成的Protobuf对象实例
  CounterProto proto = CounterProto.getDefaultInstance();
  // Protobuf构建器，用于修改计数器数据
  CounterProto.Builder builder = null;
  // 标识当前数据是否存储在proto中，false表示数据正在builder中修改
  boolean viaProto = false;
  
  /**
   * 构造空的计数器PB实现对象
   */
  public CounterPBImpl() {
    builder = CounterProto.newBuilder();
  }

  /**
   * 基于已有Protobuf对象构造计数器PB实现对象
   * @param proto 已有的CounterProto对象
   */
  public CounterPBImpl(CounterProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  @Override
  public CounterProto getProto() {
    // 如果数据在proto中直接返回，否则从builder构建proto
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  /**
   * 初始化builder，确保可以对数据进行修改
   * 如果当前使用proto存储，则将数据拷贝到builder中
   */
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = CounterProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public String getName() {
    // 选择当前存储数据的对象
    CounterProtoOrBuilder p = viaProto ? proto : builder;
    // 如果名称不存在返回null
    if (!p.hasName()) {
      return null;
    }
    return (p.getName());
  }

  @Override
  public void setName(String name) {
    // 确保builder已初始化
    maybeInitBuilder();
    // 如果名称为空，清除原有设置
    if (name == null) {
      builder.clearName();
      return;
    }
    builder.setName((name));
  }
  
  @Override
  public long getValue() {
    CounterProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getValue());
  }

  @Override
  public void setValue(long value) {
    maybeInitBuilder();
    builder.setValue((value));
  }
  
  @Override
  public String getDisplayName() {
    CounterProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasDisplayName()) {
      return null;
    }
    return (p.getDisplayName());
  }

  @Override
  public void setDisplayName(String displayName) {
    maybeInitBuilder();
    // 如果显示名称为空，清除原有设置
    if (displayName == null) {
      builder.clearDisplayName();
      return;
    }
    builder.setDisplayName((displayName));
  }

}