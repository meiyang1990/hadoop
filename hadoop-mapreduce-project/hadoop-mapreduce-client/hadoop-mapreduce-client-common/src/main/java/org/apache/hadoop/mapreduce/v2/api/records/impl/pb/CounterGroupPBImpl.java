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


import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.mapreduce.v2.api.records.Counter;
import org.apache.hadoop.mapreduce.v2.api.records.CounterGroup;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.CounterGroupProto;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.CounterGroupProtoOrBuilder;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.CounterProto;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.StringCounterMapProto;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoBase;

/**
 * CounterGroup的Protobuf序列化实现，基于ProtoBase实现，用于MapReduce任务指标统计中同类别计数器组的存储与序列化。
 * 负责维护一组同类型计数器，在RPC通信和持久化中完成内存对象与Protobuf格式的互转。
 */    
public class CounterGroupPBImpl extends ProtoBase<CounterGroupProto> implements CounterGroup {
  CounterGroupProto proto = CounterGroupProto.getDefaultInstance();
  CounterGroupProto.Builder builder = null;
  boolean viaProto = false;
  
  // 内存中存储计数器集合，key为计数器名称，value为计数器实例
  private Map<String, Counter> counters = null;
  
  
  /**
   * 构造空的计数器组实例，用于构建新对象。
   */
  public CounterGroupPBImpl() {
    builder = CounterGroupProto.newBuilder();
  }

  /**
   * 基于已有Protobuf对象构造计数器组实例，用于反序列化。
   * @param proto 已序列化的CounterGroupProto对象
   */
  public CounterGroupPBImpl(CounterGroupProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  @Override
  public CounterGroupProto getProto() {
      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  private void mergeLocalToBuilder() {
    if (this.counters != null) {
      addContersToProto();
    }
  }

  private void mergeLocalToProto() {
    if (viaProto) 
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = CounterGroupProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public String getName() {
    CounterGroupProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasName()) {
      return null;
    }
    return (p.getName());
  }

  @Override
  public void setName(String name) {
    maybeInitBuilder();
    if (name == null) {
      builder.clearName();
      return;
    }
    builder.setName((name));
  }
  
  @Override
  public String getDisplayName() {
    CounterGroupProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasDisplayName()) {
      return null;
    }
    return (p.getDisplayName());
  }

  @Override
  public void setDisplayName(String displayName) {
    maybeInitBuilder();
    if (displayName == null) {
      builder.clearDisplayName();
      return;
    }
    builder.setDisplayName((displayName));
  }
  
  @Override
  public Map<String, Counter> getAllCounters() {
    initCounters();
    return this.counters;
  }
  
  @Override
  public Counter getCounter(String key) {
    initCounters();
    return this.counters.get(key);
  }
  
  /**
   * 延迟初始化计数器集合：从Protobuf对象反序列化所有计数器到内存Map。
   */
  private void initCounters() {
    if (this.counters != null) {
      return;
    }
    CounterGroupProtoOrBuilder p = viaProto ? proto : builder;
    List<StringCounterMapProto> list = p.getCountersList();
    this.counters = new HashMap<String, Counter>();

    // 将Protobuf格式的计数器转换为内存对象存入Map
    for (StringCounterMapProto c : list) {
      this.counters.put(c.getKey(), convertFromProtoFormat(c.getValue()));
    }
  }
  
  @Override
  public void addAllCounters(final Map<String, Counter> counters) {
    if (counters == null)
      return;
    initCounters();
    this.counters.putAll(counters);
  }
  
  /**
   * 将内存中所有计数器转换写入Protobuf Builder，完成序列化准备。
   */
  private void addContersToProto() {
    maybeInitBuilder();
    builder.clearCounters();
    if (counters == null)
      return;
    // 自定义Iterable实现，将内存计数器逐个转换为Protobuf格式
    Iterable<StringCounterMapProto> iterable = new Iterable<StringCounterMapProto>() {
      
      @Override
      public Iterator<StringCounterMapProto> iterator() {
        return new Iterator<StringCounterMapProto>() {
          
          Iterator<String> keyIter = counters.keySet().iterator();
          
          @Override
          public void remove() {
            throw new UnsupportedOperationException();
          }
          
          @Override
          public StringCounterMapProto next() {
            String key = keyIter.next();
            return StringCounterMapProto.newBuilder().setKey(key).setValue(convertToProtoFormat(counters.get(key))).build();
          }
          
          @Override
          public boolean hasNext() {
            return keyIter.hasNext();
          }
        };
      }
    };
    builder.addAllCounters(iterable);
  }
  
  @Override
  public void setCounter(String key, Counter val) {
    initCounters();
    this.counters.put(key, val);
  }
  
  @Override
  public void removeCounter(String key) {
    initCounters();
    this.counters.remove(key);
  }
  
  @Override
  public void clearCounters() {
    initCounters();
    this.counters.clear();
  }

  /**
   * 将Protobuf格式的Counter转换为内存PBImpl对象。
   * @param p Protobuf格式的Counter
   * @return 内存CounterPBImpl实例
   */
  private CounterPBImpl convertFromProtoFormat(CounterProto p) {
    return new CounterPBImpl(p);
  }

  /**
   * 将内存Counter对象转换为Protobuf格式。
   * @param t 内存Counter对象
   * @return Protobuf格式的CounterProto
   */
  private CounterProto convertToProtoFormat(Counter t) {
    return ((CounterPBImpl)t).getProto();
  }
}