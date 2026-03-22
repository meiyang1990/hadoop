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
import org.apache.hadoop.mapreduce.v2.api.records.Counters;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.CounterGroupProto;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.CountersProto;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.CountersProtoOrBuilder;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.StringCounterGroupMapProto;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoBase;

/**
 * 基于Protobuf实现的MapReduce计数器集合实现类
 * 负责管理多个计数器分组，提供计数器的增删改查和增量更新功能
 * 维护Java对象模型与Protobuf PB数据结构之间的转换
 */
public class CountersPBImpl extends ProtoBase<CountersProto> implements Counters {
  // Protobuf默认实例，当viaProto为true时使用
  CountersProto proto = CountersProto.getDefaultInstance();
  // Protobuf构建器，当viaProto为false时使用
  CountersProto.Builder builder = null;
  // 当前是否使用proto存储数据，false表示使用本地Java对象存储
  boolean viaProto = false;

  // 本地缓存的计数器分组集合，key为分组名称
  private Map<String, CounterGroup> counterGroups = null;

  /**
   * 构造空的计数器集合对象，初始化Protobuf构建器
   */
  public CountersPBImpl() {
    builder = CountersProto.newBuilder();
  }

  /**
   * 基于已有Protobuf对象构造计数器集合
   * @param proto 已有的CountersProto对象
   */
  public CountersPBImpl(CountersProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  /**
   * 获取当前计数器集合对应的Protobuf对象，合并本地修改到proto
   * @return 转换后的CountersProto对象
   */
  public CountersProto getProto() {
      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  /**
   * 将本地缓存的计数器分组合并到Protobuf构建器中
   */
  private void mergeLocalToBuilder() {
    if (this.counterGroups != null) {
      addCounterGroupsToProto();
    }
  }

  /**
   * 将本地修改合并到Protobuf对象，完成本地到PB的转换
   */
  private void mergeLocalToProto() {
    if (viaProto) 
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  /**
   * 如果当前使用proto存储，则初始化Protobuf构建器
   * 准备进行本地修改
   */
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = CountersProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  @Override
  public Map<String, CounterGroup> getAllCounterGroups() {
    initCounterGroups();
    return this.counterGroups;
  }

  @Override
  public CounterGroup getCounterGroup(String key) {
    initCounterGroups();
    return this.counterGroups.get(key);
  }

  @Override
  public Counter getCounter(Enum<?> key) {
    CounterGroup group = getCounterGroup(key.getDeclaringClass().getName());
    return group == null ? null : group.getCounter(key.name());
  }

  @Override
  public void incrCounter(Enum<?> key, long amount) {
    String groupName = key.getDeclaringClass().getName();
    if (getCounterGroup(groupName) == null) {
      CounterGroup cGrp = new CounterGroupPBImpl();
      cGrp.setName(groupName);
      cGrp.setDisplayName(groupName);
      setCounterGroup(groupName, cGrp);
    }
    if (getCounterGroup(groupName).getCounter(key.name()) == null) {
      Counter c = new CounterPBImpl();
      c.setName(key.name());
      c.setDisplayName(key.name());
      c.setValue(0l);
      getCounterGroup(groupName).setCounter(key.name(), c);
    }
    Counter counter = getCounterGroup(groupName).getCounter(key.name());
    counter.setValue(counter.getValue() + amount);
  }

  /**
   * 从Protobuf初始化本地缓存的计数器分组集合，懒加载机制
   */
  private void initCounterGroups() {
    if (this.counterGroups != null) {
      return;
    }
    CountersProtoOrBuilder p = viaProto ? proto : builder;
    List<StringCounterGroupMapProto> list = p.getCounterGroupsList();
    this.counterGroups = new HashMap<String, CounterGroup>();

    for (StringCounterGroupMapProto c : list) {
      this.counterGroups.put(c.getKey(), convertFromProtoFormat(c.getValue()));
    }
  }
  
  @Override
  public void addAllCounterGroups(final Map<String, CounterGroup> counterGroups) {
    if (counterGroups == null)
      return;
    initCounterGroups();
    this.counterGroups.putAll(counterGroups);
  }
  
  /**
   * 将本地缓存的计数器分组转换后添加到Protobuf构建器
   */
  private void addCounterGroupsToProto() {
    maybeInitBuilder();
    builder.clearCounterGroups();
    if (counterGroups == null)
      return;
    Iterable<StringCounterGroupMapProto> iterable = new Iterable<StringCounterGroupMapProto>() {
      
      @Override
      public Iterator<StringCounterGroupMapProto> iterator() {
        return new Iterator<StringCounterGroupMapProto>() {
          
          Iterator<String> keyIter = counterGroups.keySet().iterator();
          
          @Override
          public void remove() {
            throw new UnsupportedOperationException();
          }
          
          @Override
          public StringCounterGroupMapProto next() {
            String key = keyIter.next();
            return StringCounterGroupMapProto.newBuilder().setKey(key).setValue(convertToProtoFormat(counterGroups.get(key))).build();
          }
          
          @Override
          public boolean hasNext() {
            return keyIter.hasNext();
          }
        };
      }
    };
    builder.addAllCounterGroups(iterable);
  }

  @Override
  public void setCounterGroup(String key, CounterGroup val) {
    initCounterGroups();
    this.counterGroups.put(key, val);
  }

  @Override
  public void removeCounterGroup(String key) {
    initCounterGroups();
    this.counterGroups.remove(key);
  }

  @Override
  public void clearCounterGroups() {
    initCounterGroups();
    this.counterGroups.clear();
  }

  /**
   * 将Protobuf格式的CounterGroup转换为Java对象实现
   * @param p Protobuf格式的CounterGroupProto
   * @return 转换后的CounterGroupPBImpl对象
   */
  private CounterGroupPBImpl convertFromProtoFormat(CounterGroupProto p) {
    return new CounterGroupPBImpl(p);
  }

  /**
   * 将Java对象格式的CounterGroup转换为Protobuf格式
   * @param t Java对象格式的CounterGroup
   * @return 转换后的CounterGroupProto对象
   */
  private CounterGroupProto convertToProtoFormat(CounterGroup t) {
    return ((CounterGroupPBImpl)t).getProto();
  }
}