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


import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.mapreduce.v2.api.protocolrecords.GetTaskAttemptCompletionEventsResponse;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptCompletionEvent;
import org.apache.hadoop.mapreduce.v2.api.records.impl.pb.TaskAttemptCompletionEventPBImpl;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.TaskAttemptCompletionEventProto;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.GetTaskAttemptCompletionEventsResponseProto;
import org.apache.hadoop.mapreduce.v2.proto.MRServiceProtos.GetTaskAttemptCompletionEventsResponseProtoOrBuilder;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoBase;

/**
 * GetTaskAttemptCompletionEventsResponse 的 Protobuf 实现类，
 * 封装获取任务尝试完成事件响应的 Protobuf 序列化/反序列化逻辑，
 * 用于 MapReduce 客户端与服务端之间的 RPC 通信数据转换。
 */
public class GetTaskAttemptCompletionEventsResponsePBImpl extends ProtoBase<GetTaskAttemptCompletionEventsResponseProto> implements GetTaskAttemptCompletionEventsResponse {
  // 存储已构建完成的 Protobuf 对象
  GetTaskAttemptCompletionEventsResponseProto proto = GetTaskAttemptCompletionEventsResponseProto.getDefaultInstance();
  // 用于构建 Protobuf 对象的 Builder
  GetTaskAttemptCompletionEventsResponseProto.Builder builder = null;
  // 标记当前数据是否来自已构建好的 Protobuf 对象
  boolean viaProto = false;
  
  // 存储业务层的任务尝试完成事件列表
  private List<TaskAttemptCompletionEvent> completionEvents = null;
  
  
  /**
   * 构造方法，初始化 Builder 用于构建新响应对象。
   */
  public GetTaskAttemptCompletionEventsResponsePBImpl() {
    builder = GetTaskAttemptCompletionEventsResponseProto.newBuilder();
  }

  /**
   * 构造方法，基于已有的 Protobuf 对象构造业务响应对象。
   * @param proto 已构建完成的 Protobuf 响应对象
   */
  public GetTaskAttemptCompletionEventsResponsePBImpl(GetTaskAttemptCompletionEventsResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }
  
  @Override
  public GetTaskAttemptCompletionEventsResponseProto getProto() {
      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  // 将本地缓存的业务对象数据合并到 Builder 中
  private void mergeLocalToBuilder() {
    if (this.completionEvents != null) {
      addCompletionEventsToProto();
    }
  }

  // 将本地缓存的数据合并生成最终的 Protobuf 对象
  private void mergeLocalToProto() {
    if (viaProto) 
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  // 若当前基于 Protobuf 读取数据，初始化 Builder 用于修改操作
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = GetTaskAttemptCompletionEventsResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }
    
  
  @Override
  public List<TaskAttemptCompletionEvent> getCompletionEventList() {
    initCompletionEvents();
    return this.completionEvents;
  }

  @Override
  public TaskAttemptCompletionEvent getCompletionEvent(int index) {
    initCompletionEvents();
    return this.completionEvents.get(index);
  }

  @Override
  public int getCompletionEventCount() {
    initCompletionEvents();
    return this.completionEvents.size();
  }
  
  // 延迟初始化：从 Protobuf 中解析出任务尝试完成事件列表
  private void initCompletionEvents() {
    if (this.completionEvents != null) {
      return;
    }
    // 根据当前状态选择 proto 或 builder
    GetTaskAttemptCompletionEventsResponseProtoOrBuilder p = viaProto ? proto : builder;
    List<TaskAttemptCompletionEventProto> list = p.getCompletionEventsList();
    this.completionEvents = new ArrayList<TaskAttemptCompletionEvent>();

    // 将 Protobuf 对象转换为业务层对象
    for (TaskAttemptCompletionEventProto c : list) {
      this.completionEvents.add(convertFromProtoFormat(c));
    }
  }
  
  @Override
  public void addAllCompletionEvents(final List<TaskAttemptCompletionEvent> completionEvents) {
    if (completionEvents == null)
      return;
    initCompletionEvents();
    this.completionEvents.addAll(completionEvents);
  }
  
  // 将本地缓存的任务完成事件列表转换为 Protobuf 格式，添加到 Builder 中
  private void addCompletionEventsToProto() {
    maybeInitBuilder();
    builder.clearCompletionEvents();
    if (completionEvents == null)
      return;
    // 自定义迭代器，批量完成业务对象到 Protobuf 对象的转换
    Iterable<TaskAttemptCompletionEventProto> iterable = new Iterable<TaskAttemptCompletionEventProto>() {
      @Override
      public Iterator<TaskAttemptCompletionEventProto> iterator() {
        return new Iterator<TaskAttemptCompletionEventProto>() {

          Iterator<TaskAttemptCompletionEvent> iter = completionEvents.iterator();

          @Override
          public boolean hasNext() {
            return iter.hasNext();
          }

          @Override
          public TaskAttemptCompletionEventProto next() {
            return convertToProtoFormat(iter.next());
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException();

          }
        };

      }
    };
    builder.addAllCompletionEvents(iterable);
  }

  @Override
  public void addCompletionEvent(TaskAttemptCompletionEvent completionEvents) {
    initCompletionEvents();
    this.completionEvents.add(completionEvents);
  }

  @Override
  public void removeCompletionEvent(int index) {
    initCompletionEvents();
    this.completionEvents.remove(index);
  }

  @Override
  public void clearCompletionEvents() {
    initCompletionEvents();
    this.completionEvents.clear();
  }

  // 将 Protobuf 格式的完成事件转换为业务层对象
  private TaskAttemptCompletionEventPBImpl convertFromProtoFormat(TaskAttemptCompletionEventProto p) {
    return new TaskAttemptCompletionEventPBImpl(p);
  }

  // 将业务层完成事件对象转换为 Protobuf 格式
  private TaskAttemptCompletionEventProto convertToProtoFormat(TaskAttemptCompletionEvent t) {
    return ((TaskAttemptCompletionEventPBImpl)t).getProto();
  }

}