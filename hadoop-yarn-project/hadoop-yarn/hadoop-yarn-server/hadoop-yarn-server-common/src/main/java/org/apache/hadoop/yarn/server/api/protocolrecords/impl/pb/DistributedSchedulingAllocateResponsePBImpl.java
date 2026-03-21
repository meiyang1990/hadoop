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

package org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb;

import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;

import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.AllocateResponsePBImpl;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.RemoteNodeProto;
import org.apache.hadoop.yarn.server.api.protocolrecords.DistributedSchedulingAllocateResponse;


import org.apache.hadoop.yarn.server.api.protocolrecords.RemoteNode;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * 分布式调度分配响应的Protobuf实现类，基于PB序列化协议实现数据编解码
 */
public class DistributedSchedulingAllocateResponsePBImpl extends
    DistributedSchedulingAllocateResponse {

  // Protobuf消息对象，当通过PB反序列化生成实例时使用
  YarnServerCommonServiceProtos.DistributedSchedulingAllocateResponseProto
      proto = YarnServerCommonServiceProtos.
          DistributedSchedulingAllocateResponseProto.getDefaultInstance();
  // Protobuf构建器，当构建新消息对象时使用
  YarnServerCommonServiceProtos.DistributedSchedulingAllocateResponseProto.
      Builder builder = null;
  // 标记当前数据是否存储在proto中，用于本地数据与proto数据合并逻辑
  boolean viaProto = false;

  // 基础分配响应对象，包含标准分配响应信息
  private AllocateResponse allocateResponse;
  // 待调度远程节点列表，分布式调度中需要节点管理器处理的目标节点
  private List<RemoteNode> nodesForScheduling;

  /**
   * 构造空的PB实现实例，用于后续构建消息
   */
  public DistributedSchedulingAllocateResponsePBImpl() {
    builder = YarnServerCommonServiceProtos.
        DistributedSchedulingAllocateResponseProto.newBuilder();
  }

  /**
   * 基于已有的Protobuf消息构造实例
   * @param proto 序列化后的PB消息对象
   */
  public DistributedSchedulingAllocateResponsePBImpl(
      YarnServerCommonServiceProtos.
      DistributedSchedulingAllocateResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  /**
   * 获取当前对象对应的Protobuf消息，合并本地修改后返回
   * @return 序列化完成的PB消息对象
   */
  public YarnServerCommonServiceProtos.
      DistributedSchedulingAllocateResponseProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  /**
   * 确保builder已初始化，如果当前是proto模式则基于proto构建builder
   */
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = YarnServerCommonServiceProtos.
          DistributedSchedulingAllocateResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }

  /**
   * 将本地修改合并到proto对象中
   */
  private synchronized void mergeLocalToProto() {
    if (viaProto)
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  /**
   * 将本地缓存的业务对象合并到Protobuf builder中
   */
  private synchronized void mergeLocalToBuilder() {
    if (this.nodesForScheduling != null) {
      builder.clearNodesForScheduling();
      Iterable<YarnServerCommonServiceProtos.RemoteNodeProto> iterable =
          getNodeIdProtoIterable(this.nodesForScheduling);
      builder.addAllNodesForScheduling(iterable);
    }
    if (this.allocateResponse != null) {
      builder.setAllocateResponse(
          ((AllocateResponsePBImpl) this.allocateResponse).getProto());
    }
  }

  @Override
  public void setAllocateResponse(AllocateResponse response) {
    maybeInitBuilder();
    if (allocateResponse == null) {
      builder.clearAllocateResponse();
    }
    this.allocateResponse = response;
  }

  @Override
  public AllocateResponse getAllocateResponse() {
    // 已缓存直接返回
    if (this.allocateResponse != null) {
      return this.allocateResponse;
    }

    YarnServerCommonServiceProtos.
        DistributedSchedulingAllocateResponseProtoOrBuilder p =
            viaProto ? proto : builder;
    // proto中不存在该字段则返回null
    if (!p.hasAllocateResponse()) {
      return null;
    }

    // 从PB反序列化为业务对象并缓存
    this.allocateResponse = new AllocateResponsePBImpl(p.getAllocateResponse());
    return this.allocateResponse;
  }

  @Override
  public void setNodesForScheduling(List<RemoteNode> nodesForScheduling) {
    maybeInitBuilder();
    // 清空处理
    if (nodesForScheduling == null || nodesForScheduling.isEmpty()) {
      if (this.nodesForScheduling != null) {
        this.nodesForScheduling.clear();
      }
      builder.clearNodesForScheduling();
      return;
    }
    // 复制传入列表数据到本地缓存
    this.nodesForScheduling = new ArrayList<>();
    this.nodesForScheduling.addAll(nodesForScheduling);
  }

  @Override
  public List<RemoteNode> getNodesForScheduling() {
    // 已初始化直接返回
    if (nodesForScheduling != null) {
      return nodesForScheduling;
    }
    // 从PB反序列化初始化
    initLocalNodesForSchedulingList();
    return nodesForScheduling;
  }

  /**
   * 从Protobuf消息反序列化为本地RemoteNode列表缓存
   */
  private synchronized void initLocalNodesForSchedulingList() {
    YarnServerCommonServiceProtos.
        DistributedSchedulingAllocateResponseProtoOrBuilder p =
            viaProto ? proto : builder;
    List<YarnServerCommonServiceProtos.RemoteNodeProto> list =
        p.getNodesForSchedulingList();
    nodesForScheduling = new ArrayList<>();
    if (list != null) {
      for (YarnServerCommonServiceProtos.RemoteNodeProto t : list) {
        nodesForScheduling.add(new RemoteNodePBImpl(t));
      }
    }
  }

  /**
   * 将RemoteNode业务对象列表转换为可迭代的Protobuf对象集合，用于写入builder
   * @param nodeList 业务层RemoteNode列表
   * @return 转换后的Protobuf对象可迭代接口
   */
  private synchronized Iterable<RemoteNodeProto> getNodeIdProtoIterable(
      final List<RemoteNode> nodeList) {
    maybeInitBuilder();
    return new Iterable<RemoteNodeProto>() {
      @Override
      public synchronized Iterator<RemoteNodeProto> iterator() {
        return new Iterator<RemoteNodeProto>() {

          Iterator<RemoteNode> iter = nodeList.iterator();

          @Override
          public boolean hasNext() {
            return iter.hasNext();
          }

          @Override
          public RemoteNodeProto next() {
            return ((RemoteNodePBImpl)iter.next()).getProto();
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException();
          }
        };
      }
    };
  }
}