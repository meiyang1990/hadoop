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

package org.apache.hadoop.yarn.server.applicationhistoryservice.records.impl.pb;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.NodeIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.PriorityPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoUtils;
import org.apache.hadoop.yarn.api.records.impl.pb.ResourcePBImpl;
import org.apache.hadoop.yarn.proto.ApplicationHistoryServerProtos.ContainerStartDataProto;
import org.apache.hadoop.yarn.proto.ApplicationHistoryServerProtos.ContainerStartDataProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.NodeIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.PriorityProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceProto;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ContainerStartData;

import org.apache.hadoop.thirdparty.protobuf.TextFormat;

/**
 * 容器启动数据的Protobuf实现类，存储容器启动时的关键信息，用于应用历史服务
 */
public class ContainerStartDataPBImpl extends ContainerStartData {

  // Protobuf协议对象实例
  ContainerStartDataProto proto = ContainerStartDataProto.getDefaultInstance();
  // Protobuf构建器实例
  ContainerStartDataProto.Builder builder = null;
  // 标记当前是否通过已有Proto对象构建
  boolean viaProto = false;

  // 缓存容器ID对象
  private ContainerId containerId;
  // 缓存分配资源对象
  private Resource resource;
  // 缓存分配节点ID对象
  private NodeId nodeId;
  // 缓存优先级对象
  private Priority priority;

  /**
   * 空构造函数，初始化Protobuf构建器
   */
  public ContainerStartDataPBImpl() {
    builder = ContainerStartDataProto.newBuilder();
  }

  /**
   * 通过已有Protobuf对象构造容器启动数据实例
   * @param proto 已有的ContainerStartDataProto对象
   */
  public ContainerStartDataPBImpl(ContainerStartDataProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  @Override
  public ContainerId getContainerId() {
    // 已缓存直接返回
    if (this.containerId != null) {
      return this.containerId;
    }
    // 根据模式选择proto或builder
    ContainerStartDataProtoOrBuilder p = viaProto ? proto : builder;
    // 无该字段返回null
    if (!p.hasContainerId()) {
      return null;
    }
    // Protobuf转API对象并缓存
    this.containerId = convertFromProtoFormat(p.getContainerId());
    return this.containerId;
  }

  @Override
  public void setContainerId(ContainerId containerId) {
    // 确保builder已初始化
    maybeInitBuilder();
    // 清空字段如果传入null
    if (containerId == null) {
      builder.clearContainerId();
    }
    // 缓存对象
    this.containerId = containerId;
  }

  @Override
  public Resource getAllocatedResource() {
    if (this.resource != null) {
      return this.resource;
    }
    ContainerStartDataProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasAllocatedResource()) {
      return null;
    }
    this.resource = convertFromProtoFormat(p.getAllocatedResource());
    return this.resource;
  }

  @Override
  public void setAllocatedResource(Resource resource) {
    maybeInitBuilder();
    if (resource == null) {
      builder.clearAllocatedResource();
    }
    this.resource = resource;
  }

  @Override
  public NodeId getAssignedNode() {
    if (this.nodeId != null) {
      return this.nodeId;
    }
    ContainerStartDataProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasAssignedNodeId()) {
      return null;
    }
    this.nodeId = convertFromProtoFormat(p.getAssignedNodeId());
    return this.nodeId;
  }

  @Override
  public void setAssignedNode(NodeId nodeId) {
    maybeInitBuilder();
    if (nodeId == null) {
      builder.clearAssignedNodeId();
    }
    this.nodeId = nodeId;
  }

  @Override
  public Priority getPriority() {
    if (this.priority != null) {
      return this.priority;
    }
    ContainerStartDataProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasPriority()) {
      return null;
    }
    this.priority = convertFromProtoFormat(p.getPriority());
    return this.priority;
  }

  @Override
  public void setPriority(Priority priority) {
    maybeInitBuilder();
    if (priority == null) {
      builder.clearPriority();
    }
    this.priority = priority;
  }

  @Override
  public long getStartTime() {
    ContainerStartDataProtoOrBuilder p = viaProto ? proto : builder;
    // 直接从proto/builder获取启动时间
    return p.getStartTime();
  }

  @Override
  public void setStartTime(long startTime) {
    maybeInitBuilder();
    // 设置启动时间到builder
    builder.setStartTime(startTime);
  }

  /**
   * 获取当前对象对应的Protobuf proto对象，合并本地缓存到proto
   * @return 构建完成的ContainerStartDataProto对象
   */
  public ContainerStartDataProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  @Override
  public int hashCode() {
    // 基于proto计算哈希值
    return getProto().hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other == null)
      return false;
    // 类型兼容则比较proto对象是否相等
    if (other.getClass().isAssignableFrom(this.getClass())) {
      return this.getProto().equals(this.getClass().cast(other).getProto());
    }
    return false;
  }

  @Override
  public String toString() {
    // 使用Protobuf短调试格式输出
    return TextFormat.shortDebugString(getProto());
  }

  /**
   * 将本地缓存的API对象合并到Protobuf builder中
   */
  private void mergeLocalToBuilder() {
    if (this.containerId != null
        && !((ContainerIdPBImpl) this.containerId).getProto().equals(
          builder.getContainerId())) {
      builder.setContainerId(convertToProtoFormat(this.containerId));
    }
    if (this.resource != null) {
      builder.setAllocatedResource(convertToProtoFormat(this.resource));
    }
    if (this.nodeId != null
        && !((NodeIdPBImpl) this.nodeId).getProto().equals(
          builder.getAssignedNodeId())) {
      builder.setAssignedNodeId(convertToProtoFormat(this.nodeId));
    }
    if (this.priority != null
        && !((PriorityPBImpl) this.priority).getProto().equals(
          builder.getPriority())) {
      builder.setPriority(convertToProtoFormat(this.priority));
    }
  }

  /**
   * 将本地缓存合并到最终proto对象
   */
  private void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  /**
   * 如果需要，初始化builder，基于已有proto创建builder
   */
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = ContainerStartDataProto.newBuilder(proto);
    }
    viaProto = false;
  }

  /**
   * ContainerId API对象转Protobuf格式
   */
  private ContainerIdProto convertToProtoFormat(ContainerId containerId) {
    return ((ContainerIdPBImpl) containerId).getProto();
  }

  /**
   * Protobuf格式转ContainerId API对象
   */
  private ContainerIdPBImpl
      convertFromProtoFormat(ContainerIdProto containerId) {
    return new ContainerIdPBImpl(containerId);
  }

  /**
   * Resource API对象转Protobuf格式
   */
  private ResourceProto convertToProtoFormat(Resource resource) {
    return ProtoUtils.convertToProtoFormat(resource);
  }

  /**
   * Protobuf格式转Resource API对象
   */
  private ResourcePBImpl convertFromProtoFormat(ResourceProto resource) {
    return new ResourcePBImpl(resource);
  }

  /**
   * NodeId API对象转Protobuf格式
   */
  private NodeIdProto convertToProtoFormat(NodeId nodeId) {
    return ((NodeIdPBImpl) nodeId).getProto();
  }

  /**
   * Protobuf格式转NodeId API对象
   */
  private NodeIdPBImpl convertFromProtoFormat(NodeIdProto nodeId) {
    return new NodeIdPBImpl(nodeId);
  }

  /**
   * Priority API对象转Protobuf格式
   */
  private PriorityProto convertToProtoFormat(Priority priority) {
    return ((PriorityPBImpl) priority).getProto();
  }

  /**
   * Protobuf格式转Priority API对象
   */
  private PriorityPBImpl convertFromProtoFormat(PriorityProto priority) {
    return new PriorityPBImpl(priority);
  }

}