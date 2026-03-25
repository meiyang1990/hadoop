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

import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.impl.pb.NodeIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoUtils;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.RemoteNodeProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.RemoteNodeProtoOrBuilder;
import org.apache.hadoop.yarn.server.api.protocolrecords.RemoteNode;

/**
 * 基于Protobuf实现的RemoteNode，用于YARN服务端间远程节点信息的序列化传输
 * 实现了 {@link RemoteNode} 接口。
 */
public class RemoteNodePBImpl extends RemoteNode {

  // Protobuf协议对象
  private RemoteNodeProto proto = RemoteNodeProto.getDefaultInstance();
  // Protobuf构建器
  private RemoteNodeProto.Builder builder = null;
  // 是否直接使用proto对象标识，false表示正在通过builder构建
  private boolean viaProto = false;

  // 缓存节点ID对象
  private NodeId nodeId = null;

  /**
   * 空构造函数，初始化Protobuf构建器
   */
  public RemoteNodePBImpl() {
    builder = RemoteNodeProto.newBuilder();
  }

  /**
   * 基于已有Protobuf对象构造封装
   * @param proto 已有的RemoteNodeProto对象
   */
  public RemoteNodePBImpl(RemoteNodeProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  /**
   * 获取当前对象对应的Protobuf协议对象，合并本地修改后返回
   * @return 序列化用的RemoteNodeProto对象
   */
  public RemoteNodeProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  // 将本地缓存的节点信息合并到Protobuf构建器
  private void mergeLocalToBuilder() {
    if (this.nodeId != null
        && !((NodeIdPBImpl) nodeId).getProto().equals(
        builder.getNodeId())) {
      builder.setNodeId(ProtoUtils.convertToProtoFormat(this.nodeId));
    }
  }

  // 将本地缓存的所有修改合并到最终Protobuf对象
  private void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  // 延迟初始化Protobuf构建器，如果当前是只读proto则转换为可写builder
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = RemoteNodeProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public NodeId getNodeId() {
    RemoteNodeProtoOrBuilder p = viaProto ? proto : builder;
    if (this.nodeId != null) {
      return this.nodeId;
    }
    if (!p.hasNodeId()) {
      return null;
    }
    // 从Protobuf转换并缓存NodeId对象
    this.nodeId = ProtoUtils.convertFromProtoFormat(p.getNodeId());
    return this.nodeId;
  }

  @Override
  public void setNodeId(NodeId nodeId) {
    maybeInitBuilder();
    if (nodeId == null) {
      builder.clearNodeId();
    }
    this.nodeId = nodeId;
  }

  @Override
  public String getHttpAddress() {
    RemoteNodeProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasHttpAddress()) {
      return null;
    }
    return (p.getHttpAddress());
  }

  @Override
  public void setHttpAddress(String httpAddress) {
    maybeInitBuilder();
    if (httpAddress == null) {
      builder.clearHttpAddress();
      return;
    }
    builder.setHttpAddress(httpAddress);
  }

  @Override
  public String getRackName() {
    RemoteNodeProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasRackName()) {
      return null;
    }
    return (p.getRackName());
  }

  @Override
  public void setRackName(String rackName) {
    maybeInitBuilder();
    if (rackName == null) {
      builder.clearRackName();
      return;
    }
    builder.setRackName(rackName);
  }

  @Override
  public String getNodePartition() {
    RemoteNodeProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasNodePartition()) {
      return null;
    }
    return (p.getNodePartition());
  }

  @Override
  public void setNodePartition(String nodePartition) {
    maybeInitBuilder();
    if (nodePartition == null) {
      builder.clearNodePartition();
      return;
    }
    builder.setNodePartition(nodePartition);
  }

  @Override
  public int hashCode() {
    return getProto().hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other == null) {
      return false;
    }
    if (other.getClass().isAssignableFrom(this.getClass())) {
      return this.getProto().equals(this.getClass().cast(other).getProto());
    }
    return false;
  }
}