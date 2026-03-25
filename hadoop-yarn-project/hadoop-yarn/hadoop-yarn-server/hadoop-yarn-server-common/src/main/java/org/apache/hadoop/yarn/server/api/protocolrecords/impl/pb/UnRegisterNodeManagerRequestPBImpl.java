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
import org.apache.hadoop.yarn.proto.YarnProtos.NodeIdProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.UnRegisterNodeManagerRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.UnRegisterNodeManagerRequestProtoOrBuilder;
import org.apache.hadoop.yarn.server.api.protocolrecords.UnRegisterNodeManagerRequest;

/**
 * 取消NodeManager注册请求的Protobuf实现类
 */
public class UnRegisterNodeManagerRequestPBImpl extends
    UnRegisterNodeManagerRequest {
  private UnRegisterNodeManagerRequestProto proto =
      UnRegisterNodeManagerRequestProto.getDefaultInstance();
  private UnRegisterNodeManagerRequestProto.Builder builder = null;
  private boolean viaProto = false;

  private NodeId nodeId = null;

  /**
   * 构造空的取消NodeManager注册请求
   */
  public UnRegisterNodeManagerRequestPBImpl() {
    builder = UnRegisterNodeManagerRequestProto.newBuilder();
  }

  /**
   * 基于已有Protobuf对象构造取消NodeManager注册请求
   * @param proto Protobuf序列化后的请求对象
   */
  public UnRegisterNodeManagerRequestPBImpl(
      UnRegisterNodeManagerRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  /**
   * 获取当前请求对应的Protobuf对象
   * @return 序列化后的Protobuf请求对象
   */
  public UnRegisterNodeManagerRequestProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  /**
   * 将本地缓存的节点ID合并到Protobuf Builder中
   */
  private void mergeLocalToBuilder() {
    if (this.nodeId != null) {
      builder.setNodeId(convertToProtoFormat(this.nodeId));
    }
  }

  /**
   * 将本地缓存的字段合并到Protobuf对象
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
   * 如果需要，初始化Protobuf Builder
   */
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = UnRegisterNodeManagerRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public NodeId getNodeId() {
    UnRegisterNodeManagerRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (this.nodeId != null) {
      return this.nodeId;
    }
    if (!p.hasNodeId()) {
      return null;
    }
    this.nodeId = convertFromProtoFormat(p.getNodeId());
    return this.nodeId;
  }

  @Override
  public void setNodeId(NodeId updatedNodeId) {
    maybeInitBuilder();
    if (updatedNodeId == null) {
      builder.clearNodeId();
    }
    this.nodeId = updatedNodeId;
  }

  /**
   * 将Protobuf格式的NodeId转换为API层对象
   * @param p Protobuf格式NodeId
   * @return API层NodeId对象
   */
  private NodeIdPBImpl convertFromProtoFormat(NodeIdProto p) {
    return new NodeIdPBImpl(p);
  }

  /**
   * 将API层NodeId对象转换为Protobuf格式
   * @param t API层NodeId对象
   * @return Protobuf格式NodeId
   */
  private NodeIdProto convertToProtoFormat(NodeId t) {
    return ((NodeIdPBImpl) t).getProto();
  }
}