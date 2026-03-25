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

import org.apache.hadoop.thirdparty.protobuf.TextFormat;

import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;

import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.RegisterApplicationMasterResponsePBImpl;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoUtils;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.RemoteNodeProto;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterDistributedSchedulingAMResponse;


import org.apache.hadoop.yarn.server.api.protocolrecords.RemoteNode;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * 文件说明：分布式调度AM注册响应的Protobuf实现类，实现分布式调度场景下ApplicationMaster注册响应的序列化/反序列化
 * 实现了 {@link RegisterDistributedSchedulingAMResponse} 接口。
 */
public class RegisterDistributedSchedulingAMResponsePBImpl extends
    RegisterDistributedSchedulingAMResponse {

  // Protobuf协议对象，存储序列化后的响应数据
  YarnServerCommonServiceProtos.RegisterDistributedSchedulingAMResponseProto
      proto =
          YarnServerCommonServiceProtos.
          RegisterDistributedSchedulingAMResponseProto
              .getDefaultInstance();
  // Protobuf构建器，用于构造修改协议对象
  YarnServerCommonServiceProtos.RegisterDistributedSchedulingAMResponseProto.
      Builder builder = null;
  // 标识当前数据是否存储在proto对象中，控制本地对象与proto的合并逻辑
  boolean viaProto = false;

  // 容器最大资源限制本地缓存
  private Resource maxContainerResource;
  // 容器最小资源限制本地缓存
  private Resource minContainerResource;
  // 容器增量资源配置本地缓存
  private Resource incrContainerResource;
  // 可调度节点列表本地缓存
  private List<RemoteNode> nodesForScheduling;
  // 基础AM注册响应本地缓存
  private RegisterApplicationMasterResponse registerApplicationMasterResponse;

  /**
   * 无参构造函数，初始化Protobuf构建器
   */
  public RegisterDistributedSchedulingAMResponsePBImpl() {
    builder = YarnServerCommonServiceProtos.
        RegisterDistributedSchedulingAMResponseProto.newBuilder();
  }

  /**
   * 基于已有Protobuf对象构造响应对象
   * @param proto 已序列化的Protobuf响应对象
   */
  public RegisterDistributedSchedulingAMResponsePBImpl(
      YarnServerCommonServiceProtos.
          RegisterDistributedSchedulingAMResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  /**
   * 获取当前响应的Protobuf协议对象，合并本地修改后返回
   * @return 合并完成的Protobuf协议对象
   */
  public YarnServerCommonServiceProtos.
      RegisterDistributedSchedulingAMResponseProto
          getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  /**
   * 延迟初始化Protobuf构建器，当需要修改对象时确保构建器可用
   */
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = YarnServerCommonServiceProtos.
          RegisterDistributedSchedulingAMResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }

  /**
   * 将本地缓存的修改合并到Protobuf协议对象
   */
  private synchronized void mergeLocalToProto() {
    if (viaProto)
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  /**
   * 将所有本地缓存的字段合并到Protobuf构建器
   */
  private synchronized void mergeLocalToBuilder() {
    if (this.nodesForScheduling != null) {
      builder.clearNodesForScheduling();
      Iterable<YarnServerCommonServiceProtos.RemoteNodeProto> iterable =
          getNodeIdProtoIterable(this.nodesForScheduling);
      builder.addAllNodesForScheduling(iterable);
    }
    if (this.maxContainerResource != null) {
      builder.setMaxContainerResource(ProtoUtils.convertToProtoFormat(
          this.maxContainerResource));
    }
    if (this.minContainerResource != null) {
      builder.setMinContainerResource(ProtoUtils.convertToProtoFormat(
          this.minContainerResource));
    }
    if (this.incrContainerResource != null) {
      builder.setIncrContainerResource(
          ProtoUtils.convertToProtoFormat(this.incrContainerResource));
    }
    if (this.registerApplicationMasterResponse != null) {
      builder.setRegisterResponse(
          ((RegisterApplicationMasterResponsePBImpl)
              this.registerApplicationMasterResponse).getProto());
    }
  }

  @Override
  public void setRegisterResponse(RegisterApplicationMasterResponse resp) {
    maybeInitBuilder();
    if (registerApplicationMasterResponse == null) {
      builder.clearRegisterResponse();
    }
    this.registerApplicationMasterResponse = resp;
  }

  @Override
  public RegisterApplicationMasterResponse getRegisterResponse() {
    if (this.registerApplicationMasterResponse != null) {
      return this.registerApplicationMasterResponse;
    }

    YarnServerCommonServiceProtos.
        RegisterDistributedSchedulingAMResponseProtoOrBuilder p =
            viaProto ? proto : builder;
    if (!p.hasRegisterResponse()) {
      return null;
    }

    this.registerApplicationMasterResponse =
        new RegisterApplicationMasterResponsePBImpl(p.getRegisterResponse());
    return this.registerApplicationMasterResponse;
  }

  @Override
  public void setMaxContainerResource(Resource maxResource) {
    maybeInitBuilder();
    if (maxContainerResource == null) {
      builder.clearMaxContainerResource();
    }
    this.maxContainerResource = maxResource;
  }

  @Override
  public Resource getMaxContainerResource() {
    if (this.maxContainerResource != null) {
      return this.maxContainerResource;
    }

    YarnServerCommonServiceProtos.
        RegisterDistributedSchedulingAMResponseProtoOrBuilder p =
            viaProto ? proto : builder;
    if (!p.hasMaxContainerResource()) {
      return null;
    }

    this.maxContainerResource = ProtoUtils.convertFromProtoFormat(p
        .getMaxContainerResource());
    return this.maxContainerResource;
  }

  @Override
  public void setMinContainerResource(Resource minResource) {
    maybeInitBuilder();
    if (minResource == null) {
      builder.clearMinContainerResource();
    }
    this.minContainerResource = minResource;
  }

  @Override
  public Resource getMinContainerResource() {
    if (this.minContainerResource != null) {
      return this.minContainerResource;
    }

    YarnServerCommonServiceProtos.
        RegisterDistributedSchedulingAMResponseProtoOrBuilder p =
            viaProto ? proto : builder;
    if (!p.hasMinContainerResource()) {
      return null;
    }

    this.minContainerResource = ProtoUtils.convertFromProtoFormat(p
        .getMinContainerResource());
    return this.minContainerResource;
  }

  @Override
  public void setIncrContainerResource(Resource incrResource) {
    maybeInitBuilder();
    if (incrResource == null) {
      builder.clearIncrContainerResource();
    }
    this.incrContainerResource = incrResource;
  }

  @Override
  public Resource getIncrContainerResource() {
    if (this.incrContainerResource != null) {
      return this.incrContainerResource;
    }

    YarnServerCommonServiceProtos.
        RegisterDistributedSchedulingAMResponseProtoOrBuilder p =
            viaProto ? proto : builder;
    if (!p.hasIncrContainerResource()) {
      return null;
    }

    this.incrContainerResource = ProtoUtils.convertFromProtoFormat(p
        .getIncrContainerResource());
    return this.incrContainerResource;
  }

  @Override
  public void setContainerTokenExpiryInterval(int interval) {
    maybeInitBuilder();
    builder.setContainerTokenExpiryInterval(interval);
  }

  @Override
  public int getContainerTokenExpiryInterval() {
    YarnServerCommonServiceProtos.
        RegisterDistributedSchedulingAMResponseProtoOrBuilder p =
            viaProto ? proto : builder;
    if (!p.hasContainerTokenExpiryInterval()) {
      return 0;
    }
    return p.getContainerTokenExpiryInterval();
  }

  @Override
  public void setContainerIdStart(long containerIdStart) {
    maybeInitBuilder();
    builder.setContainerIdStart(containerIdStart);
  }

  @Override
  public long getContainerIdStart() {
    YarnServerCommonServiceProtos.
        RegisterDistributedSchedulingAMResponseProtoOrBuilder p =
            viaProto ? proto : builder;
    if (!p.hasContainerIdStart()) {
      return 0;
    }
    return p.getContainerIdStart();
  }

  @Override
  public void setNodesForScheduling(List<RemoteNode> nodesForScheduling) {
    maybeInitBuilder();
    if (nodesForScheduling == null || nodesForScheduling.isEmpty()) {
      if (this.nodesForScheduling != null) {
        this.nodesForScheduling.clear();
      }
      builder.clearNodesForScheduling();
      return;
    }
    this.nodesForScheduling = new ArrayList<>();
    this.nodesForScheduling.addAll(nodesForScheduling);
  }

  @Override
  public List<RemoteNode> getNodesForScheduling() {
    if (nodesForScheduling != null) {
      return nodesForScheduling;
    }
    initLocalNodesForSchedulingList();
    return nodesForScheduling;
  }

  /**
   * 从Protobuf对象反序列化初始化可调度节点列表本地缓存
   */
  private synchronized void initLocalNodesForSchedulingList() {
    YarnServerCommonServiceProtos.
        RegisterDistributedSchedulingAMResponseProtoOrBuilder p =
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
   * 将本地RemoteNode列表转换为Protobuf RemoteNodeProto可迭代对象，用于序列化
   * @param nodeList 本地RemoteNode节点列表
   * @return 可迭代的Protobuf节点对象
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

  @Override
  public String toString() {
    return TextFormat.shortDebugString(getProto());
  }
}