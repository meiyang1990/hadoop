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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.security.proto.SecurityProtos.TokenProto;
import org.apache.hadoop.yarn.api.protocolrecords.SignalContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.SignalContainerRequestPBImpl;
import org.apache.hadoop.yarn.server.api.records.AppCollectorData;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoUtils;
import org.apache.hadoop.yarn.api.records.impl.pb.ResourcePBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.TokenPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.AppCollectorDataProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.ContainerQueuingLimitProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.SignalContainerRequestProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.MasterKeyProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.NodeActionProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.NodeHeartbeatResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.NodeHeartbeatResponseProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.SystemCredentialsForAppsProto;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.records.ContainerQueuingLimit;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.api.records.NodeAction;
import org.apache.hadoop.yarn.server.api.records.impl.pb.ContainerQueuingLimitPBImpl;
import org.apache.hadoop.yarn.server.api.records.impl.pb.MasterKeyPBImpl;

/**
 * NodeHeartbeatResponse 的 Protobuf 实现类，封装RM对NodeManager心跳的响应数据。
 * 负责将API层对象与Protobuf序列化格式互相转换，支持RPC传输。
 */
public class NodeHeartbeatResponsePBImpl extends NodeHeartbeatResponse {
  // Protobuf消息默认实例
  private NodeHeartbeatResponseProto proto = NodeHeartbeatResponseProto
      .getDefaultInstance();
  // Protobuf消息构建器
  private NodeHeartbeatResponseProto.Builder builder = null;
  // 标记当前是否通过Protobuf实例构造
  private boolean viaProto = false;

  // 需要清理的容器ID列表
  private List<ContainerId> containersToCleanup = null;
  // 需要从NodeManager移除的容器ID列表
  private List<ContainerId> containersToBeRemovedFromNM = null;
  // 需要清理的应用ID列表
  private List<ApplicationId> applicationsToCleanup = null;
  // 节点可用资源
  private Resource resource = null;
  // 应用收集器地址映射表
  private Map<ApplicationId, AppCollectorData> appCollectorsMap = null;

  // 容器令牌主密钥
  private MasterKey containerTokenMasterKey = null;
  // NodeManager令牌主密钥
  private MasterKey nmTokenMasterKey = null;
  // 容器排队限制配置
  private ContainerQueuingLimit containerQueuingLimit = null;
  // 需要更新资源的容器列表
  private List<Container> containersToUpdate = null;
  // NOTE: This is required for backward compatibility.
  // 需要减少资源的容器列表，用于向后兼容
  private List<Container> containersToDecrease = null;
  // 需要发送信号的容器请求列表
  private List<SignalContainerRequest> containersToSignal = null;

  /**
   * 构造空的心跳响应对象，初始化Protobuf构建器。
   */
  public NodeHeartbeatResponsePBImpl() {
    builder = NodeHeartbeatResponseProto.newBuilder();
  }

  /**
   * 从已有Protobuf消息构造心跳响应对象。
   * @param proto 已序列化的Protobuf心跳响应消息
   */
  public NodeHeartbeatResponsePBImpl(NodeHeartbeatResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  /**
   * 获取最终构建完成的Protobuf消息对象，合并本地修改到Protobuf。
   * @return 序列化完成的Protobuf心跳响应消息
   */
  public NodeHeartbeatResponseProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  /**
   * 将所有本地缓存的对象合并到Protobuf构建器中。
   */
  private void mergeLocalToBuilder() {
    if (this.containersToCleanup != null) {
      addContainersToCleanupToProto();
    }
    if (this.applicationsToCleanup != null) {
      addApplicationsToCleanupToProto();
    }
    if (this.containersToBeRemovedFromNM != null) {
      addContainersToBeRemovedFromNMToProto();
    }
    if (this.containerTokenMasterKey != null) {
      builder.setContainerTokenMasterKey(
          convertToProtoFormat(this.containerTokenMasterKey));
    }
    if (this.nmTokenMasterKey != null) {
      builder.setNmTokenMasterKey(
          convertToProtoFormat(this.nmTokenMasterKey));
    }
    if (this.containerQueuingLimit != null) {
      builder.setContainerQueuingLimit(
          convertToProtoFormat(this.containerQueuingLimit));
    }
    if (this.containersToUpdate != null) {
      addContainersToUpdateToProto();
    }
    if (this.containersToDecrease != null) {
      addContainersToDecreaseToProto();
    }
    if (this.containersToSignal != null) {
      addContainersToSignalToProto();
    }
    if (this.resource != null) {
      builder.setResource(convertToProtoFormat(this.resource));
    }
    if (this.appCollectorsMap != null) {
      addAppCollectorsMapToProto();
    }
  }

  /**
   * 将本地应用收集器映射转换为Protobuf格式写入构建器。
   */
  private void addAppCollectorsMapToProto() {
    maybeInitBuilder();
    builder.clearAppCollectors();
    for (Map.Entry<ApplicationId, AppCollectorData> entry
        : appCollectorsMap.entrySet()) {
      AppCollectorData data = entry.getValue();
      AppCollectorDataProto.Builder appCollectorDataBuilder =
          AppCollectorDataProto.newBuilder()
              .setAppId(ProtoUtils.convertToProtoFormat(entry.getKey()))
              .setAppCollectorAddr(data.getCollectorAddr())
              .setRmIdentifier(data.getRMIdentifier())
              .setVersion(data.getVersion());
      if (data.getCollectorToken() != null) {
        appCollectorDataBuilder.setAppCollectorToken(
            convertToProtoFormat(data.getCollectorToken()));
      }
      builder.addAppCollectors(appCollectorDataBuilder);
    }
  }

  /**
   * 合并本地修改到Protobuf消息，完成最终构建。
   */
  private void mergeLocalToProto() {
    if (viaProto) 
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  /**
   * 延迟初始化Protobuf构建器，仅在需要修改时创建。
   */
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = NodeHeartbeatResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }


  @Override
  public int getResponseId() {
    NodeHeartbeatResponseProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getResponseId());
  }

  @Override
  public void setResponseId(int responseId) {
    maybeInitBuilder();
    builder.setResponseId((responseId));
  }

  @Override
  public Resource getResource() {
    NodeHeartbeatResponseProtoOrBuilder p = viaProto ? proto : builder;
    if (this.resource != null) {
      return this.resource;
    }
    if (!p.hasResource()) {
      return null;
    }
    this.resource = convertFromProtoFormat(p.getResource());
    return this.resource;
  }

  @Override
  public void setResource(Resource resource) {
    maybeInitBuilder();
    if (resource == null) {
      builder.clearResource();
    }
    this.resource = resource;
  }

  @Override
  public MasterKey getContainerTokenMasterKey() {
    NodeHeartbeatResponseProtoOrBuilder p = viaProto ? proto : builder;
    if (this.containerTokenMasterKey != null) {
      return this.containerTokenMasterKey;
    }
    if (!p.hasContainerTokenMasterKey()) {
      return null;
    }
    this.containerTokenMasterKey =
        convertFromProtoFormat(p.getContainerTokenMasterKey());
    return this.containerTokenMasterKey;
  }

  @Override
  public void setContainerTokenMasterKey(MasterKey masterKey) {
    maybeInitBuilder();
    if (masterKey == null)
      builder.clearContainerTokenMasterKey();
    this.containerTokenMasterKey = masterKey;
  }

  @Override
  public MasterKey getNMTokenMasterKey() {
    NodeHeartbeatResponseProtoOrBuilder p = viaProto ? proto : builder;
    if (this.nmTokenMasterKey != null) {
      return this.nmTokenMasterKey;
    }
    if (!p.hasNmTokenMasterKey()) {
      return null;
    }
    this.nmTokenMasterKey =
        convertFromProtoFormat(p.getNmTokenMasterKey());
    return this.nmTokenMasterKey;
  }

  @Override
  public void setNMTokenMasterKey(MasterKey masterKey) {
    maybeInitBuilder();
    if (masterKey == null)
      builder.clearNmTokenMasterKey();
    this.nmTokenMasterKey = masterKey;
  }

  @Override
  public ContainerQueuingLimit getContainerQueuingLimit() {
    NodeHeartbeatResponseProtoOrBuilder p = viaProto ? proto : builder;
    if (this.containerQueuingLimit != null) {
      return this.containerQueuingLimit;
    }
    if (!p.hasContainerQueuingLimit()) {
      return null;
    }
    this.containerQueuingLimit =
        convertFromProtoFormat(p.getContainerQueuingLimit());
    return this.containerQueuingLimit;
  }

  @Override
  public void setContainerQueuingLimit(ContainerQueuingLimit
      containerQueuingLimit) {
    maybeInitBuilder();
    if (containerQueuingLimit == null) {
      builder.clearContainerQueuingLimit();
    }
    this.containerQueuingLimit = containerQueuingLimit;
  }

  @Override
  public NodeAction getNodeAction() {
    NodeHeartbeatResponseProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasNodeAction()) {
      return null;
    }
    return (convertFromProtoFormat(p.getNodeAction()));
  }

  @Override
  public void setNodeAction(NodeAction nodeAction) {
    maybeInitBuilder();
    if (nodeAction == null) {
      builder.clearNodeAction();
      return;
    }
    builder.setNodeAction(convertToProtoFormat(nodeAction));
  }

  @Override
  public String getDiagnosticsMessage() {
    NodeHeartbeatResponseProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasDiagnosticsMessage()) {
      return null;
    }
    return p.getDiagnosticsMessage();
  }

  @Override
  public void setDiagnosticsMessage(String diagnosticsMessage) {
    maybeInitBuilder();
    if (diagnosticsMessage == null) {
      builder.clearDiagnosticsMessage();
      return;
    }
    builder.setDiagnosticsMessage((diagnosticsMessage));
  }

  @Override
  public List<ContainerId> getContainersToCleanup() {
    initContainersToCleanup();
    return this.containersToCleanup;
  }

  @Override
  public List<ContainerId> getContainersToBeRemovedFromNM() {
    initContainersToBeRemovedFromNM();
    return this.containersToBeRemovedFromNM;
  }

  /**
   * 延迟从Protobuf初始化待清理容器列表。
   */
  private void initContainersToCleanup() {
    if (this.containersToCleanup != null) {
      return;
    }
    NodeHeartbeatResponseProtoOrBuilder p = viaProto ? proto : builder;
    List<ContainerIdProto> list = p.getContainersToCleanupList();
    this.containersToCleanup = new ArrayList<ContainerId>();

    for (ContainerIdProto c : list) {
      this.containersToCleanup.add(convertFromProtoFormat(c));
    }
  }

  /**
   * 延迟从Protobuf初始化待移除容器列表。
   */
  private void initContainersToBeRemovedFromNM() {
    if (this.containersToBeRemovedFromNM != null) {
      return;
    }
    NodeHeartbeatResponseProtoOrBuilder p = viaProto ? proto : builder;
    List<ContainerIdProto> list = p.getContainersToBeRemovedFromNmList();
    this.containersToBeRemovedFromNM = new ArrayList<ContainerId>();

    for (ContainerIdProto c : list) {
      this.containersToBeRemovedFromNM.add(convertFromProtoFormat(c));
    }
  }

  @Override
  public void addAllContainersToCleanup(
      final List<ContainerId> containersToCleanup) {
    if (containersToCleanup == null)
      return;
    initContainersToCleanup();
    this.containersToCleanup.addAll(containersToCleanup);
  }

  @Override
  public void
      addContainersToBeRemovedFromNM(final List<ContainerId> containers) {
    if (containers == null)
      return;
    initContainersToBeRemovedFromNM();
    this.containersToBeRemovedFromNM.addAll(containers);
  }

  /**
   * 将待清理容器列表转换为Protobuf格式写入构建器。
   */
  private void addContainersToCleanupToProto() {
    maybeInitBuilder();
    builder.clearContainersToCleanup();
    if (containersToCleanup == null)
      return;
    Iterable<ContainerIdProto> iterable = new Iterable<ContainerIdProto>() {

      @Override
      public Iterator<ContainerIdProto> iterator() {
        return new Iterator<ContainerIdProto>() {

          Iterator<ContainerId> iter = containersToCleanup.iterator();

          @Override
          public boolean hasNext() {
            return iter.hasNext();
          }

          @Override
          public ContainerIdProto next() {
            return convertToProtoFormat(iter.next());
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException();

          }
        };

      }
    };
    builder.addAllContainersToCleanup(iterable);
  }

  /**
   * 将待移除容器列表转换为Protobuf格式写入构建器。
   */
  private void addContainersToBeRemovedFromNMToProto() {
    maybeInitBuilder();
    builder.clearContainersToBeRemovedFromNm();
    if (containersToBeRemovedFromNM == null)
      return;
    Iterable<ContainerIdProto> iterable = new Iterable<ContainerIdProto>() {

      @Override
      public Iterator<ContainerIdProto> iterator() {
        return new Iterator<ContainerIdProto>() {

          Iterator<ContainerId> iter = containersToBeRemovedFromNM.iterator();

          @Override
          public boolean hasNext() {
            return iter.hasNext();
          }

          @Override
          public ContainerIdProto next() {
            return convertToProtoFormat(iter.next());
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException();

          }
        };

      }
    };
    builder.addAllContainersToBeRemovedFromNm(iterable);
  }

  @Override
  public List<ApplicationId> getApplicationsToCleanup