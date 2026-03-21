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


import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoUtils;
import org.apache.hadoop.yarn.api.records.impl.pb.ResourcePBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.MasterKeyProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.NodeActionProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.RegisterNodeManagerResponseProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.RegisterNodeManagerResponseProtoOrBuilder;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerResponse;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.api.records.NodeAction;
import org.apache.hadoop.yarn.server.api.records.impl.pb.MasterKeyPBImpl;

/**
 * 基于Protobuf实现的NodeManager注册ResourceManager响应PB实现类
 * 负责Protocol Buffers格式与高层API对象之间的互转
 */
public class RegisterNodeManagerResponsePBImpl
    extends RegisterNodeManagerResponse {
  // 存储Protobuf消息对象
  private RegisterNodeManagerResponseProto proto =
      RegisterNodeManagerResponseProto.getDefaultInstance();
  // 用于构建Protobuf消息的Builder
  private RegisterNodeManagerResponseProto.Builder builder = null;
  // 标记当前是否通过现有Protobuf对象构造
  private boolean viaProto = false;
  // 缓存节点总资源对象
  private Resource resource = null;

  // 缓存容器令牌主密钥对象
  private MasterKey containerTokenMasterKey = null;
  // 缓存NodeManager令牌主密钥对象
  private MasterKey nmTokenMasterKey = null;

  // 标记是否需要重新构建Protobuf对象
  private boolean rebuild = false;

  /**
   * 默认构造方法，初始化Builder
   */
  public RegisterNodeManagerResponsePBImpl() {
    builder = RegisterNodeManagerResponseProto.newBuilder();
  }

  /**
   * 通过已有的Protobuf对象构造响应实例
   * @param proto 已构建好的RegisterNodeManagerResponseProto对象
   */
  public RegisterNodeManagerResponsePBImpl(RegisterNodeManagerResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  /**
   * 获取当前响应对应的Protobuf对象
   * @return 构建完成的RegisterNodeManagerResponseProto
   */
  public RegisterNodeManagerResponseProto getProto() {
    if (rebuild)
      mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  /**
   * 将本地缓存的对象合并到Builder中
   */
  private void mergeLocalToBuilder() {
    if (this.containerTokenMasterKey != null) {
      builder.setContainerTokenMasterKey(
          convertToProtoFormat(this.containerTokenMasterKey));
    }
    if (this.nmTokenMasterKey != null) {
      builder.setNmTokenMasterKey(
          convertToProtoFormat(this.nmTokenMasterKey));
    }
    if (this.resource != null) {
      builder.setResource(convertToProtoFormat(this.resource));
    }
  }

  /**
   * 将本地修改合并到Protobuf对象中
   */
  private void mergeLocalToProto() {
    if (viaProto) 
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    rebuild = false;
    viaProto = true;
  }

  /**
   * 延迟初始化Builder，基于现有proto创建Builder
   */
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = RegisterNodeManagerResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public Resource getResource() {
    RegisterNodeManagerResponseProtoOrBuilder p = viaProto ? proto : builder;
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
    RegisterNodeManagerResponseProtoOrBuilder p = viaProto ? proto : builder;
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
    rebuild = true;
  }

  @Override
  public MasterKey getNMTokenMasterKey() {
    RegisterNodeManagerResponseProtoOrBuilder p = viaProto ? proto : builder;
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
    rebuild = true;
  }

  @Override
  public String getDiagnosticsMessage() {
    RegisterNodeManagerResponseProtoOrBuilder p = viaProto ? proto : builder;
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
  public String getRMVersion() {
    RegisterNodeManagerResponseProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasRmVersion()) {
      return null;
    }
    return p.getRmVersion();
  }

  @Override
  public void setRMVersion(String rmVersion) {
    maybeInitBuilder();
    if (rmVersion == null) {
      builder.clearRmIdentifier();
      return;
    }
    builder.setRmVersion(rmVersion);
  }

  @Override
  public NodeAction getNodeAction() {
    RegisterNodeManagerResponseProtoOrBuilder p = viaProto ? proto : builder;
    if(!p.hasNodeAction()) {
      return null;
    }
    return convertFromProtoFormat(p.getNodeAction());
  }

  @Override
  public void setNodeAction(NodeAction nodeAction) {
    maybeInitBuilder();
    if (nodeAction == null) {
      builder.clearNodeAction();
    } else {
      builder.setNodeAction(convertToProtoFormat(nodeAction));
    }
    rebuild = true;
  }

  @Override
  public long getRMIdentifier() {
    RegisterNodeManagerResponseProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getRmIdentifier());
  }

  @Override
  public void setRMIdentifier(long rmIdentifier) {
    maybeInitBuilder();
    builder.setRmIdentifier(rmIdentifier);
  }

  /** Protobuf格式转NodeAction枚举 */
  private NodeAction convertFromProtoFormat(NodeActionProto p) {
    return  NodeAction.valueOf(p.name());
  }

  /** NodeAction枚举转Protobuf格式 */
  private NodeActionProto convertToProtoFormat(NodeAction t) {
    return NodeActionProto.valueOf(t.name());
  }

  /** Protobuf格式转MasterKey对象 */
  private MasterKeyPBImpl convertFromProtoFormat(MasterKeyProto p) {
    return new MasterKeyPBImpl(p);
  }

  /** MasterKey对象转Protobuf格式 */
  private MasterKeyProto convertToProtoFormat(MasterKey t) {
    return ((MasterKeyPBImpl)t).getProto();
  }

  /** Protobuf格式转Resource对象 */
  private ResourcePBImpl convertFromProtoFormat(ResourceProto p) {
    return new ResourcePBImpl(p);
  }

  /** Resource对象转Protobuf格式 */
  private ResourceProto convertToProtoFormat(Resource t) {
    return ProtoUtils.convertToProtoFormat(t);
  }

  @Override
  public boolean getAreNodeLabelsAcceptedByRM() {
    RegisterNodeManagerResponseProtoOrBuilder p =
        this.viaProto ? this.proto : this.builder;
    return p.getAreNodeLabelsAcceptedByRM();
  }

  @Override
  public void setAreNodeLabelsAcceptedByRM(boolean areNodeLabelsAcceptedByRM) {
    maybeInitBuilder();
    this.builder.setAreNodeLabelsAcceptedByRM(areNodeLabelsAcceptedByRM);
  }

  @Override
  public boolean getAreNodeAttributesAcceptedByRM() {
    RegisterNodeManagerResponseProtoOrBuilder p =
        this.viaProto ? this.proto : this.builder;
    return p.getAreNodeAttributesAcceptedByRM();
  }

  @Override
  public void setAreNodeAttributesAcceptedByRM(
      boolean areNodeAttributesAcceptedByRM) {
    maybeInitBuilder();
    this.builder
        .setAreNodeAttributesAcceptedByRM(areNodeAttributesAcceptedByRM);
  }
}