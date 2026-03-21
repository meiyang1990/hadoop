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

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.PriorityPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoUtils;
import org.apache.hadoop.yarn.api.records.impl.pb.ResourcePBImpl;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.proto.YarnProtos;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerStateProto;
import org.apache.hadoop.yarn.proto.YarnProtos.PriorityProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.NMContainerStatusProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.NMContainerStatusProtoOrBuilder;
import org.apache.hadoop.yarn.server.api.protocolrecords.NMContainerStatus;

import java.util.HashSet;
import java.util.Set;

/**
 * NMContainerStatus的Protobuf序列化实现，用于NodeManager向ResourceManager汇报容器状态
 * 基于PB实现协议缓存，支持本地对象和PB proto两种存储格式的懒加载转换
 */
public class NMContainerStatusPBImpl extends NMContainerStatus {

  // PB proto对象，当viaProto为true时使用原始proto存储
  NMContainerStatusProto proto = NMContainerStatusProto
    .getDefaultInstance();
  // PB构建器，当通过本地对象构建时使用builder缓存修改
  NMContainerStatusProto.Builder builder = null;
  // 当前是否通过原始proto对象构造，标志存储格式
  boolean viaProto = false;

  // 本地缓存的容器ID对象，懒加载从proto转换
  private ContainerId containerId = null;
  // 本地缓存的容器分配资源对象，懒加载从proto转换
  private Resource resource = null;
  // 本地缓存的容器优先级对象，懒加载从proto转换
  private Priority priority = null;
  // 本地缓存的容器分配标签集合，懒加载从proto转换
  private Set<String> allocationTags = null;

  /**
   * 空构造函数，用于新建对象
   */
  public NMContainerStatusPBImpl() {
    builder = NMContainerStatusProto.newBuilder();
  }

  /**
   * 基于已有proto对象构造，使用proto存储格式
   * @param proto 已有的NMContainerStatusProto对象
   */
  public NMContainerStatusPBImpl(NMContainerStatusProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  /**
   * 获取当前对象对应的proto对象，自动合并本地修改生成最终proto
   * @return 合并修改后的NMContainerStatusProto
   */
  public NMContainerStatusProto getProto() {

    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  @Override
  public int hashCode() {
    return this.getProto().hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other == null)
      return false;
    if (other.getClass().isAssignableFrom(this.getClass())) {
      return this.getProto().equals(this.getClass().cast(other).getProto());
    }
    return false;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("[").append(getContainerId()).append(", ")
        .append("CreateTime: ").append(getCreationTime()).append(", ")
        .append("Version: ").append(getVersion()).append(", ")
        .append("State: ").append(getContainerState()).append(", ")
        .append("Capability: ").append(getAllocatedResource()).append(", ")
        .append("Diagnostics: ").append(getDiagnostics()).append(", ")
        .append("ExitStatus: ").append(getContainerExitStatus()).append(", ")
        .append("NodeLabelExpression: ").append(getNodeLabelExpression())
        .append(", ")
        .append("Priority: ").append(getPriority()).append(", ")
        .append("AllocationRequestId: ").append(getAllocationRequestId())
        .append(", ")
        .append("AllocationTags: ").append(getAllocationTags()).append(", ")
        .append("]");
    return sb.toString();
  }

  @Override
  public Resource getAllocatedResource() {
    if (this.resource != null) {
      return this.resource;
    }
    NMContainerStatusProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasResource()) {
      return null;
    }
    this.resource = convertFromProtoFormat(p.getResource());
    return this.resource;
  }

  @Override
  public ContainerId getContainerId() {
    if (this.containerId != null) {
      return this.containerId;
    }
    NMContainerStatusProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasContainerId()) {
      return null;
    }
    this.containerId = convertFromProtoFormat(p.getContainerId());
    return this.containerId;
  }

  @Override
  public String getDiagnostics() {
    NMContainerStatusProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasDiagnostics()) {
      return null;
    }
    return (p.getDiagnostics());
  }

  @Override
  public ContainerState getContainerState() {
    NMContainerStatusProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasContainerState()) {
      return null;
    }
    return convertFromProtoFormat(p.getContainerState());
  }

  @Override
  public void setAllocatedResource(Resource resource) {
    maybeInitBuilder();
    if (resource == null)
      builder.clearResource();
    this.resource = resource;
  }

  public void setContainerId(ContainerId containerId) {
    maybeInitBuilder();
    if (containerId == null)
      builder.clearContainerId();
    this.containerId = containerId;
  }

  @Override
  public void setDiagnostics(String diagnosticsInfo) {
    maybeInitBuilder();
    if (diagnosticsInfo == null) {
      builder.clearDiagnostics();
      return;
    }
    builder.setDiagnostics(diagnosticsInfo);
  }

  @Override
  public void setContainerState(ContainerState containerState) {
    maybeInitBuilder();
    if (containerState == null) {
      builder.clearContainerState();
      return;
    }
    builder.setContainerState(convertToProtoFormat(containerState));
  }

  @Override
  public int getContainerExitStatus() {
    NMContainerStatusProtoOrBuilder p = viaProto ? proto : builder;
    return p.getContainerExitStatus();
  }

  @Override
  public void setContainerExitStatus(int containerExitStatus) {
    maybeInitBuilder();
    builder.setContainerExitStatus(containerExitStatus);
  }

  @Override
  public int getVersion() {
    NMContainerStatusProtoOrBuilder p = viaProto ? proto : builder;
    return p.getVersion();
  }

  @Override
  public void setVersion(int version) {
    maybeInitBuilder();
    builder.setVersion(version);
  }

  @Override
  public Priority getPriority() {
    NMContainerStatusProtoOrBuilder p = viaProto ? proto : builder;
    if (this.priority != null) {
      return this.priority;
    }
    if (!p.hasPriority()) {
      return null;
    }
    this.priority = convertFromProtoFormat(p.getPriority());
    return this.priority;
  }

  @Override
  public void setPriority(Priority priority) {
    maybeInitBuilder();
    if (priority == null) 
      builder.clearPriority();
    this.priority = priority;
  }

  @Override
  public long getCreationTime() {
    NMContainerStatusProtoOrBuilder p = viaProto ? proto : builder;
    return p.getCreationTime();
  }

  @Override
  public void setCreationTime(long creationTime) {
    maybeInitBuilder();
    builder.setCreationTime(creationTime);
  }
  
  @Override
  public String getNodeLabelExpression() {
    NMContainerStatusProtoOrBuilder p = viaProto ? proto : builder;
    if (p.hasNodeLabelExpression()) {
      return p.getNodeLabelExpression();
    }
    return CommonNodeLabelsManager.NO_LABEL;
  }

  @Override
  public void setNodeLabelExpression(String nodeLabelExpression) {
    maybeInitBuilder();
    if (nodeLabelExpression == null) {
      builder.clearNodeLabelExpression();
      return;
    }
    builder.setNodeLabelExpression(nodeLabelExpression);
  }

  @Override
  public synchronized ExecutionType getExecutionType() {
    NMContainerStatusProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasExecutionType()) {
      return ExecutionType.GUARANTEED;
    }
    return convertFromProtoFormat(p.getExecutionType());
  }

  @Override
  public synchronized void setExecutionType(ExecutionType executionType) {
    maybeInitBuilder();
    if (executionType == null) {
      builder.clearExecutionType();
      return;
    }
    builder.setExecutionType(convertToProtoFormat(executionType));
  }

  @Override
  public long getAllocationRequestId() {
    NMContainerStatusProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getAllocationRequestId());
  }

  @Override
  public void setAllocationRequestId(long allocationRequestId) {
    maybeInitBuilder();
    builder.setAllocationRequestId(allocationRequestId);
  }

  /**
   * 懒加载从proto转换分配标签集合到本地缓存
   */
  private void initAllocationTags() {
    if (this.allocationTags != null) {
      return;
    }
    NMContainerStatusProtoOrBuilder p = viaProto ? proto : builder;
    this.allocationTags = new HashSet<>();
    this.allocationTags.addAll(p.getAllocationTagsList());
  }

  @Override
  public Set<String> getAllocationTags() {
    initAllocationTags();
    return this.allocationTags;
  }

  @Override
  public void setAllocationTags(Set<String> allocationTags) {
    maybeInitBuilder();
    builder.clearAllocationTags();
    this.allocationTags = allocationTags;
  }

  /**
   * 将本地缓存的对象修改合并到PB构建器中
   */
  private void mergeLocalToBuilder() {
    if (this.containerId != null
        && !((ContainerIdPBImpl) containerId).getProto().equals(
          builder.getContainerId())) {
      builder.setContainerId(convertToProtoFormat(this.containerId));
    }

    if (this.resource != null) {
      builder.setResource(convertToProtoFormat(this.resource));
    }

    if (this.priority != null) {
      builder.setPriority(convertToProtoFormat(this.priority));
    }
    if (this.allocationTags != null) {
      builder.clearAllocationTags();
      builder.addAllAllocationTags(this.allocationTags);
    }
  }

  /**
   * 将本地修改合并到最终proto对象
   */
  private void mergeLocalToProto() {
    if (viaProto)
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  /**
   * 初始化构建器，当从proto修改时，基于现有proto创建构建器
   */
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = NMContainerStatusProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private ContainerIdPBImpl convertFromProtoFormat(ContainerIdProto p) {
    return new ContainerIdPBImpl(p);
  }

  private ContainerIdProto convertToProtoFormat(ContainerId t) {
    return ((ContainerIdPBImpl) t).getProto();
  }

  private ResourcePBImpl convertFromProtoFormat(ResourceProto p) {
    return new ResourcePBImpl(p);
  }

  private ResourceProto convertToProtoFormat(Resource t) {
    return ProtoUtils.convertToProtoFormat(t);
  }

  private ContainerStateProto
      convertToProtoFormat(ContainerState containerState) {
    return ProtoUtils.convertToProtoFormat(containerState);
  }

  private ContainerState convertFromProtoFormat(
      ContainerStateProto containerState) {
    return ProtoUtils.convertFromProtoFormat(containerState);
  }

  private PriorityPBImpl convertFromProtoFormat(PriorityProto p) {
    return new PriorityPBImpl(p);
  }

  private PriorityProto convertToProtoFormat(Priority t) {
    return ((PriorityPBImpl)t).getProto();
  }

  private ExecutionType convertFromProtoFormat(
      YarnProtos.ExecutionTypeProto e) {
    return ProtoUtils.convertFromProtoFormat(e);
  }

  private YarnProtos.ExecutionTypeProto convertToProtoFormat(ExecutionType e) {
    return ProtoUtils.convertToProtoFormat(e);
  }
}