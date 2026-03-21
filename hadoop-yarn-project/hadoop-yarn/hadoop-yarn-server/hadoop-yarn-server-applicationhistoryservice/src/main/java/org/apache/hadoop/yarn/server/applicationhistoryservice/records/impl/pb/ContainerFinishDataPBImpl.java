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
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoUtils;
import org.apache.hadoop.yarn.proto.ApplicationHistoryServerProtos.ContainerFinishDataProto;
import org.apache.hadoop.yarn.proto.ApplicationHistoryServerProtos.ContainerFinishDataProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerStateProto;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ContainerFinishData;

import org.apache.hadoop.thirdparty.protobuf.TextFormat;

/**
 * 容器结束数据的Protobuf实现类，存储容器完成后的状态信息，用于应用历史服务
 */
public class ContainerFinishDataPBImpl extends ContainerFinishData {

  // Protobuf默认实例，用于只读场景
  ContainerFinishDataProto proto = ContainerFinishDataProto
    .getDefaultInstance();
  // Protobuf构建器，用于可修改场景
  ContainerFinishDataProto.Builder builder = null;
  // 标识当前是否直接使用proto实例，未使用builder
  boolean viaProto = false;

  // 缓存容器ID对象，避免重复转换
  private ContainerId containerId;

  /**
   * 构造空的容器结束数据对象，初始化Builder
   */
  public ContainerFinishDataPBImpl() {
    builder = ContainerFinishDataProto.newBuilder();
  }

  /**
   * 基于已有Protobuf对象构造容器结束数据
   * @param proto 已有的ContainerFinishDataProto对象
   */
  public ContainerFinishDataPBImpl(ContainerFinishDataProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  @Override
  public ContainerId getContainerId() {
    // 已有缓存直接返回
    if (this.containerId != null) {
      return this.containerId;
    }
    // 获取当前操作的proto对象或builder
    ContainerFinishDataProtoOrBuilder p = viaProto ? proto : builder;
    // 不存在容器ID字段返回null
    if (!p.hasContainerId()) {
      return null;
    }
    // 从Protobuf格式转换并缓存
    this.containerId = convertFromProtoFormat(p.getContainerId());
    return this.containerId;
  }

  @Override
  public void setContainerId(ContainerId containerId) {
    maybeInitBuilder();
    // 清空字段
    if (containerId == null) {
      builder.clearContainerId();
    }
    // 缓存对象
    this.containerId = containerId;
  }

  @Override
  public long getFinishTime() {
    ContainerFinishDataProtoOrBuilder p = viaProto ? proto : builder;
    return p.getFinishTime();
  }

  @Override
  public void setFinishTime(long finishTime) {
    maybeInitBuilder();
    builder.setFinishTime(finishTime);
  }

  @Override
  public String getDiagnosticsInfo() {
    ContainerFinishDataProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasDiagnosticsInfo()) {
      return null;
    }
    return p.getDiagnosticsInfo();
  }

  @Override
  public void setDiagnosticsInfo(String diagnosticsInfo) {
    maybeInitBuilder();
    if (diagnosticsInfo == null) {
      builder.clearDiagnosticsInfo();
      return;
    }
    builder.setDiagnosticsInfo(diagnosticsInfo);
  }

  @Override
  public int getContainerExitStatus() {
    ContainerFinishDataProtoOrBuilder p = viaProto ? proto : builder;
    return p.getContainerExitStatus();
  }

  @Override
  public ContainerState getContainerState() {
    ContainerFinishDataProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasContainerState()) {
      return null;
    }
    return convertFromProtoFormat(p.getContainerState());
  }

  @Override
  public void setContainerState(ContainerState state) {
    maybeInitBuilder();
    if (state == null) {
      builder.clearContainerState();
      return;
    }
    builder.setContainerState(convertToProtoFormat(state));
  }

  @Override
  public void setContainerExitStatus(int containerExitStatus) {
    maybeInitBuilder();
    builder.setContainerExitStatus(containerExitStatus);
  }

  /**
   * 获取当前对象对应的Protobuf对象，合并本地缓存到proto
   * @return 构建完成的ContainerFinishDataProto
   */
  public ContainerFinishDataProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  @Override
  public int hashCode() {
    return getProto().hashCode();
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
    return TextFormat.shortDebugString(getProto());
  }

  /**
   * 将本地缓存的容器ID合并到Protobuf Builder中
   */
  private void mergeLocalToBuilder() {
    if (this.containerId != null
        && !((ContainerIdPBImpl) this.containerId).getProto().equals(
          builder.getContainerId())) {
      builder.setContainerId(convertToProtoFormat(this.containerId));
    }
  }

  /**
   * 将本地缓存合并到Protobuf对象
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
   * 如果当前是只读模式，初始化Builder用于修改
   */
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = ContainerFinishDataProto.newBuilder(proto);
    }
    viaProto = false;
  }

  /**
   * 将容器ID对象转换为Protobuf格式
   * @param containerId 容器ID对象
   * @return Protobuf格式的容器ID
   */
  private ContainerIdProto convertToProtoFormat(ContainerId containerId) {
    return ((ContainerIdPBImpl) containerId).getProto();
  }

  /**
   * 从Protobuf格式转换为容器ID对象
   * @param containerId Protobuf格式的容器ID
   * @return 容器ID对象
   */
  private ContainerIdPBImpl
      convertFromProtoFormat(ContainerIdProto containerId) {
    return new ContainerIdPBImpl(containerId);
  }

  /**
   * 将容器状态枚举转换为Protobuf格式
   * @param state 容器状态枚举
   * @return Protobuf格式的容器状态
   */
  private ContainerStateProto convertToProtoFormat(ContainerState state) {
    return ProtoUtils.convertToProtoFormat(state);
  }

  /**
   * 从Protobuf格式转换为容器状态枚举
   * @param containerState Protobuf格式的容器状态
   * @return 容器状态枚举
   */
  private ContainerState convertFromProtoFormat(
      ContainerStateProto containerState) {
    return ProtoUtils.convertFromProtoFormat(containerState);
  }

}