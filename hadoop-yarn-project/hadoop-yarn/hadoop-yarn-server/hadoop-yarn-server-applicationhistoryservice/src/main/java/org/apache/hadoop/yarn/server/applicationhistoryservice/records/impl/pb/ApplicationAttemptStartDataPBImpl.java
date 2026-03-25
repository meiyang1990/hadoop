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

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationAttemptIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerIdPBImpl;
import org.apache.hadoop.yarn.proto.ApplicationHistoryServerProtos.ApplicationAttemptStartDataProto;
import org.apache.hadoop.yarn.proto.ApplicationHistoryServerProtos.ApplicationAttemptStartDataProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationAttemptIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerIdProto;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ApplicationAttemptStartData;

import org.apache.hadoop.thirdparty.protobuf.TextFormat;

/**
 * ApplicationAttemptStartData 的 Protobuf 实现类，用于应用历史服务中存储应用尝试启动信息的PB序列化实现
 */
public class ApplicationAttemptStartDataPBImpl extends
    ApplicationAttemptStartData {

  // Protobuf 默认实例，当通过proto方式存储数据时使用
  ApplicationAttemptStartDataProto proto = ApplicationAttemptStartDataProto
    .getDefaultInstance();
  // Protobuf 构建器，当通过本地对象方式修改数据时使用
  ApplicationAttemptStartDataProto.Builder builder = null;
  // 标识当前数据是否已经通过proto存储
  boolean viaProto = false;

  public ApplicationAttemptStartDataPBImpl() {
    builder = ApplicationAttemptStartDataProto.newBuilder();
  }

  public ApplicationAttemptStartDataPBImpl(
      ApplicationAttemptStartDataProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  // 本地缓存的应用尝试ID
  private ApplicationAttemptId applicationAttemptId;
  // 本地缓存的ApplicationMaster容器ID
  private ContainerId masterContainerId;

  @Override
  public ApplicationAttemptId getApplicationAttemptId() {
    // 如果本地缓存已有，直接返回
    if (this.applicationAttemptId != null) {
      return this.applicationAttemptId;
    }
    // 根据当前存储方式选择proto或builder
    ApplicationAttemptStartDataProtoOrBuilder p = viaProto ? proto : builder;
    // 如果proto中没有该字段，返回null
    if (!p.hasApplicationAttemptId()) {
      return null;
    }
    // 从Protobuf格式转换为本地对象并缓存
    this.applicationAttemptId =
        convertFromProtoFormat(p.getApplicationAttemptId());
    return this.applicationAttemptId;
  }

  @Override
  public void
      setApplicationAttemptId(ApplicationAttemptId applicationAttemptId) {
    // 确保builder已初始化
    maybeInitBuilder();
    // 如果传入null，清除该字段
    if (applicationAttemptId == null) {
      builder.clearApplicationAttemptId();
    }
    // 更新本地缓存
    this.applicationAttemptId = applicationAttemptId;
  }

  @Override
  public String getHost() {
    ApplicationAttemptStartDataProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasHost()) {
      return null;
    }
    return p.getHost();
  }

  @Override
  public void setHost(String host) {
    maybeInitBuilder();
    if (host == null) {
      builder.clearHost();
      return;
    }
    builder.setHost(host);
  }

  @Override
  public int getRPCPort() {
    ApplicationAttemptStartDataProtoOrBuilder p = viaProto ? proto : builder;
    return p.getRpcPort();
  }

  @Override
  public void setRPCPort(int rpcPort) {
    maybeInitBuilder();
    builder.setRpcPort(rpcPort);
  }

  @Override
  public ContainerId getMasterContainerId() {
    // 如果本地缓存已有，直接返回
    if (this.masterContainerId != null) {
      return this.masterContainerId;
    }
    // 根据当前存储方式选择proto或builder
    ApplicationAttemptStartDataProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasApplicationAttemptId()) {
      return null;
    }
    // 从Protobuf格式转换为本地对象并缓存
    this.masterContainerId = convertFromProtoFormat(p.getMasterContainerId());
    return this.masterContainerId;
  }

  @Override
  public void setMasterContainerId(ContainerId masterContainerId) {
    // 确保builder已初始化
    maybeInitBuilder();
    // 如果传入null，清除该字段
    if (masterContainerId == null) {
      builder.clearMasterContainerId();
    }
    // 更新本地缓存
    this.masterContainerId = masterContainerId;
  }

  /**
   * 获取当前对象对应的Protobuf实例，合并本地修改到proto后返回
   * @return ApplicationAttemptStartDataProto Protobuf对象
   */
  public ApplicationAttemptStartDataProto getProto() {
    // 合并本地修改到proto
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
   * 将本地缓存的对象合并到Protobuf builder中
   */
  private void mergeLocalToBuilder() {
    // 如果应用尝试ID存在且变更，更新到builder
    if (this.applicationAttemptId != null
        && !((ApplicationAttemptIdPBImpl) this.applicationAttemptId).getProto()
          .equals(builder.getApplicationAttemptId())) {
      builder
        .setApplicationAttemptId(convertToProtoFormat(this.applicationAttemptId));
    }
    // 如果Master容器ID存在且变更，更新到builder
    if (this.masterContainerId != null
        && !((ContainerIdPBImpl) this.masterContainerId).getProto().equals(
          builder.getMasterContainerId())) {
      builder
        .setMasterContainerId(convertToProtoFormat(this.masterContainerId));
    }
  }

  /**
   * 将本地修改合并到最终Protobuf对象
   */
  private void mergeLocalToProto() {
    // 如果当前是proto模式，先初始化builder
    if (viaProto) {
      maybeInitBuilder();
    }
    // 合并本地修改到builder
    mergeLocalToBuilder();
    // 构建新的proto对象
    proto = builder.build();
    viaProto = true;
  }

  /**
   * 初始化builder，确保可以修改数据
   */
  private void maybeInitBuilder() {
    // 如果当前是proto模式或者builder未初始化，基于现有proto创建新builder
    if (viaProto || builder == null) {
      builder = ApplicationAttemptStartDataProto.newBuilder(proto);
    }
    // 标记当前数据不再是仅proto存储
    viaProto = false;
  }

  /**
   * 将Protobuf格式的ApplicationAttemptId转换为本地实现对象
   * @param applicationAttemptId Protobuf格式的应用尝试ID
   * @return 本地实现对象
   */
  private ApplicationAttemptIdPBImpl convertFromProtoFormat(
      ApplicationAttemptIdProto applicationAttemptId) {
    return new ApplicationAttemptIdPBImpl(applicationAttemptId);
  }

  /**
   * 将本地实现对象转换为Protobuf格式的ApplicationAttemptId
   * @param applicationAttemptId 本地应用尝试ID对象
   * @return Protobuf格式对象
   */
  private ApplicationAttemptIdProto convertToProtoFormat(
      ApplicationAttemptId applicationAttemptId) {
    return ((ApplicationAttemptIdPBImpl) applicationAttemptId).getProto();
  }

  /**
   * 将Protobuf格式的ContainerId转换为本地实现对象
   * @param containerId Protobuf格式容器ID
   * @return 本地实现对象
   */
  private ContainerIdPBImpl
      convertFromProtoFormat(ContainerIdProto containerId) {
    return new ContainerIdPBImpl(containerId);
  }

  /**
   * 将本地实现对象转换为Protobuf格式的ContainerId
   * @param masterContainerId 本地容器ID对象
   * @return Protobuf格式对象
   */
  private ContainerIdProto convertToProtoFormat(ContainerId masterContainerId) {
    return ((ContainerIdPBImpl) masterContainerId).getProto();
  }

}