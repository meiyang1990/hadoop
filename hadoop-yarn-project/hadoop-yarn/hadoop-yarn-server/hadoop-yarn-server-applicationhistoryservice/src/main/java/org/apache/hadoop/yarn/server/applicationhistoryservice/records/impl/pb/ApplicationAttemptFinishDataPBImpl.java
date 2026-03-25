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
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationAttemptState;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationAttemptIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoUtils;
import org.apache.hadoop.yarn.proto.ApplicationHistoryServerProtos.ApplicationAttemptFinishDataProto;
import org.apache.hadoop.yarn.proto.ApplicationHistoryServerProtos.ApplicationAttemptFinishDataProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationAttemptIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.FinalApplicationStatusProto;
import org.apache.hadoop.yarn.proto.YarnProtos.YarnApplicationAttemptStateProto;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ApplicationAttemptFinishData;

import org.apache.hadoop.thirdparty.protobuf.TextFormat;

/**
 * 应用尝试完成数据的Protobuf实现，存储应用尝试结束相关信息，用于应用历史服务
 */
public class ApplicationAttemptFinishDataPBImpl extends
    ApplicationAttemptFinishData {

  // Protobuf默认实例，用于只读场景
  ApplicationAttemptFinishDataProto proto = ApplicationAttemptFinishDataProto
    .getDefaultInstance();
  // Protobuf构建器，用于可修改场景
  ApplicationAttemptFinishDataProto.Builder builder = null;
  // 标记当前是否通过已有proto实例构造
  boolean viaProto = false;

  public ApplicationAttemptFinishDataPBImpl() {
    builder = ApplicationAttemptFinishDataProto.newBuilder();
  }

  public ApplicationAttemptFinishDataPBImpl(
      ApplicationAttemptFinishDataProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  // 缓存应用尝试ID对象
  private ApplicationAttemptId applicationAttemptId;

  @Override
  public ApplicationAttemptId getApplicationAttemptId() {
    // 已缓存直接返回
    if (this.applicationAttemptId != null) {
      return this.applicationAttemptId;
    }
    // 根据场景选择proto或builder
    ApplicationAttemptFinishDataProtoOrBuilder p = viaProto ? proto : builder;
    // 无设置返回null
    if (!p.hasApplicationAttemptId()) {
      return null;
    }
    // 从proto转换并缓存
    this.applicationAttemptId =
        convertFromProtoFormat(p.getApplicationAttemptId());
    return this.applicationAttemptId;
  }

  @Override
  public void
      setApplicationAttemptId(ApplicationAttemptId applicationAttemptId) {
    // 确保builder已初始化
    maybeInitBuilder();
    // 清空字段
    if (applicationAttemptId == null) {
      builder.clearApplicationAttemptId();
    }
    // 缓存对象
    this.applicationAttemptId = applicationAttemptId;
  }

  @Override
  public String getTrackingURL() {
    ApplicationAttemptFinishDataProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasTrackingUrl()) {
      return null;
    }
    return p.getTrackingUrl();
  }

  @Override
  public void setTrackingURL(String trackingURL) {
    maybeInitBuilder();
    if (trackingURL == null) {
      builder.clearTrackingUrl();
      return;
    }
    builder.setTrackingUrl(trackingURL);
  }

  @Override
  public String getDiagnosticsInfo() {
    ApplicationAttemptFinishDataProtoOrBuilder p = viaProto ? proto : builder;
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
  public FinalApplicationStatus getFinalApplicationStatus() {
    ApplicationAttemptFinishDataProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasFinalApplicationStatus()) {
      return null;
    }
    return convertFromProtoFormat(p.getFinalApplicationStatus());
  }

  @Override
  public void setFinalApplicationStatus(
      FinalApplicationStatus finalApplicationStatus) {
    maybeInitBuilder();
    if (finalApplicationStatus == null) {
      builder.clearFinalApplicationStatus();
      return;
    }
    builder
      .setFinalApplicationStatus(convertToProtoFormat(finalApplicationStatus));
  }

  @Override
  public YarnApplicationAttemptState getYarnApplicationAttemptState() {
    ApplicationAttemptFinishDataProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasYarnApplicationAttemptState()) {
      return null;
    }
    return convertFromProtoFormat(p.getYarnApplicationAttemptState());
  }

  @Override
  public void setYarnApplicationAttemptState(YarnApplicationAttemptState state) {
    maybeInitBuilder();
    if (state == null) {
      builder.clearYarnApplicationAttemptState();
      return;
    }
    builder.setYarnApplicationAttemptState(convertToProtoFormat(state));
  }

  /**
   * 获取当前对象对应的Protobuf实例，合并本地修改到proto
   * @return Protobuf实例
   */
  public ApplicationAttemptFinishDataProto getProto() {
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

  // 将本地缓存的应用尝试ID合并到builder
  private void mergeLocalToBuilder() {
    if (this.applicationAttemptId != null
        && !((ApplicationAttemptIdPBImpl) this.applicationAttemptId).getProto()
          .equals(builder.getApplicationAttemptId())) {
      builder
        .setApplicationAttemptId(convertToProtoFormat(this.applicationAttemptId));
    }
  }

  // 将本地修改合并到proto实例
  private void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  // 初始化builder，确保可修改
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = ApplicationAttemptFinishDataProto.newBuilder(proto);
    }
    viaProto = false;
  }

  // 应用尝试ID从Protobuf格式转换
  private ApplicationAttemptIdPBImpl convertFromProtoFormat(
      ApplicationAttemptIdProto applicationAttemptId) {
    return new ApplicationAttemptIdPBImpl(applicationAttemptId);
  }

  // 应用尝试ID转换为Protobuf格式
  private ApplicationAttemptIdProto convertToProtoFormat(
      ApplicationAttemptId applicationAttemptId) {
    return ((ApplicationAttemptIdPBImpl) applicationAttemptId).getProto();
  }

  // 最终应用状态从Protobuf格式转换
  private FinalApplicationStatus convertFromProtoFormat(
      FinalApplicationStatusProto finalApplicationStatus) {
    return ProtoUtils.convertFromProtoFormat(finalApplicationStatus);
  }

  // 最终应用状态转换为Protobuf格式
  private FinalApplicationStatusProto convertToProtoFormat(
      FinalApplicationStatus finalApplicationStatus) {
    return ProtoUtils.convertToProtoFormat(finalApplicationStatus);
  }

  // YARN应用尝试状态转换为Protobuf格式
  private YarnApplicationAttemptStateProto convertToProtoFormat(
      YarnApplicationAttemptState state) {
    return ProtoUtils.convertToProtoFormat(state);
  }

  // YARN应用尝试状态从Protobuf格式转换
  private YarnApplicationAttemptState convertFromProtoFormat(
      YarnApplicationAttemptStateProto yarnApplicationAttemptState) {
    return ProtoUtils.convertFromProtoFormat(yarnApplicationAttemptState);
  }

}