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

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.LogAggregationStatus;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoUtils;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.LogAggregationStatusProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.LogAggregationReportProto;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.LogAggregationReportProtoOrBuilder;
import org.apache.hadoop.yarn.server.api.protocolrecords.LogAggregationReport;

import org.apache.hadoop.thirdparty.protobuf.TextFormat;

/**
 * 日志聚合报告的Protobuf实现，用于NodeManager向ResourceManager上报应用日志聚合状态
 */
@Private
@Unstable
public class LogAggregationReportPBImpl extends LogAggregationReport {

  // Protobuf原型对象，只读模式使用
  LogAggregationReportProto proto = LogAggregationReportProto
    .getDefaultInstance();
  // Protobuf构建器，可写模式使用
  LogAggregationReportProto.Builder builder = null;
  // 标识当前是否通过Proto实例构造
  boolean viaProto = false;

  // 缓存的应用ID对象，避免重复转换
  private ApplicationId applicationId;

  /**
   * 空构造函数，初始化构建器
   */
  public LogAggregationReportPBImpl() {
    builder = LogAggregationReportProto.newBuilder();
  }

  /**
   * 通过已有Proto实例构造
   * @param proto 已有的LogAggregationReportProto实例
   */
  public LogAggregationReportPBImpl(LogAggregationReportProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  /**
   * 获取当前对象对应的Protobuf原型，合并本地缓存到Proto
   * @return 合并后的LogAggregationReportProto实例
   */
  public LogAggregationReportProto getProto() {
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

  // 将本地缓存的字段合并到Protobuf构建器
  private void mergeLocalToBuilder() {
    if (this.applicationId != null
        && !((ApplicationIdPBImpl) this.applicationId).getProto().equals(
          builder.getApplicationId())) {
      builder.setApplicationId(convertToProtoFormat(this.applicationId));
    }
  }

  // 将本地缓存合并到最终Proto实例
  private void mergeLocalToProto() {
    if (viaProto)
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  // 如果当前基于Proto实例，初始化构建器用于修改
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = LogAggregationReportProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public ApplicationId getApplicationId() {
    if (this.applicationId != null) {
      return this.applicationId;
    }

    LogAggregationReportProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasApplicationId()) {
      return null;
    }
    // 从Proto转换并缓存应用ID
    this.applicationId = convertFromProtoFormat(p.getApplicationId());
    return this.applicationId;
  }

  @Override
  public void setApplicationId(ApplicationId appId) {
    maybeInitBuilder();
    if (appId == null)
      builder.clearApplicationId();
    this.applicationId = appId;
  }

  // 将应用ID对象转换为Protobuf格式
  private ApplicationIdProto convertToProtoFormat(ApplicationId t) {
    return ((ApplicationIdPBImpl) t).getProto();
  }

  // 将Protobuf格式转换为应用ID对象
  private ApplicationIdPBImpl convertFromProtoFormat(
      ApplicationIdProto applicationId) {
    return new ApplicationIdPBImpl(applicationId);
  }

  @Override
  public LogAggregationStatus getLogAggregationStatus() {
    LogAggregationReportProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasLogAggregationStatus()) {
      return null;
    }
    return convertFromProtoFormat(p.getLogAggregationStatus());
  }

  @Override
  public void
      setLogAggregationStatus(LogAggregationStatus logAggregationStatus) {
    maybeInitBuilder();
    if (logAggregationStatus == null) {
      builder.clearLogAggregationStatus();
      return;
    }
    builder.setLogAggregationStatus(convertToProtoFormat(logAggregationStatus));
  }

  // 将日志聚合状态从Protobuf格式转换
  private LogAggregationStatus convertFromProtoFormat(
      LogAggregationStatusProto s) {
    return ProtoUtils.convertFromProtoFormat(s);
  }

  // 将日志聚合状态转换为Protobuf格式
  private LogAggregationStatusProto
      convertToProtoFormat(LogAggregationStatus s) {
    return ProtoUtils.convertToProtoFormat(s);
  }

  @Override
  public String getDiagnosticMessage() {
    LogAggregationReportProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasDiagnostics()) {
      return null;
    }
    return p.getDiagnostics();
  }

  @Override
  public void setDiagnosticMessage(String diagnosticMessage) {
    maybeInitBuilder();
    if (diagnosticMessage == null) {
      builder.clearDiagnostics();
      return;
    }
    builder.setDiagnostics(diagnosticMessage);
  }
}