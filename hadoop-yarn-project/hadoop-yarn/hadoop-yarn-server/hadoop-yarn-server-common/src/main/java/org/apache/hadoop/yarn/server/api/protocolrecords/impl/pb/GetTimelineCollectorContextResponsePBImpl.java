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
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.GetTimelineCollectorContextResponseProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnServerCommonServiceProtos.GetTimelineCollectorContextResponseProto;
import org.apache.hadoop.yarn.server.api.protocolrecords.GetTimelineCollectorContextResponse;

/**
 * 获取时间线收集器上下文响应的Protobuf实现类，封装PB序列化/反序列化逻辑
 */
public class GetTimelineCollectorContextResponsePBImpl extends
    GetTimelineCollectorContextResponse {

  // Protobuf消息对象，当通过已有proto构建时使用
  private GetTimelineCollectorContextResponseProto proto =
      GetTimelineCollectorContextResponseProto.getDefaultInstance();
  // Protobuf构建器，当需要修改字段时使用
  private GetTimelineCollectorContextResponseProto.Builder builder = null;
  // 标记当前是否使用已构建的proto对象
  private boolean viaProto = false;

  /**
   * 空构造函数，初始化PB构建器
   */
  public GetTimelineCollectorContextResponsePBImpl() {
    builder = GetTimelineCollectorContextResponseProto.newBuilder();
  }

  /**
   * 通过已有proto对象构造响应实例
   * @param proto 已构建的proto消息对象
   */
  public GetTimelineCollectorContextResponsePBImpl(
      GetTimelineCollectorContextResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  /**
   * 获取当前响应对应的proto对象，处理本地修改合并
   * @return 构建完成的proto消息对象
   */
  public GetTimelineCollectorContextResponseProto getProto() {
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
    if (other == null) {
      return false;
    }
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
   * 将本地修改合并到proto对象中
   */
  private void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    proto = builder.build();
    viaProto = true;
  }

  /**
   * 延迟初始化PB构建器，基于现有proto对象创建
   */
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = GetTimelineCollectorContextResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public String getUserId() {
    GetTimelineCollectorContextResponseProtoOrBuilder p =
        viaProto ? proto : builder;
    if (!p.hasUserId()) {
      return null;
    }
    return p.getUserId();
  }

  @Override
  public void setUserId(String userId) {
    maybeInitBuilder();
    if (userId == null) {
      builder.clearUserId();
      return;
    }
    builder.setUserId(userId);
  }

  @Override
  public String getFlowName() {
    GetTimelineCollectorContextResponseProtoOrBuilder p =
        viaProto ? proto : builder;
    if (!p.hasFlowName()) {
      return null;
    }
    return p.getFlowName();
  }

  @Override
  public void setFlowName(String flowName) {
    maybeInitBuilder();
    if (flowName == null) {
      builder.clearFlowName();
      return;
    }
    builder.setFlowName(flowName);
  }

  @Override
  public String getFlowVersion() {
    GetTimelineCollectorContextResponseProtoOrBuilder p =
        viaProto ? proto : builder;
    if (!p.hasFlowVersion()) {
      return null;
    }
    return p.getFlowVersion();
  }

  @Override
  public void setFlowVersion(String flowVersion) {
    maybeInitBuilder();
    if (flowVersion == null) {
      builder.clearFlowVersion();
      return;
    }
    builder.setFlowVersion(flowVersion);
  }

  @Override
  public long getFlowRunId() {
    GetTimelineCollectorContextResponseProtoOrBuilder p =
        viaProto ? proto : builder;
    return p.getFlowRunId();
  }

  @Override
  public void setFlowRunId(long flowRunId) {
    maybeInitBuilder();
    builder.setFlowRunId(flowRunId);
  }
}