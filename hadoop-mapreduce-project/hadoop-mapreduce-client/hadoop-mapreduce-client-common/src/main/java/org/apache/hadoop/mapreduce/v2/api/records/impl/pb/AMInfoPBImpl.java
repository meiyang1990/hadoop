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

package org.apache.hadoop.mapreduce.v2.api.records.impl.pb;

import org.apache.hadoop.mapreduce.v2.api.records.AMInfo;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.AMInfoProto;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.AMInfoProtoOrBuilder;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationAttemptIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoBase;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationAttemptIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerIdProto;

/**
 * AMInfo的Protobuf实现，基于ProtoBase实现AM信息的序列化与反序列化，
 * 存储MapReduce ApplicationMaster的运行基本信息，用于RPC通信和状态持久化。
 */
public class AMInfoPBImpl extends ProtoBase<AMInfoProto> implements AMInfo {

  AMInfoProto proto = AMInfoProto.getDefaultInstance();
  AMInfoProto.Builder builder = null;
  boolean viaProto = false;
  
  private ApplicationAttemptId appAttemptId;
  private ContainerId containerId;
  
  /**
   * 无参构造函数，初始化Protobuf构建器。
   */
  public AMInfoPBImpl() {
    builder = AMInfoProto.newBuilder();
  }

  /**
   * 通过已有的AMInfoProto构造AMInfoPBImpl对象。
   * @param proto 已构建完成的AMInfoProto实例
   */
  public AMInfoPBImpl(AMInfoProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  @Override
  public synchronized AMInfoProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  /**
   * 将本地缓存的对象字段合并到Protobuf Builder中。
   */
  private synchronized void mergeLocalToBuilder() {
    if (this.appAttemptId != null
        && !((ApplicationAttemptIdPBImpl) this.appAttemptId).getProto().equals(
            builder.getApplicationAttemptId())) {
      builder.setApplicationAttemptId(convertToProtoFormat(this.appAttemptId));
    }
    if (this.getContainerId() != null
        && !((ContainerIdPBImpl) this.containerId).getProto().equals(
            builder.getContainerId())) {
      builder.setContainerId(convertToProtoFormat(this.containerId));
    }
  }

  /**
   * 将本地修改合并到最终的Protobuf对象中。
   */
  private synchronized void mergeLocalToProto() {
    if (viaProto)
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  /**
   * 如果当前是Proto模式，初始化Builder用于修改。
   */
  private synchronized void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = AMInfoProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public synchronized ApplicationAttemptId getAppAttemptId() {
    AMInfoProtoOrBuilder p = viaProto ? proto : builder;
    if (appAttemptId != null) {
      return appAttemptId;
    } // Else via proto
    if (!p.hasApplicationAttemptId()) {
      return null;
    }
    appAttemptId = convertFromProtoFormat(p.getApplicationAttemptId());
    return appAttemptId;
  }

  @Override
  public synchronized void setAppAttemptId(ApplicationAttemptId appAttemptId) {
    maybeInitBuilder();
    if (appAttemptId == null) {
      builder.clearApplicationAttemptId();
    }
    this.appAttemptId = appAttemptId;
  }

  @Override
  public synchronized long getStartTime() {
    AMInfoProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getStartTime());
  }
  
  @Override
  public synchronized void setStartTime(long startTime) {
    maybeInitBuilder();
    builder.setStartTime(startTime);
  }

  @Override
  public synchronized ContainerId getContainerId() {
    AMInfoProtoOrBuilder p = viaProto ? proto : builder;
    if (containerId != null) {
      return containerId;
    } // Else via proto
    if (!p.hasContainerId()) {
      return null;
    }
    containerId = convertFromProtoFormat(p.getContainerId());
    return containerId;
  }
  
  @Override
  public synchronized void setContainerId(ContainerId containerId) {
    maybeInitBuilder();
    if (containerId == null) {
      builder.clearContainerId();
    }
    this.containerId = containerId;
  }

  @Override
  public synchronized String getNodeManagerHost() {
    AMInfoProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasNodeManagerHost()) {
      return null;
    }
    return p.getNodeManagerHost();
  }

  @Override
  public synchronized void setNodeManagerHost(String nmHost) {
    maybeInitBuilder();
    if (nmHost == null) {
      builder.clearNodeManagerHost();
      return;
    }
    builder.setNodeManagerHost(nmHost);
  }

  @Override
  public synchronized int getNodeManagerPort() {
    AMInfoProtoOrBuilder p = viaProto ? proto : builder;
    return (p.getNodeManagerPort());
  }
  
  @Override
  public synchronized void setNodeManagerPort(int nmPort) {
    maybeInitBuilder();
    builder.setNodeManagerPort(nmPort);
  }

  @Override
  public synchronized int getNodeManagerHttpPort() {
    AMInfoProtoOrBuilder p = viaProto ? proto : builder;
    return p.getNodeManagerHttpPort();
  }

  @Override
  public synchronized void setNodeManagerHttpPort(int httpPort) {
    maybeInitBuilder();
    builder.setNodeManagerHttpPort(httpPort);
  }

  /**
   * 将Protobuf格式的ApplicationAttemptId转换为API层对象。
   * @param p Protobuf格式的ApplicationAttemptId
   * @return API层ApplicationAttemptId对象
   */
  private ApplicationAttemptIdPBImpl convertFromProtoFormat(
      ApplicationAttemptIdProto p) {
    return new ApplicationAttemptIdPBImpl(p);
  }

  /**
   * 将Protobuf格式的ContainerId转换为API层对象。
   * @param p Protobuf格式的ContainerId
   * @return API层ContainerId对象
   */
  private ContainerIdPBImpl convertFromProtoFormat(ContainerIdProto p) {
    return new ContainerIdPBImpl(p);
  }

  /**
   * 将API层ApplicationAttemptId转换为Protobuf格式。
   * @param t API层ApplicationAttemptId对象
   * @return Protobuf格式的ApplicationAttemptId
   */
  private
      ApplicationAttemptIdProto convertToProtoFormat(ApplicationAttemptId t) {
    return ((ApplicationAttemptIdPBImpl) t).getProto();
  }

  /**
   * 将API层ContainerId转换为Protobuf格式。
   * @param t API层ContainerId对象
   * @return Protobuf格式的ContainerId
   */
  private ContainerIdProto convertToProtoFormat(ContainerId t) {
    return ((ContainerIdPBImpl) t).getProto();
  }
}