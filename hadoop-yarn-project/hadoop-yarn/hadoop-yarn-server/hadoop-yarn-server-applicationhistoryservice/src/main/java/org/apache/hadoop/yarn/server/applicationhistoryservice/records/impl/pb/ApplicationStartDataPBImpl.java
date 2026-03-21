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

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationIdPBImpl;
import org.apache.hadoop.yarn.proto.ApplicationHistoryServerProtos.ApplicationStartDataProto;
import org.apache.hadoop.yarn.proto.ApplicationHistoryServerProtos.ApplicationStartDataProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationIdProto;
import org.apache.hadoop.yarn.server.applicationhistoryservice.records.ApplicationStartData;

import org.apache.hadoop.thirdparty.protobuf.TextFormat;

/**
 * 应用启动数据的Protobuf实现，应用历史服务中存储应用启动信息的数据结构
 */
public class ApplicationStartDataPBImpl extends ApplicationStartData {

  // 已构建完成的Protobuf对象，只读模式使用
  ApplicationStartDataProto proto = ApplicationStartDataProto
    .getDefaultInstance();
  // Protobuf构建器，可写模式使用
  ApplicationStartDataProto.Builder builder = null;
  // 标记当前是否使用现成的proto对象
  boolean viaProto = false;

  // 缓存应用ID对象
  private ApplicationId applicationId;

  /**
   * 构造空的应用启动数据对象，初始化构建器
   */
  public ApplicationStartDataPBImpl() {
    builder = ApplicationStartDataProto.newBuilder();
  }

  /**
   * 基于已有Protobuf对象构造应用启动数据
   * @param proto 已有的ApplicationStartDataProto对象
   */
  public ApplicationStartDataPBImpl(ApplicationStartDataProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  @Override
  /**
   * 获取应用ID
   * @return 应用ID对象
   */
  public ApplicationId getApplicationId() {
    // 已缓存直接返回
    if (this.applicationId != null) {
      return this.applicationId;
    }
    // 根据当前模式选择proto或builder
    ApplicationStartDataProtoOrBuilder p = viaProto ? proto : builder;
    // 不存在应用ID返回null
    if (!p.hasApplicationId()) {
      return null;
    }
    // 从Protobuf格式转换并缓存
    this.applicationId = convertFromProtoFormat(p.getApplicationId());
    return this.applicationId;
  }

  @Override
  /**
   * 设置应用ID
   * @param applicationId 要设置的应用ID
   */
  public void setApplicationId(ApplicationId applicationId) {
    // 确保builder已初始化
    maybeInitBuilder();
    // 清空builder中的应用ID字段
    if (applicationId == null) {
      builder.clearApplicationId();
    }
    // 缓存应用ID对象
    this.applicationId = applicationId;
  }

  @Override
  /**
   * 获取应用名称
   * @return 应用名称字符串
   */
  public String getApplicationName() {
    ApplicationStartDataProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasApplicationName()) {
      return null;
    }
    return p.getApplicationName();
  }

  @Override
  /**
   * 设置应用名称
   * @param applicationName 要设置的应用名称
   */
  public void setApplicationName(String applicationName) {
    maybeInitBuilder();
    if (applicationName == null) {
      builder.clearApplicationName();
      return;
    }
    builder.setApplicationName(applicationName);
  }

  @Override
  /**
   * 获取应用类型
   * @return 应用类型字符串
   */
  public String getApplicationType() {
    ApplicationStartDataProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasApplicationType()) {
      return null;
    }
    return p.getApplicationType();
  }

  @Override
  /**
   * 设置应用类型
   * @param applicationType 要设置的应用类型
   */
  public void setApplicationType(String applicationType) {
    maybeInitBuilder();
    if (applicationType == null) {
      builder.clearApplicationType();
      return;
    }
    builder.setApplicationType(applicationType);
  }

  @Override
  /**
   * 获取提交应用的用户名
   * @return 用户名字符串
   */
  public String getUser() {
    ApplicationStartDataProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasUser()) {
      return null;
    }
    return p.getUser();
  }

  @Override
  /**
   * 设置提交应用的用户名
   * @param user 用户名字符串
   */
  public void setUser(String user) {
    maybeInitBuilder();
    if (user == null) {
      builder.clearUser();
      return;
    }
    builder.setUser(user);
  }

  @Override
  /**
   * 获取应用提交到的队列名称
   * @return 队列名称字符串
   */
  public String getQueue() {
    ApplicationStartDataProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasQueue()) {
      return null;
    }
    return p.getQueue();
  }

  @Override
  /**
   * 设置应用提交到的队列名称
   * @param queue 队列名称字符串
   */
  public void setQueue(String queue) {
    maybeInitBuilder();
    if (queue == null) {
      builder.clearQueue();
      return;
    }
    builder.setQueue(queue);
  }

  @Override
  /**
   * 获取应用提交时间
   * @return 提交时间戳(毫秒)
   */
  public long getSubmitTime() {
    ApplicationStartDataProtoOrBuilder p = viaProto ? proto : builder;
    return p.getSubmitTime();
  }

  @Override
  /**
   * 设置应用提交时间
   * @param submitTime 提交时间戳(毫秒)
   */
  public void setSubmitTime(long submitTime) {
    maybeInitBuilder();
    builder.setSubmitTime(submitTime);
  }

  @Override
  /**
   * 获取应用启动时间
   * @return 启动时间戳(毫秒)
   */
  public long getStartTime() {
    ApplicationStartDataProtoOrBuilder p = viaProto ? proto : builder;
    return p.getStartTime();
  }

  @Override
  /**
   * 设置应用启动时间
   * @param startTime 启动时间戳(毫秒)
   */
  public void setStartTime(long startTime) {
    maybeInitBuilder();
    builder.setStartTime(startTime);
  }

  /**
   * 获取当前对象对应的Protobuf对象，合并本地缓存字段后构建
   * @return 构建完成的ApplicationStartDataProto
   */
  public ApplicationStartDataProto getProto() {
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
   * 将本地缓存的应用ID合并到Protobuf构建器中
   */
  private void mergeLocalToBuilder() {
    if (this.applicationId != null
        && !((ApplicationIdPBImpl) this.applicationId).getProto().equals(
          builder.getApplicationId())) {
      builder.setApplicationId(convertToProtoFormat(this.applicationId));
    }
  }

  /**
   * 将本地缓存合并到proto对象，完成最终构建
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
   * 延迟初始化构建器：如果当前是只读模式，基于现有proto新建构建器
   */
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = ApplicationStartDataProto.newBuilder(proto);
    }
    viaProto = false;
  }

  /**
   * 将应用ID对象转换为Protobuf格式
   * @param applicationId 应用ID对象
   * @return Protobuf格式的应用ID
   */
  private ApplicationIdProto convertToProtoFormat(ApplicationId applicationId) {
    return ((ApplicationIdPBImpl) applicationId).getProto();
  }

  /**
   * 将Protobuf格式的应用ID转换为对象
   * @param applicationId Protobuf格式应用ID
   * @return 应用ID对象
   */
  private ApplicationIdPBImpl convertFromProtoFormat(
      ApplicationIdProto applicationId) {
    return new ApplicationIdPBImpl(applicationId);
  }
}