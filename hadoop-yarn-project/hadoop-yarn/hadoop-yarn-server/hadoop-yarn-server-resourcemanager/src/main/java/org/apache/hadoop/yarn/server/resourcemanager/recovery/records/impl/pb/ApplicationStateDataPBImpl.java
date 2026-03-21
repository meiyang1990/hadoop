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

package org.apache.hadoop.yarn.server.resourcemanager.recovery.records.impl.pb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.ipc.CallerContext;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ApplicationTimeoutType;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationSubmissionContextPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoUtils;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationTimeoutMapProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerRecoveryProtos.ApplicationStateDataProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerRecoveryProtos.ApplicationStateDataProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerRecoveryProtos.RMAppStateProto;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.ApplicationStateData;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;

import org.apache.hadoop.thirdparty.protobuf.ByteString;
import org.apache.hadoop.thirdparty.protobuf.TextFormat;

/**
 * YARN RM应用状态数据的Protobuf实现，用于RM恢复时持久化存储应用状态信息。
 * 基于Protobuf序列化格式实现ApplicationStateData接口，提供状态数据的存取和转换能力。
 */
public class ApplicationStateDataPBImpl extends ApplicationStateData {
  // 存储应用状态的Protobuf对象
  ApplicationStateDataProto proto = 
            ApplicationStateDataProto.getDefaultInstance();
  // Protobuf构建器，用于构建新的proto对象
  ApplicationStateDataProto.Builder builder = null;
  // 标记当前是否通过proto方式存储数据（false表示正在通过builder构建）
  boolean viaProto = false;
  
  // 缓存应用提交上下文对象
  private ApplicationSubmissionContext applicationSubmissionContext = null;
  // 缓存应用超时配置映射表
  private Map<ApplicationTimeoutType, Long> applicationTimeouts = null;
  
  /**
   * 构造函数，初始化空的Protobuf构建器。
   */
  public ApplicationStateDataPBImpl() {
    builder = ApplicationStateDataProto.newBuilder();
  }

  /**
   * 构造函数，基于已有Protobuf对象封装。
   * @param proto 已有的应用状态Protobuf对象
   */
  public ApplicationStateDataPBImpl(
      ApplicationStateDataProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  @Override
  public ApplicationStateDataProto getProto() {
    // 合并本地缓存数据到Protobuf
    mergeLocalToProto();
    // 根据存储方式获取最终的proto对象
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  /**
   * 将本地缓存的Java对象合并到Protobuf构建器中。
   */
  private void mergeLocalToBuilder() {
    if (this.applicationSubmissionContext != null) {
      builder.setApplicationSubmissionContext(
          ((ApplicationSubmissionContextPBImpl)applicationSubmissionContext)
          .getProto());
    }

    if (this.applicationTimeouts != null) {
      // 将超时配置映射添加到Protobuf构建器
      addApplicationTimeouts();
    }
  }

  /**
   * 将本地缓存数据合并到最终Protobuf对象。
   */
  private void mergeLocalToProto() {
    // 如果当前是proto模式，先初始化构建器
    if (viaProto) 
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  /**
   * 如果需要，初始化Protobuf构建器。
   */
  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = ApplicationStateDataProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public long getSubmitTime() {
    ApplicationStateDataProtoOrBuilder p = viaProto ? proto : builder;
    // 没有设置提交时间则返回-1
    if (!p.hasSubmitTime()) {
      return -1;
    }
    return (p.getSubmitTime());
  }

  @Override
  public void setSubmitTime(long submitTime) {
    maybeInitBuilder();
    builder.setSubmitTime(submitTime);
  }

  @Override
  public long getStartTime() {
    ApplicationStateDataProtoOrBuilder p = viaProto ? proto : builder;
    return p.getStartTime();
  }

  @Override
  public void setStartTime(long startTime) {
    maybeInitBuilder();
    builder.setStartTime(startTime);
  }


  @Override
  public long getLaunchTime() {
    ApplicationStateDataProtoOrBuilder p = viaProto ? proto : builder;
    return p.getLaunchTime();
  }

  @Override
  public void setLaunchTime(long launchTime) {
    maybeInitBuilder();
    builder.setLaunchTime(launchTime);
  }

  @Override
  public String getUser() {
    ApplicationStateDataProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasUser()) {
      return null;
    }
    return (p.getUser());

  }
  
  @Override
  public void setUser(String user) {
    maybeInitBuilder();
    builder.setUser(user);
  }

  @Override
  public String getRealUser() {
    ApplicationStateDataProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasRealUser()) {
      return null;
    }
    return (p.getRealUser());
  }

  @Override
  public void setRealUser(String realUser) {
    maybeInitBuilder();
    builder.setRealUser(realUser);
  }

  @Override
  public ApplicationSubmissionContext getApplicationSubmissionContext() {
    ApplicationStateDataProtoOrBuilder p = viaProto ? proto : builder;
    // 直接返回缓存对象（如果已加载）
    if(applicationSubmissionContext != null) {
      return applicationSubmissionContext;
    }
    if (!p.hasApplicationSubmissionContext()) {
      return null;
    }
    // 从Protobuf转换为Java对象并缓存
    applicationSubmissionContext = 
        new ApplicationSubmissionContextPBImpl(
            p.getApplicationSubmissionContext());
    return applicationSubmissionContext;
  }

  @Override
  public void setApplicationSubmissionContext(
      ApplicationSubmissionContext context) {
    maybeInitBuilder();
    if (context == null) {
      builder.clearApplicationSubmissionContext();
    }
    this.applicationSubmissionContext = context;
  }

  @Override
  public RMAppState getState() {
    ApplicationStateDataProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasApplicationState()) {
      return null;
    }
    // 将Protobuf枚举转换为RM内部状态枚举
    return convertFromProtoFormat(p.getApplicationState());
  }

  @Override
  public void setState(RMAppState finalState) {
    maybeInitBuilder();
    if (finalState == null) {
      builder.clearApplicationState();
      return;
    }
    // 将RM内部状态枚举转换为Protobuf枚举
    builder.setApplicationState(convertToProtoFormat(finalState));
  }

  @Override
  public String getDiagnostics() {
    ApplicationStateDataProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasDiagnostics()) {
      return null;
    }
    return p.getDiagnostics();
  }

  @Override
  public void setDiagnostics(String diagnostics) {
    maybeInitBuilder();
    if (diagnostics == null) {
      builder.clearDiagnostics();
      return;
    }
    builder.setDiagnostics(diagnostics);
  }

  @Override
  public long getFinishTime() {
    ApplicationStateDataProtoOrBuilder p = viaProto ? proto : builder;
    return p.getFinishTime();
  }

  @Override
  public void setFinishTime(long finishTime) {
    maybeInitBuilder();
    builder.setFinishTime(finishTime);
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
  public CallerContext getCallerContext() {
    ApplicationStateDataProtoOrBuilder p = viaProto ? proto : builder;
    RpcHeaderProtos.RPCCallerContextProto pbContext = p.getCallerContext();
    if (pbContext != null) {
      // 从Protobuf还原CallerContext对象
      CallerContext context = new CallerContext.Builder(pbContext.getContext())
          .setSignature(pbContext.getSignature().toByteArray()).build();
      return context;
    }

    return null;
  }

  @Override
  public void setCallerContext(CallerContext callerContext) {
    if (callerContext != null) {
      maybeInitBuilder();

      // 构建CallerContext的Protobuf对象
      RpcHeaderProtos.RPCCallerContextProto.Builder b = RpcHeaderProtos.RPCCallerContextProto
          .newBuilder();
      if (callerContext.isContextValid()) {
        b.setContext(callerContext.getContext());
      }
      if (callerContext.getSignature() != null) {
        b.setSignature(ByteString.copyFrom(callerContext.getSignature()));
      }

      // 只要有一个字段有效就设置到builder中
      if(callerContext.isContextValid()
          || callerContext.getSignature() != null) {
        builder.setCallerContext(b);
      }
    }
  }

  @Override
  public String toString() {
    return TextFormat.shortDebugString(getProto());
  }

  private static String RM_APP_PREFIX = "RMAPP_";
  /**
   * 将RMAppState枚举转换为Protobuf枚举。
   * @param e RM内部应用状态枚举
   * @return Protobuf格式应用状态枚举
   */
  public static RMAppStateProto convertToProtoFormat(RMAppState e) {
    return RMAppStateProto.valueOf(RM_APP_PREFIX + e.name());
  }

  /**
   * 将Protobuf枚举转换为RMAppState枚举。
   * @param e Protobuf格式应用状态枚举
   * @return RM内部应用状态枚举
   */
  public static RMAppState convertFromProtoFormat(RMAppStateProto e) {
    return RMAppState.valueOf(e.name().replace(RM_APP_PREFIX, ""));
  }

  @Override
  public Map<ApplicationTimeoutType, Long> getApplicationTimeouts() {
    initApplicationTimeout();
    return this.applicationTimeouts;
  }

  /**
   * 从Protobuf中初始化应用超时配置映射表。
   */
  private void initApplicationTimeout() {
    if (this.applicationTimeouts != null) {
      return;
    }
    ApplicationStateDataProtoOrBuilder p = viaProto ? proto : builder;
    List<ApplicationTimeoutMapProto> lists = p.getApplicationTimeoutsList();
    this.applicationTimeouts =
        new HashMap<ApplicationTimeoutType, Long>(lists.size());
    // 逐个转换Protobuf条目到Java Map
    for (ApplicationTimeoutMapProto timeoutProto : lists) {
      this.applicationTimeouts.put(
          ProtoUtils
              .convertFromProtoFormat(timeoutProto.getApplicationTimeoutType()),
          timeoutProto.getTimeout());
    }
  }

  @Override
  public void setApplicationTimeouts(
      Map<ApplicationTimeoutType, Long> appTimeouts) {
    if (appTimeouts == null) {
      return;
    }
    initApplicationTimeout();
    this.applicationTimeouts.clear();
    this.applicationTimeouts.putAll(appTimeouts);
  }

  /**
   * 将本地缓存的应用超时配置添加到Protobuf构建器。
   */
  private void addApplicationTimeouts() {
    maybeInitBuilder();
    builder.clearApplicationTimeouts();
    if (applicationTimeouts == null) {
      return;
    }
    // 自定义迭代器，将Map条目转换为Protobuf对象
    Iterable<? extends ApplicationTimeoutMapProto> values =
        new Iterable<ApplicationTimeoutMapProto>() {

          @Override
          public Iterator<ApplicationTimeoutMapProto> iterator() {
            return new Iterator<ApplicationTimeoutMapProto>() {
              private Iterator<ApplicationTimeoutType> iterator =
                  applicationTimeouts.keySet().iterator();

              @Override
              public boolean hasNext() {
                return iterator.hasNext();
              }

              @Override
              public ApplicationTimeoutMapProto next() {
                ApplicationTimeoutType key = iterator.next();
                // 构建单个超时配置的Protobuf对象
                return ApplicationTimeoutMapProto.newBuilder()
                    .setTimeout(applicationTimeouts.get(key))
                    .setApplicationTimeoutType(
                        ProtoUtils.convertToProtoFormat(key))
                    .build();
              }

              @Override
              public void remove() {
                throw new UnsupportedOperationException();
              }
            };
          }
        };
    this.builder.addAllApplicationTimeouts(values);
  }
}