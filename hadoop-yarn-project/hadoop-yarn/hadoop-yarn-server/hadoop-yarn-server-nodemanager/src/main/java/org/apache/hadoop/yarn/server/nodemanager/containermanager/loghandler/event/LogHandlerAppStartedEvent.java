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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.event;

import java.util.Map;

import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.LogAggregationContext;

/**
 * 日志处理器应用启动事件，当NodeManager上启动应用时触发，封装应用日志聚合所需的全部信息。
 * 继承自LogHandlerEvent，用于日志处理流程的事件驱动处理。
 */
public class LogHandlerAppStartedEvent extends LogHandlerEvent {

  private final ApplicationId applicationId;
  private final String user;
  private final Credentials credentials;
  private final Map<ApplicationAccessType, String> appAcls;
  private final LogAggregationContext logAggregationContext;
  /**
   * The value will be set when the application is recovered from state store.
   * We use this value in AppLogAggregatorImpl to determine, if log retention
   * policy is enabled, if we need to upload old application log files. Files
   * older than retention policy will not be uploaded but scheduled for
   * deletion.
   */
  private final long recoveredAppLogInitedTime;

  /**
   * 构造应用启动日志事件，不指定日志聚合上下文和恢复时间。
   * @param appId 应用ID
   * @param user 应用提交用户
   * @param credentials 应用安全凭证
   * @param appAcls 应用访问控制列表
   */
  public LogHandlerAppStartedEvent(ApplicationId appId, String user,
      Credentials credentials, Map<ApplicationAccessType, String> appAcls) {
    this(appId, user, credentials, appAcls, null, -1);
  }

  /**
   * 构造应用启动日志事件，指定日志聚合上下文，不指定恢复时间。
   * @param appId 应用ID
   * @param user 应用提交用户
   * @param credentials 应用安全凭证
   * @param appAcls 应用访问控制列表
   * @param logAggregationContext 日志聚合上下文
   */
  public LogHandlerAppStartedEvent(ApplicationId appId, String user,
      Credentials credentials, Map<ApplicationAccessType, String> appAcls,
      LogAggregationContext logAggregationContext) {
    this(appId, user, credentials, appAcls, logAggregationContext, -1);
  }

  /**
   * 完整构造应用启动日志事件，包含所有参数。
   * @param appId 应用ID
   * @param user 应用提交用户
   * @param credentials 应用安全凭证
   * @param appAcls 应用访问控制列表
   * @param logAggregationContext 日志聚合上下文
   * @param appLogInitedTime 恢复应用的日志初始化时间，从状态 store 恢复时设置
   */
  public LogHandlerAppStartedEvent(ApplicationId appId, String user,
      Credentials credentials, Map<ApplicationAccessType, String> appAcls,
      LogAggregationContext logAggregationContext, long appLogInitedTime) {
    super(LogHandlerEventType.APPLICATION_STARTED);
    this.applicationId = appId;
    this.user = user;
    this.credentials = credentials;
    this.appAcls = appAcls;
    this.logAggregationContext = logAggregationContext;
    this.recoveredAppLogInitedTime = appLogInitedTime;
  }

  public ApplicationId getApplicationId() {
    return this.applicationId;
  }

  public Credentials getCredentials() {
    return this.credentials;
  }

  public String getUser() {
    return this.user;
  }

  public Map<ApplicationAccessType, String> getApplicationAcls() {
    return this.appAcls;
  }

  public LogAggregationContext getLogAggregationContext() {
    return this.logAggregationContext;
  }

  public long getRecoveredAppLogInitedTime() {
    return this.recoveredAppLogInitedTime;
  }
}