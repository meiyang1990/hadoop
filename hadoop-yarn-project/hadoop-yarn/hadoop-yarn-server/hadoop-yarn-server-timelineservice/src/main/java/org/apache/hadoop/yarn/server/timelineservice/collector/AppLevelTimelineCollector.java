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

package org.apache.hadoop.yarn.server.timelineservice.collector;

import java.util.concurrent.Future;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.client.TimelineDelegationTokenIdentifier;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 应用级别时间线收集器服务，负责处理单个YARN应用的时间线数据写入，并将数据持久化到后端存储。
 * 负责管理该应用相关的生命周期流程，包括委托令牌的更新和服务启停。
 */
@Private
@Unstable
public class AppLevelTimelineCollector extends TimelineCollector {
  private static final Logger LOG =
      LoggerFactory.getLogger(TimelineCollector.class);

  // 当前收集器对应YARN应用ID
  private final ApplicationId appId;
  // 应用提交用户名
  private final String appUser;
  // 时间线实体上下文，存储当前应用的上下文信息
  private final TimelineCollectorContext context;
  // 当前操作的用户信息
  private UserGroupInformation currentUser;
  // 当前应用的时间线委托令牌
  private Token<TimelineDelegationTokenIdentifier> delegationTokenForApp;
  // 委托令牌最大有效期时间戳
  private long tokenMaxDate = 0;
  // 委托令牌更新者
  private String tokenRenewer;
  // 委托令牌更新/重新生成的异步任务Future
  private Future<?> renewalOrRegenerationFuture;

  /**
   * 构造应用级别时间线收集器，不指定提交用户。
   * @param appId YARN应用ID
   */
  public AppLevelTimelineCollector(ApplicationId appId) {
    this(appId, null);
  }

  /**
   * 构造应用级别时间线收集器，指定应用ID和提交用户。
   * @param appId YARN应用ID
   * @param user 应用提交用户名
   */
  public AppLevelTimelineCollector(ApplicationId appId, String user) {
    super(AppLevelTimelineCollector.class.getName() + " - " + appId.toString());
    Preconditions.checkNotNull(appId, "AppId shouldn't be null");
    this.appId = appId;
    this.appUser = user;
    context = new TimelineCollectorContext();
  }

  public UserGroupInformation getCurrentUser() {
    return currentUser;
  }

  public String getAppUser() {
    return appUser;
  }

  /**
   * 设置应用的委托令牌及相关更新任务信息。
   * @param token 委托令牌
   * @param appRenewalOrRegenerationFuture 更新/重新生成异步任务
   * @param tknMaxDate 令牌最大有效期
   * @param renewer 令牌更新者
   */
  void setDelegationTokenAndFutureForApp(
      Token<TimelineDelegationTokenIdentifier> token,
      Future<?> appRenewalOrRegenerationFuture, long tknMaxDate,
      String renewer) {
    this.delegationTokenForApp = token;
    this.tokenMaxDate = tknMaxDate;
    this.tokenRenewer = renewer;
    this.renewalOrRegenerationFuture = appRenewalOrRegenerationFuture;
  }

  /**
   * 更新应用的委托令牌更新/重新生成异步任务。
   * @param appRenewalOrRegenerationFuture 新的异步任务Future
   */
  void setRenewalOrRegenerationFutureForApp(
      Future<?> appRenewalOrRegenerationFuture) {
    this.renewalOrRegenerationFuture = appRenewalOrRegenerationFuture;
  }

  /**
   * 取消应用的委托令牌更新/重新生成异步任务。
   */
  void cancelRenewalOrRegenerationFutureForApp() {
    if (renewalOrRegenerationFuture != null &&
        !renewalOrRegenerationFuture.isDone()) {
      renewalOrRegenerationFuture.cancel(true);
    }
  }

  long getAppDelegationTokenMaxDate() {
    return tokenMaxDate;
  }

  String getAppDelegationTokenRenewer() {
    return tokenRenewer;
  }

  @VisibleForTesting
  public Token<TimelineDelegationTokenIdentifier> getDelegationTokenForApp() {
    return this.delegationTokenForApp;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    // 从配置中读取集群ID，设置到上下文中
    context.setClusterId(conf.get(YarnConfiguration.RM_CLUSTER_ID,
        YarnConfiguration.DEFAULT_RM_CLUSTER_ID));
    // 先设置默认值，后续会通过RPC调用从NodeManager获取最新上下文信息进行更新
    // 当前用户通常不是应用提交用户，但需要保证该字段非空
    currentUser = UserGroupInformation.getCurrentUser();
    context.setUserId(currentUser.getShortUserName());
    context.setAppId(appId.toString());
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    // 停止时取消未完成的令牌更新任务
    cancelRenewalOrRegenerationFutureForApp();
    super.serviceStop();
  }

  @Override
  public TimelineCollectorContext getTimelineEntityContext() {
    return context;
  }
}