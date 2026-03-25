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

package org.apache.hadoop.yarn.server.resourcemanager.metrics;

import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;

/**
 * 系统指标发布器接口，用于将应用和容器的生命周期事件发布到Timeline服务
 */
public interface SystemMetricsPublisher {

  /**
   * 发布应用创建事件
   * @param app 资源管理器应用对象
   * @param createdTime 创建时间戳
   */
  void appCreated(RMApp app, long createdTime);

  /**
   * 发布应用启动完成事件
   * @param app 资源管理器应用对象
   * @param launchTime 启动时间戳
   */
  void appLaunched(RMApp app, long launchTime);

  /**
   * 发布应用访问权限更新事件
   * @param app 资源管理器应用对象
   * @param appViewACLs 更新后的查看权限ACL列表
   * @param updatedTime 更新时间戳
   */
  void appACLsUpdated(RMApp app, String appViewACLs, long updatedTime);

  /**
   * 发布应用通用更新事件
   * @param app 资源管理器应用对象
   * @param updatedTime 更新时间戳
   */
  void appUpdated(RMApp app, long updatedTime);

  /**
   * 发布应用状态更新事件
   * @param app 资源管理器应用对象
   * @param appState 更新后的Yarn应用状态
   * @param updatedTime 更新时间戳
   */
  void appStateUpdated(RMApp app, YarnApplicationState appState,
      long updatedTime);

  /**
   * 发布应用完成事件
   * @param app 资源管理器应用对象
   * @param state 应用最终状态
   * @param finishedTime 完成时间戳
   */
  void appFinished(RMApp app, RMAppState state, long finishedTime);

  /**
   * 发布应用尝试注册完成事件
   * @param appAttempt 资源管理器应用尝试对象
   * @param registeredTime 注册完成时间戳
   */
  void appAttemptRegistered(RMAppAttempt appAttempt, long registeredTime);

  /**
   * 发布应用尝试完成事件
   * @param appAttempt 资源管理器应用尝试对象
   * @param appAttemtpState 应用尝试最终状态
   * @param app 所属应用对象
   * @param finishedTime 完成时间戳
   */
  void appAttemptFinished(RMAppAttempt appAttempt,
      RMAppAttemptState appAttemtpState, RMApp app, long finishedTime);

  /**
   * 发布容器创建事件
   * @param container 资源管理器容器对象
   * @param createdTime 创建时间戳
   */
  void containerCreated(RMContainer container, long createdTime);

  /**
   * 发布容器完成事件
   * @param container 资源管理器容器对象
   * @param finishedTime 完成时间戳
   */
  void containerFinished(RMContainer container, long finishedTime);
}