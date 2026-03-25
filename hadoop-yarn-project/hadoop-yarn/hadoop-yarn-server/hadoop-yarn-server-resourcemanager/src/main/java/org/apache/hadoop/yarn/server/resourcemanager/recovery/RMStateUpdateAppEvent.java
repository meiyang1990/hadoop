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

package org.apache.hadoop.yarn.server.resourcemanager.recovery;

import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.ApplicationStateData;

import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.SettableFuture;

/**
 * YARN ResourceManager 状态存储更新应用状态事件，用于向RM状态存储传递应用信息更新请求。
 * 该事件在RM状态恢复流程中承载应用状态数据和更新配置。
 */
public class RMStateUpdateAppEvent extends RMStateStoreEvent {
  // 待更新的应用状态数据
  private final ApplicationStateData appState;
  // After application state is updated in state store,
  // should notify back to application or not
  // 状态更新完成后是否需要通知回应用
  private boolean notifyApplication;
  // 异步更新结果future，用于通知调用方更新完成
  private SettableFuture<Object> future;

  /**
   * 构造应用状态更新事件，默认更新后通知应用。
   * @param appState 待更新的应用状态数据
   */
  public RMStateUpdateAppEvent(ApplicationStateData appState) {
    this (appState, true);
  }

  /**
   * 构造应用状态更新事件，可指定是否通知应用。
   * @param appState 待更新的应用状态数据
   * @param notifyApplication 更新完成后是否需要通知应用
   */
  public RMStateUpdateAppEvent(ApplicationStateData appState,
      boolean notifyApplication) {
    super(RMStateStoreEventType.UPDATE_APP);
    this.appState = appState;
    this.notifyApplication = notifyApplication;
    this.future = null;
  }

  /**
   * 构造应用状态更新事件，支持异步结果回调。
   * @param appState 待更新的应用状态数据
   * @param notifyApp 更新完成后是否需要通知应用
   * @param future 用于接收更新结果的future
   */
  public RMStateUpdateAppEvent(ApplicationStateData appState, boolean notifyApp,
      SettableFuture<Object> future) {
    super(RMStateStoreEventType.UPDATE_APP);
    this.appState = appState;
    this.notifyApplication = notifyApp;
    this.future = future;
  }

  /**
   * 获取待更新的应用状态数据。
   * @return 应用状态数据对象
   */
  public ApplicationStateData getAppState() {
    return appState;
  }

  /**
   * 获取更新完成后是否需要通知应用的标志位。
   * @return true表示需要通知，false表示不需要
   */
  public boolean isNotifyApplication() {
    return notifyApplication;
  }

  /**
   * 获取异步更新结果的future对象。
   * @return 结果future，null表示不需要异步回调
   */
  public SettableFuture<Object> getResult() {
    return future;
  }
}