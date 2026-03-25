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

package org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEventType;

/**
 * 应用尝试状态更新事件，用于通知ResourceManager应用尝试更新了进度和追踪地址。
 */
public class RMAppAttemptStatusupdateEvent extends RMAppAttemptEvent {

  // 应用尝试进度，取值范围0-1
  private final float progress;
  // 应用尝试追踪地址，用于Web UI跳转到应用监控页面
  private final String trackingUrl;

  /**
   * 构造仅更新进度的状态更新事件。
   * @param appAttemptId 应用尝试ID
   * @param progress 应用尝试进度
   */
  public RMAppAttemptStatusupdateEvent(ApplicationAttemptId appAttemptId,
      float progress) {
    this(appAttemptId, progress, null);
  }

  /**
   * 构造同时更新进度和追踪地址的状态更新事件。
   * @param appAttemptId 应用尝试ID
   * @param progress 应用尝试进度
   * @param trackingUrl 应用追踪地址
   */
  public RMAppAttemptStatusupdateEvent(ApplicationAttemptId appAttemptId,
                                       float progress, String trackingUrl) {
    super(appAttemptId, RMAppAttemptEventType.STATUS_UPDATE);
    this.progress = progress;
    this.trackingUrl = trackingUrl;
  }

  /**
   * 获取应用尝试进度。
   * @return 进度值，0-1
   */
  public float getProgress() {
    return this.progress;
  }

  /**
   * 获取应用追踪地址。
   * @return 追踪URL，可能为null
   */
  public String getTrackingUrl() {
    return this.trackingUrl;
  }

}