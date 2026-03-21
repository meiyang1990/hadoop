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
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEventType;

/**
 * 应用尝试注销事件，承载应用尝试完成注销所需的最终状态信息
 */
public class RMAppAttemptUnregistrationEvent extends RMAppAttemptEvent {

  // 最终跟踪URL，用于WebUI追踪应用运行结果
  private final String finalTrackingUrl;
  // 应用尝试最终运行状态
  private final FinalApplicationStatus finalStatus;

  /**
   * 构造应用尝试注销事件
   * @param appAttemptId 应用尝试ID
   * @param trackingUrl 最终跟踪URL
   * @param finalStatus 应用最终运行状态
   * @param diagnostics 诊断信息
   */
  public RMAppAttemptUnregistrationEvent(ApplicationAttemptId appAttemptId,
      String trackingUrl, FinalApplicationStatus finalStatus,
      String diagnostics) {
    super(appAttemptId, RMAppAttemptEventType.UNREGISTERED, diagnostics);
    this.finalTrackingUrl = trackingUrl;
    this.finalStatus = finalStatus;
  }

  /**
   * 获取最终跟踪URL
   * @return 最终跟踪URL
   */
  public String getFinalTrackingUrl() {
    return this.finalTrackingUrl;
  }

  /**
   * 获取应用最终运行状态
   * @return 应用最终运行状态
   */
  public FinalApplicationStatus getFinalApplicationStatus() {
    return this.finalStatus;
  }

}