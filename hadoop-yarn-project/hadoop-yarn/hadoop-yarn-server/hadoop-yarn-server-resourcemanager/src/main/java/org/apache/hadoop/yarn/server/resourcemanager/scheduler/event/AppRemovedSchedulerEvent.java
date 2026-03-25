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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.event;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;

/**
 * YARN资源调度器应用移除事件，通知调度器移除已结束的应用。
 * 当应用完成运行后，发送该事件给调度器清理该应用的调度信息。
 */
public class AppRemovedSchedulerEvent extends SchedulerEvent {

  private final ApplicationId applicationId;
  private final RMAppState finalState;

  /**
   * 构造应用移除调度事件。
   * @param applicationId 被移除的应用ID
   * @param finalState 应用结束时的最终状态
   */
  public AppRemovedSchedulerEvent(ApplicationId applicationId,
      RMAppState finalState) {
    super(SchedulerEventType.APP_REMOVED);
    this.applicationId = applicationId;
    this.finalState = finalState;
  }

  /**
   * 获取被移除应用的ID。
   * @return 应用ID
   */
  public ApplicationId getApplicationID() {
    return this.applicationId;
  }

  /**
   * 获取应用结束时的最终状态。
   * @return 应用最终状态
   */
  public RMAppState getFinalState() {
    return this.finalState;
  }
}