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

package org.apache.hadoop.yarn.server.nodemanager.timelineservice;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.event.AbstractEvent;

/**
 * NodeManager时间线服务事件，会被发送给NMTimelinePublisher，
 * 最终由发布者推送到v2版本时间线服务存储。
 */
public class NMTimelineEvent extends AbstractEvent<NMTimelineEventType> {
  // 关联的应用ID
  private ApplicationId appId;

  /**
   * 构造NodeManager时间线事件，自动使用当前时间作为事件时间戳
   * @param type 事件类型
   * @param appId 关联的应用ID
   */
  public NMTimelineEvent(NMTimelineEventType type, ApplicationId appId) {
    super(type, System.currentTimeMillis());
    this.appId=appId;
  }

  /**
   * 获取事件关联的应用ID
   * @return 应用ID
   */
  public ApplicationId getApplicationId() {
    return appId;
  }
}