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

package org.apache.hadoop.yarn.server.resourcemanager;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.event.AbstractEvent;

/**
 * YARN ResourceManager 应用管理器事件，封装应用管理相关的事件信息。
 * 用于RMAppManager状态机驱动，处理应用提交、移动等管理操作。
 */
public class RMAppManagerEvent extends AbstractEvent<RMAppManagerEventType> {

  // 关联的应用ID
  private final ApplicationId appId;
  // 应用移动目标队列（移动应用场景使用）
  private final String targetQueueForMove;

  /**
   * 构造不涉及队列移动的应用管理器事件。
   * @param appId 目标应用ID
   * @param type 事件类型
   */
  public RMAppManagerEvent(ApplicationId appId, RMAppManagerEventType type) {
    this(appId, "", type);
  }

  /**
   * 构造完整的应用管理器事件，支持应用移动场景。
   * @param appId 目标应用ID
   * @param targetQueueForMove 应用移动的目标队列
   * @param type 事件类型
   */
  public RMAppManagerEvent(ApplicationId appId, String targetQueueForMove,
      RMAppManagerEventType type) {
    super(type);
    this.appId = appId;
    this.targetQueueForMove = targetQueueForMove;
  }

  /**
   * 获取事件关联的应用ID。
   * @return 应用ID
   */
  public ApplicationId getApplicationId() {
    return this.appId;
  }

  /**
   * 获取应用移动的目标队列。
   * @return 目标队列名称，无移动操作时为空字符串
   */
  public String getTargetQueueForMove() {
    return this.targetQueueForMove;
  }
}