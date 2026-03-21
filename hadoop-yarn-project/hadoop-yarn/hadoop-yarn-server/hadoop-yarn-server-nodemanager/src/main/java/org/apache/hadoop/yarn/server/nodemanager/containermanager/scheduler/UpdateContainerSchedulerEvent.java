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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.scheduler;

import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container
    .Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.UpdateContainerTokenEvent;

/**
 * 容器调度器处理的容器更新事件，封装容器令牌更新相关信息，供NodeManager容器调度器处理容器资源/执行类型更新请求。
 */
public class UpdateContainerSchedulerEvent extends ContainerSchedulerEvent {

  private final UpdateContainerTokenEvent containerEvent;
  private final ContainerTokenIdentifier originalToken;

  /**
   * 构造容器更新调度事件。
   *
   * @param container 目标容器
   * @param origToken 更新前的原始容器令牌
   * @param event 容器令牌更新事件
   */
  public UpdateContainerSchedulerEvent(Container container,
      ContainerTokenIdentifier origToken, UpdateContainerTokenEvent event) {
    super(container, ContainerSchedulerEventType.UPDATE_CONTAINER);
    this.containerEvent = event;
    this.originalToken = origToken;
  }

  /**
   * 获取更新前的原始容器令牌。
   *
   * @return 原始容器令牌
   */
  public ContainerTokenIdentifier getOriginalToken() {
    return this.originalToken;
  }

  /**
   * 获取更新后的新容器令牌。
   *
   * @return 更新后的容器令牌
   */
  public ContainerTokenIdentifier getUpdatedToken() {
    return containerEvent.getUpdatedToken();
  }

  /**
   * 判断本次更新是否为资源变更更新。
   * @return true表示本次更新是资源变更，false否则
   */
  public boolean isResourceChange() {
    return containerEvent.isResourceChange();
  }

  /**
   * 判断本次更新是否为执行类型更新。
   * @return true表示本次更新是执行类型变更，false否则
   */
  public boolean isExecTypeUpdate() {
    return containerEvent.isExecTypeUpdate();
  }

  /**
   * 判断本次资源更新是否为资源增加。
   * @return true表示本次更新是增加容器资源，false表示减少资源
   */
  public boolean isIncrease() {
    return containerEvent.isIncrease();
  }
}