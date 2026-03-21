// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.nodemanager.containermanager.container;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;

/**
 * 节点管理器容器处理的容器令牌更新事件，用于容器令牌过期后的更新及相关属性变更通知。
 */
public class UpdateContainerTokenEvent extends ContainerEvent {
  private final ContainerTokenIdentifier updatedToken;
  private final boolean isResourceChange;
  private final boolean isExecTypeUpdate;
  private final boolean isIncrease;

  /**
   * 构造容器令牌更新事件。
   *
   * @param cID 容器ID
   * @param updatedToken 更新后的容器令牌
   * @param isResourceChange 是否伴随资源变更
   * @param isExecTypeUpdate 是否伴随执行类型变更
   * @param isIncrease 是否为容器资源扩容
   */
  public UpdateContainerTokenEvent(ContainerId cID,
      ContainerTokenIdentifier updatedToken, boolean isResourceChange,
      boolean isExecTypeUpdate, boolean isIncrease) {
    super(cID, ContainerEventType.UPDATE_CONTAINER_TOKEN);
    this.updatedToken = updatedToken;
    this.isResourceChange = isResourceChange;
    this.isExecTypeUpdate = isExecTypeUpdate;
    this.isIncrease = isIncrease;
  }

  /**
   * 获取更新后的容器令牌。
   *
   * @return 更新后的容器令牌标识
   */
  public ContainerTokenIdentifier getUpdatedToken() {
    return updatedToken;
  }

  /**
   * 判断本次更新是否包含资源变更。
   *
   * @return true表示包含资源变更，false表示不包含
   */
  public boolean isResourceChange() {
    return isResourceChange;
  }

  /**
   * 判断本次更新是否包含执行类型变更。
   *
   * @return true表示包含执行类型变更，false表示不包含
   */
  public boolean isExecTypeUpdate() {
    return isExecTypeUpdate;
  }

  /**
   * 判断本次更新是否为容器资源扩容。
   *
   * @return true表示是资源扩容，false表示不是
   */
  public boolean isIncrease() {
    return isIncrease;
  }
}