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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.container;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ResourceSet;

/**
 * 容器重新初始化事件，由容器管理器发送给容器实例，触发容器重新初始化
 * 主要用于容器升级场景，在不重启容器的前提下更新容器配置和资源
 */
public class ContainerReInitEvent extends ContainerEvent {

  // 重新初始化使用的容器启动上下文，包含升级后的配置信息
  private final ContainerLaunchContext reInitLaunchContext;
  // 重新初始化需要的本地化资源集合
  private final ResourceSet resourceSet;
  // 是否自动提交本次重新初始化，无需后续确认
  private final boolean autoCommit;

  /**
   * 构造容器重新初始化事件
   * @param cID 目标容器ID
   * @param upgradeContext 升级上下文，包含新的容器启动配置
   * @param resourceSet 重新初始化需要的本地化资源集合
   * @param autoCommit 是否自动提交本次重新初始化
   */
  public ContainerReInitEvent(ContainerId cID,
      ContainerLaunchContext upgradeContext,
      ResourceSet resourceSet, boolean autoCommit){
    super(cID, ContainerEventType.REINITIALIZE_CONTAINER);
    this.reInitLaunchContext = upgradeContext;
    this.resourceSet = resourceSet;
    this.autoCommit = autoCommit;
  }

  /**
   * 获取重新初始化使用的容器启动上下文（用于升级）
   * @return 容器启动上下文
   */
  public ContainerLaunchContext getReInitLaunchContext() {
    return reInitLaunchContext;
  }

  /**
   * 获取重新初始化需要的本地化资源集合
   * @return 资源集合
   */
  public ResourceSet getResourceSet() {
    return resourceSet;
  }

  /**
   * 判断本次重新初始化是否需要自动提交
   * @return 自动提交标识，true表示无需额外确认直接完成重新初始化
   */
  public boolean isAutoCommit() {
    return autoCommit;
  }
}