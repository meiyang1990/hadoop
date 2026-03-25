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

package org.apache.hadoop.yarn.server.nodemanager.containermanager;

import org.apache.hadoop.service.ServiceStateChangeListener;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.api.records.ContainerQueuingLimit;
import org.apache.hadoop.yarn.server.api.records.OpportunisticContainersStatus;
import org.apache.hadoop.yarn.server.nodemanager.ContainerManagerEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ResourceLocalizationService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor
    .ContainersMonitor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.scheduler
    .ContainerScheduler;

/**
 * NodeManager 容器管理器接口，负责管理节点上所有容器的完整生命周期。
 * 整合容器调度、资源监控、资源本地化等核心子模块，对外提供统一容器管理入口。
 */
public interface ContainerManager extends ServiceStateChangeListener,
    ContainerManagementProtocol,
    EventHandler<ContainerManagerEvent> {

  /**
   * 获取当前节点的容器监控组件，用于监控容器资源使用情况。
   * @return 容器监控实例
   */
  ContainersMonitor getContainersMonitor();

  /**
   * 获取当前节点机会型容器的运行状态统计信息。
   * @return 机会型容器状态
   */
  OpportunisticContainersStatus getOpportunisticContainersStatus();

  /**
   * 更新节点容器排队限制配置，控制排队容器的最大数量/资源配额。
   * @param queuingLimit 容器排队限制参数
   */
  void updateQueuingLimit(ContainerQueuingLimit queuingLimit);

  /**
   * 获取当前节点的容器调度组件，负责容器排队与调度执行。
   * @return 容器调度实例
   */
  ContainerScheduler getContainerScheduler();

  /**
   * 处理凭据更新请求，刷新容器使用的认证凭据。
   */
  void handleCredentialUpdate();

  /**
   * 获取资源本地化服务，负责容器依赖资源的下载与本地化管理。
   * @return 资源本地化服务实例
   */
  ResourceLocalizationService getResourceLocalizationService();

}