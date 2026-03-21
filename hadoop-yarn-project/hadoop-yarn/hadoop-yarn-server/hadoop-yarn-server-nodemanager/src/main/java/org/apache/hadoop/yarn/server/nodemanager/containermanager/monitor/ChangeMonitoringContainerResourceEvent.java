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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;

/**
 * YARN NodeManager 容器监控资源变更事件，用于通知监控系统更新容器资源配置。
 * 当容器资源发生动态调整时，触发该事件更新监控使用的资源配额。
 */
public class ChangeMonitoringContainerResourceEvent extends ContainersMonitorEvent {
  // 容器更新后的目标资源配置
  private final Resource resource;

  /**
   * 构造容器资源变更事件
   * @param containerId 目标容器ID
   * @param resource 更新后的容器资源配置
   */
  public ChangeMonitoringContainerResourceEvent(ContainerId containerId,
      Resource resource) {
    super(containerId,
        ContainersMonitorEventType.CHANGE_MONITORING_CONTAINER_RESOURCE);
    this.resource = resource;
  }

  /**
   * 获取更新后的容器资源配置
   * @return 更新后的容器资源
   */
  public Resource getResource() {
    return this.resource;
  }
}