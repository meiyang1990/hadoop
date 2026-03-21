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

/**
 * 容器停止监控事件，通知容器监控停止对指定容器的资源监控。
 * 用于容器退出或容器重初始化场景，取消容器的资源使用监控。
 */
public class ContainerStopMonitoringEvent extends ContainersMonitorEvent {

  /** 标记是否为容器重初始化场景触发停止监控 */
  private final boolean forReInit;

  /**
   * 构造普通容器停止监控事件，非重初始化场景。
   * @param containerId 目标容器ID
   */
  public ContainerStopMonitoringEvent(ContainerId containerId) {
    super(containerId, ContainersMonitorEventType.STOP_MONITORING_CONTAINER);
    forReInit = false;
  }

  /**
   * 构造可指定是否为重初始化场景的停止监控事件。
   * @param containerId 目标容器ID
   * @param forReInit 是否为容器重初始化场景触发
   */
  public ContainerStopMonitoringEvent(ContainerId containerId,
      boolean forReInit) {
    super(containerId, ContainersMonitorEventType.STOP_MONITORING_CONTAINER);
    this.forReInit = forReInit;
  }

  /**
   * 获取是否为容器重初始化场景触发停止监控。
   * @return true表示为重初始化场景，false表示普通退出场景
   */
  public boolean isForReInit() {
    return forReInit;
  }
}