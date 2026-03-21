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
 * YARN NodeManager容器监控启动事件，用于触发对新启动容器的资源监控
 * 携带容器资源限制和启动耗时相关信息，传递给容器监控系统处理
 */
public class ContainerStartMonitoringEvent extends ContainersMonitorEvent {

  // 虚拟内存限制（字节）
  private final long vmemLimit;
  // 物理内存限制（字节）
  private final long pmemLimit;
  // CPU核心数限制
  private final int cpuVcores;
  // 容器启动过程耗时（毫秒）
  private final long launchDuration;
  // 容器资源本地化耗时（毫秒）
  private final long localizationDuration;

  /**
   * 构造容器启动监控事件
   * @param containerId 目标容器ID
   * @param vmemLimit 虚拟内存限制（字节）
   * @param pmemLimit 物理内存限制（字节）
   * @param cpuVcores CPU核心数限制
   * @param launchDuration 容器启动过程耗时（毫秒）
   * @param localizationDuration 容器资源本地化耗时（毫秒）
   */
  public ContainerStartMonitoringEvent(ContainerId containerId,
      long vmemLimit, long pmemLimit, int cpuVcores, long launchDuration,
      long localizationDuration) {
    super(containerId, ContainersMonitorEventType.START_MONITORING_CONTAINER);
    this.vmemLimit = vmemLimit;
    this.pmemLimit = pmemLimit;
    this.cpuVcores = cpuVcores;
    this.launchDuration = launchDuration;
    this.localizationDuration = localizationDuration;
  }

  public long getVmemLimit() {
    return this.vmemLimit;
  }

  public long getPmemLimit() {
    return this.pmemLimit;
  }

  public int getCpuVcores() {
    return this.cpuVcores;
  }

  public long getLaunchDuration() {
    return this.launchDuration;
  }

  public long getLocalizationDuration() {
    return this.localizationDuration;
  }
}