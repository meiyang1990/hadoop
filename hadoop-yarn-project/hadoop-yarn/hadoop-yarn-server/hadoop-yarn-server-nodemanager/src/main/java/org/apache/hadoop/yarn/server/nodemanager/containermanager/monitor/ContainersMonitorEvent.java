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
import org.apache.hadoop.yarn.event.AbstractEvent;

/**
 * 容器监控事件，用于NodeManager容器监控模块的事件驱动处理
 */
public class ContainersMonitorEvent extends
    AbstractEvent<ContainersMonitorEventType> {

  // 关联的容器ID
  private final ContainerId containerId;

  /**
   * 构造容器监控事件
   * @param containerId 关联的容器ID
   * @param eventType 事件类型
   */
  public ContainersMonitorEvent(ContainerId containerId,
      ContainersMonitorEventType eventType) {
    super(eventType);
    this.containerId = containerId;
  }

  /**
   * 获取事件关联的容器ID
   * @return 容器ID
   */
  public ContainerId getContainerId() {
    return this.containerId;
  }

}