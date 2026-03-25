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
import org.apache.hadoop.yarn.event.AbstractEvent;

/**
 * NodeManager容器管理模块的容器事件基类，封装容器相关事件的通用信息
 * 所有容器状态变更事件都继承此类，用于YARN事件驱动状态机处理
 */
public class ContainerEvent extends AbstractEvent<ContainerEventType> {

  // 关联的容器ID
  private final ContainerId containerID;

  /**
   * 构造容器事件实例
   * @param cID 关联的容器ID
   * @param eventType 容器事件类型
   */
  public ContainerEvent(ContainerId cID, ContainerEventType eventType) {
    super(eventType, System.currentTimeMillis());
    this.containerID = cID;
  }

  /**
   * 获取该事件关联的容器ID
   * @return 容器ID
   */
  public ContainerId getContainerID() {
    return containerID;
  }

}