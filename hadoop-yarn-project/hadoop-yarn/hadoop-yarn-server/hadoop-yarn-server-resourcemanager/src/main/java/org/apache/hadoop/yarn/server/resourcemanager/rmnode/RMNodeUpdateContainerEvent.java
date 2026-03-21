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

package org.apache.hadoop.yarn.server.resourcemanager.rmnode;

import java.util.Map;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerUpdateType;
import org.apache.hadoop.yarn.api.records.NodeId;

/**
 * RM节点容器更新事件，用于通知RMNode需要更新节点上运行容器的资源配置。
 * 该事件会被ResourceManager节点状态机处理，触发对应容器的更新操作。
 *
 */
public class RMNodeUpdateContainerEvent extends RMNodeEvent {
  // 待更新容器集合，key为待更新容器对象，value为更新类型
  private Map<Container, ContainerUpdateType> toBeUpdatedContainers;

  /**
   * 构造容器更新事件实例
   * @param nodeId 目标节点ID
   * @param toBeUpdatedContainers 待更新容器与更新类型映射
   */
  public RMNodeUpdateContainerEvent(NodeId nodeId,
      Map<Container, ContainerUpdateType> toBeUpdatedContainers) {
    super(nodeId, RMNodeEventType.UPDATE_CONTAINER);
    this.toBeUpdatedContainers = toBeUpdatedContainers;
  }

  /**
   * 获取所有待更新的容器列表及其更新类型
   * @return 待更新容器与更新类型映射
   */
  public Map<Container, ContainerUpdateType> getToBeUpdatedContainers() {
    return toBeUpdatedContainers;
  }
}