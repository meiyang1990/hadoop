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

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;

import java.util.List;

/**
 * AM已拉取完成容器列表事件，当AM隐式确认已成功接收容器完成通知后触发该事件
 * 用于RM节点状态机处理，清理NM端已完成的容器信息
 */
public class RMNodeFinishedContainersPulledByAMEvent extends RMNodeEvent {

  // 已被AM拉取处理的完成容器ID列表
  private List<ContainerId> containers;

  /**
   * 构造AM已拉取完成容器事件
   * @param nodeId 目标节点ID
   * @param containers 已拉取的完成容器ID列表
   */
  public RMNodeFinishedContainersPulledByAMEvent(NodeId nodeId,
      List<ContainerId> containers) {
    super(nodeId, RMNodeEventType.FINISHED_CONTAINERS_PULLED_BY_AM);
    this.containers = containers;
  }

  /**
   * 获取已拉取的完成容器ID列表
   * @return 已拉取的完成容器ID列表
   */
  public List<ContainerId> getContainers() {
    return this.containers;
  }
}