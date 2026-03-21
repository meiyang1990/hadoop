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

/**
 * RMNode清理容器事件，通知ResourceManager需要清理指定节点上的指定容器。
 * 用于容器完成/释放后，更新ResourceManager端节点的容器资源记录。
 */
public class RMNodeCleanContainerEvent extends RMNodeEvent {

  /** 需要清理的容器ID */
  private ContainerId contId;

  /**
   * 构造清理容器事件。
   * @param nodeId 目标节点ID
   * @param contId 需要清理的容器ID
   */
  public RMNodeCleanContainerEvent(NodeId nodeId, ContainerId contId) {
    super(nodeId, RMNodeEventType.CLEANUP_CONTAINER);
    this.contId = contId;
  }

  /**
   * 获取需要清理的容器ID。
   * @return 待清理容器的ID
   */
  public ContainerId getContainerId() {
    return this.contId;
  }
}