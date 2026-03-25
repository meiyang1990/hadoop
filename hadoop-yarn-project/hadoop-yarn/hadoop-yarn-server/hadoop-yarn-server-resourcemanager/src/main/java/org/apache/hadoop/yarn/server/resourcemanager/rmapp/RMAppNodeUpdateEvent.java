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

package org.apache.hadoop.yarn.server.resourcemanager.rmapp;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.NodeUpdateType;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;

/**
 * 应用节点状态更新事件，承载YARN集群中节点状态变化信息，通知RM应用处理节点可用性变更。
 * 当集群节点状态发生变化时，会生成该事件传递给对应RM应用进行处理。
 */
public class RMAppNodeUpdateEvent extends RMAppEvent {

  /**
   * 应用层面的节点更新类型，定义节点对应用的可用状态
   */
  public enum RMAppNodeUpdateType {
    /** 节点可用，可分配容器运行任务 */
    NODE_USABLE, 
    /** 节点不可用，无法分配新容器 */
    NODE_UNUSABLE,
    /** 节点正在下线过程中 */
    NODE_DECOMMISSIONING;

    /**
     * 将RM应用层节点更新类型转换为公共API层的NodeUpdateType
     * @param rmAppNodeUpdateType 应用层节点更新类型
     * @return 公共API层对应节点更新类型
     */
    public static NodeUpdateType convertToNodeUpdateType(
        RMAppNodeUpdateType rmAppNodeUpdateType) {
      return NodeUpdateType.valueOf(rmAppNodeUpdateType.name());
    }
  }

  // 发生状态变更的RM节点对象
  private final RMNode node;
  // 本次更新的类型
  private final RMAppNodeUpdateType updateType;

  /**
   * 构造应用节点更新事件
   * @param appId 目标应用ID
   * @param node 发生状态变更的节点
   * @param updateType 更新类型
   */
  public RMAppNodeUpdateEvent(ApplicationId appId, RMNode node,
      RMAppNodeUpdateType updateType) {
    super(appId, RMAppEventType.NODE_UPDATE);
    this.node = node;
    this.updateType = updateType;
  }

  /**
   * 获取发生状态变更的节点对象
   * @return RM节点对象
   */
  public RMNode getNode() {
    return node;
  }

  /**
   * 获取本次节点更新类型
   * @return 节点更新类型
   */
  public RMAppNodeUpdateType getUpdateType() {
    return updateType;
  }

}