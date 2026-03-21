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
import org.apache.hadoop.yarn.api.records.NodeId;

/**
 * 应用在NM节点上启动完成的事件，通知RM应用已在指定节点上开始运行。
 */
public class RMAppRunningOnNodeEvent extends RMAppEvent {
  // 应用启动所在节点ID
  private final NodeId node;
  // 标识该事件是否从已获取容器状态转换而来
  private final boolean createdFromAcquiredState;

  /**
   * 构造函数，默认不从已获取容器状态创建。
   * @param appId 应用ID
   * @param node 运行应用的节点ID
   */
  public RMAppRunningOnNodeEvent(ApplicationId appId, NodeId node) {
    this(appId, node, false);
  }

  /**
   * 构造函数，支持指定状态来源。
   * @param appId 应用ID
   * @param node 运行应用的节点ID
   * @param createdFromAcquiredState 是否从已获取容器状态转换而来
   */
  public RMAppRunningOnNodeEvent(
      ApplicationId appId,
      NodeId node,
      boolean createdFromAcquiredState
  ) {
    super(appId, RMAppEventType.APP_RUNNING_ON_NODE);
    this.node = node;
    this.createdFromAcquiredState = createdFromAcquiredState;
  }
  
  /**
   * 获取运行应用的节点ID。
   * @return 节点ID
   */
  public NodeId getNodeId() {
    return node;
  }

  /**
   * 检查该事件是否从已获取容器状态转换而来。
   * @return 从已获取状态转换返回true，否则返回false
   */
  public boolean isCreatedFromAcquiredState() {
    return createdFromAcquiredState;
  }
}