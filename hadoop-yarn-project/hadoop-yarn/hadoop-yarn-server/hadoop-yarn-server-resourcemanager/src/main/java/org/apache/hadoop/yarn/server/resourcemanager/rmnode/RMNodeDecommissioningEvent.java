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

import org.apache.hadoop.yarn.api.records.NodeId;

/**
 * YARN ResourceManager 节点下线事件，用于触发节点优雅下线流程。
 * 承载优雅下线所需的配置信息，传递给RM节点状态机处理。
 */
public class RMNodeDecommissioningEvent extends RMNodeEvent {
  // 优雅下线超时时间，单位秒，为空则使用默认配置
  private final Integer decommissioningTimeout;

  /**
   * 构造节点优雅下线事件，可指定自定义超时时间。
   * @param nodeId 目标下线节点ID
   * @param timeout 自定义优雅下线超时时间，单位秒；null表示使用默认配置
   */
  public RMNodeDecommissioningEvent(NodeId nodeId, Integer timeout) {
    super(nodeId, RMNodeEventType.GRACEFUL_DECOMMISSION);
    this.decommissioningTimeout = timeout;
  }

  /**
   * 获取配置的优雅下线超时时间。
   * @return 超时时间（秒），返回null表示使用默认配置
   */
  public Integer getDecommissioningTimeout() {
    return this.decommissioningTimeout;
  }
}