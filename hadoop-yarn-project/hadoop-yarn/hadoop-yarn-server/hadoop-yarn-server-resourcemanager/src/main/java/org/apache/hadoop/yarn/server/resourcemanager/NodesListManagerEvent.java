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

package org.apache.hadoop.yarn.server.resourcemanager;

import org.apache.hadoop.yarn.event.AbstractEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;

/**
 * 节点列表管理器事件，封装节点列表变更相关事件信息
 * 用于YARN资源管理器内部驱动节点状态变更的事件处理
 */
public class NodesListManagerEvent extends
    AbstractEvent<NodesListManagerEventType> {
  // 触发本次事件的节点对象
  private final RMNode node;

  /**
   * 构造节点列表管理器事件
   * @param type 事件类型
   * @param node 关联的RM节点
   */
  public NodesListManagerEvent(NodesListManagerEventType type, RMNode node) {
    super(type);
    this.node = node;
  }

  /**
   * 获取事件关联的RM节点
   * @return 触发事件的RM节点
   */
  public RMNode getNode() {
    return node;
  }
}