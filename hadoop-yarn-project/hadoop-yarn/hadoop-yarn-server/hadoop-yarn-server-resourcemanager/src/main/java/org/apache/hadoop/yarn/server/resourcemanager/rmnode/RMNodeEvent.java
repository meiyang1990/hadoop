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
import org.apache.hadoop.yarn.event.AbstractEvent;

/**
 * YARN ResourceManager 节点事件基类，封装所有RM节点相关事件的公共属性
 * 所有针对RM节点的不同类型事件都继承此类，用于RM内部状态机驱动节点状态流转
 */
public class RMNodeEvent extends AbstractEvent<RMNodeEventType> {

  // 事件关联的节点ID
  private final NodeId nodeId;

  /**
   * 构造RM节点事件
   * @param nodeId 事件关联的节点ID
   * @param type 事件类型
   */
  public RMNodeEvent(NodeId nodeId, RMNodeEventType type) {
    super(type);
    this.nodeId = nodeId;
  }

  /**
   * 获取事件关联的节点ID
   * @return 关联节点的NodeId对象
   */
  public NodeId getNodeId() {
    return this.nodeId;
  }
}