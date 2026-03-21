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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.event;

import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;

/**
 * 节点下线调度事件，通知调度器集群中移除了一个节点，需要清理该节点上的资源和调度信息。
 */
public class NodeRemovedSchedulerEvent extends SchedulerEvent {

  /** 已被移除的RM节点对象 */
  private final RMNode rmNode;

  /**
   * 构造节点移除调度事件
   * @param rmNode 被移除的RM节点
   */
  public NodeRemovedSchedulerEvent(RMNode rmNode) {
    super(SchedulerEventType.NODE_REMOVED);
    this.rmNode = rmNode;
  }

  /**
   * 获取被移除的RM节点对象
   * @return 已移除的RM节点实例
   */
  public RMNode getRemovedRMNode() {
    return rmNode;
  }

}