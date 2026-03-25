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
 * 节点更新调度事件，通知资源调度器集群节点信息发生了变化。
 * 当RM节点资源、状态更新时，会生成该事件交由调度器处理更新。
 */
public class NodeUpdateSchedulerEvent extends SchedulerEvent {

  // 发生更新的RM节点对象
  private final RMNode rmNode;

  /**
   * 构造节点更新调度事件
   * @param rmNode 发生更新的RM节点
   */
  public NodeUpdateSchedulerEvent(RMNode rmNode) {
    super(SchedulerEventType.NODE_UPDATE);
    this.rmNode = rmNode;
  }

  /**
   * 获取发生更新的RM节点对象
   * @return 发生更新的RM节点
   */
  public RMNode getRMNode() {
    return rmNode;
  }
}