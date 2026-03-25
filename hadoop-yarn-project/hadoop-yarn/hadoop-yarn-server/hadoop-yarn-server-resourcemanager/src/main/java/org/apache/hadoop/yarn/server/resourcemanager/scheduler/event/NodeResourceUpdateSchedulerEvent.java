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

import org.apache.hadoop.yarn.api.records.ResourceOption;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;

/**
 * 节点资源更新调度事件，当YARN集群节点资源发生变化时，
 * 向资源调度器发送该事件触发调度器重新计算节点可用资源。
 */
public class NodeResourceUpdateSchedulerEvent extends SchedulerEvent {

  private final RMNode rmNode;
  private final ResourceOption resourceOption;
  
  /**
   * 构造节点资源更新调度事件。
   * @param rmNode 目标资源节点
   * @param resourceOption 新的资源配置选项
   */
  public NodeResourceUpdateSchedulerEvent(RMNode rmNode,
      ResourceOption resourceOption) {
    super(SchedulerEventType.NODE_RESOURCE_UPDATE);
    this.rmNode = rmNode;
    this.resourceOption = resourceOption;
  }

  public RMNode getRMNode() {
    return rmNode;
  }

  public ResourceOption getResourceOption() {
    return resourceOption;
  }

}