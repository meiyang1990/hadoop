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

package org.apache.hadoop.yarn.server.resourcemanager.rmcontainer;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.scheduler.SchedulerRequestKey;

/**
 * 容器已被预留的事件，表示YARN调度器为容器预留了节点资源
 * 
 * 该事件封装了预留资源量、预留生效节点以及调度请求关键字信息
 */
public class RMContainerReservedEvent extends RMContainerEvent {

  // 预留的资源量
  private final Resource reservedResource;
  // 进行资源预留的节点ID
  private final NodeId reservedNode;
  // 对应调度请求的关键字
  private final SchedulerRequestKey reservedSchedulerKey;
  
  /**
   * 构造容器预留事件
   * @param containerId 容器ID
   * @param reservedResource 预留的资源量
   * @param reservedNode 预留资源所在节点
   * @param reservedSchedulerKey 对应调度请求关键字
   */
  public RMContainerReservedEvent(ContainerId containerId,
      Resource reservedResource, NodeId reservedNode, 
      SchedulerRequestKey reservedSchedulerKey) {
    super(containerId, RMContainerEventType.RESERVED);
    this.reservedResource = reservedResource;
    this.reservedNode = reservedNode;
    this.reservedSchedulerKey = reservedSchedulerKey;
  }

  /** 获取预留的资源量 */
  public Resource getReservedResource() {
    return reservedResource;
  }

  /** 获取预留资源所在节点ID */
  public NodeId getReservedNode() {
    return reservedNode;
  }

  /** 获取对应调度请求关键字 */
  public SchedulerRequestKey getReservedSchedulerKey() {
    return reservedSchedulerKey;
  }

}