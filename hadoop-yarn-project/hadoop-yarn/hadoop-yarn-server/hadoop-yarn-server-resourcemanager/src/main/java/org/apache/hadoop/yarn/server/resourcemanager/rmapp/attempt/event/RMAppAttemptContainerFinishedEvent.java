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

package org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEventType;

/**
 * YARN ResourceManager中，应用尝试容器完成事件。
 * 当某个容器执行完成时，触发此事件通知应用尝试更新状态。
 */
public class RMAppAttemptContainerFinishedEvent extends RMAppAttemptEvent {

  // 已完成容器的状态信息
  private final ContainerStatus containerStatus;
  // 容器所在节点ID
  private final NodeId nodeId;

  /**
   * 构造容器完成事件。
   * @param appAttemptId 应用尝试ID
   * @param containerStatus 已完成容器的状态
   * @param nodeId 容器所在节点ID
   */
  public RMAppAttemptContainerFinishedEvent(ApplicationAttemptId appAttemptId, 
      ContainerStatus containerStatus, NodeId nodeId) {
    super(appAttemptId, RMAppAttemptEventType.CONTAINER_FINISHED);
    this.containerStatus = containerStatus;
    this.nodeId = nodeId;
  }

  /**
   * 获取已完成容器的状态信息。
   * @return 容器状态
   */
  public ContainerStatus getContainerStatus() {
    return this.containerStatus;
  }

  /**
   * 获取已完成容器所在节点ID。
   * @return 节点ID
   */
  public NodeId getNodeId() {
    return this.nodeId;
  }
}