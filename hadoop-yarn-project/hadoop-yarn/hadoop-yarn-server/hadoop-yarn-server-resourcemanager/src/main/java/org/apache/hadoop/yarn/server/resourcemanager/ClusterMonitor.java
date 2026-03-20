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

import org.apache.hadoop.yarn.api.records.ResourceOption;
import org.apache.hadoop.yarn.server.api.protocolrecords.NMContainerStatus;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;

import java.util.List;

/**
 * 集群状态监控接口
 * 实现此接口的类可接收集群状态变化通知，包括节点的添加、移除、更新和资源变更
 * 典型用例：调度器需要监听节点变化以动态调整资源分配策略
 */
public interface ClusterMonitor {

  /**
   * 节点添加通知：新 NodeManager 加入集群时触发
   * @param containerStatuses 节点上已存在的容器状态列表（节点重启后恢复）
   * @param rmNode 新添加的节点对象
   */
  void addNode(List<NMContainerStatus> containerStatuses, RMNode rmNode);

  /**
   * 节点移除通知：NodeManager 从集群中移除时触发（失联、退役等）
   * @param removedRMNode 被移除的节点对象
   */
  void removeNode(RMNode removedRMNode);

  /**
   * 节点状态更新通知：节点心跳或状态变化时触发
   * @param rmNode 状态发生变化的节点对象
   */
  void updateNode(RMNode rmNode);

  /**
   * 节点资源更新通知：节点可用资源变化时触发（如管理员动态调整资源）
   * @param rmNode 资源发生变化的节点对象
   * @param resourceOption 新的资源配置选项
   */
  void updateNodeResource(RMNode rmNode, ResourceOption resourceOption);
}
