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
 * YARN ResourceManager 集群状态变化监控接口，实现类会收到集群状态变更通知，
 * 比如节点上线、节点下线、节点信息更新等事件。
 * 用于解耦集群状态变更处理逻辑，支持不同的监控实现扩展。
 */
public interface ClusterMonitor {

  /**
   * 新增节点通知，处理节点上线事件
   * @param containerStatuses 新节点上已有的容器状态列表
   * @param rmNode 新增的RM节点对象
   */
  void addNode(List<NMContainerStatus> containerStatuses, RMNode rmNode);

  /**
   * 移除节点通知，处理节点下线事件
   * @param removedRMNode 被移除的RM节点对象
   */
  void removeNode(RMNode removedRMNode);

  /**
   * 更新节点信息通知，处理节点状态变更
   * @param rmNode 需要更新的RM节点对象
   */
  void updateNode(RMNode rmNode);

  /**
   * 更新节点资源通知，处理节点资源配置变更
   * @param rmNode 需要更新资源的RM节点对象
   * @param resourceOption 新的资源配置选项
   */
  void updateNodeResource(RMNode rmNode, ResourceOption resourceOption);
}