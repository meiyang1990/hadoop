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

import java.util.List;

import org.apache.hadoop.yarn.server.api.protocolrecords.NMContainerStatus;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;

/**
 * 节点加入调度器事件，通知资源调度器有新的NodeManager节点上线加入集群
 * 承载新增节点及其已有容器的状态信息
 */
public class NodeAddedSchedulerEvent extends SchedulerEvent {

  private final RMNode rmNode;
  private final List<NMContainerStatus> containerReports;

  /**
   * 构造不包含已有容器信息的节点添加事件
   * @param rmNode 新加入的RMNode节点对象
   */
  public NodeAddedSchedulerEvent(RMNode rmNode) {
    super(SchedulerEventType.NODE_ADDED);
    this.rmNode = rmNode;
    this.containerReports = null;
  }

  /**
   * 构造包含已有容器信息的节点添加事件，用于节点重新加入时恢复容器状态
   * @param rmNode 新加入的RMNode节点对象
   * @param containerReports 节点上已有容器的状态报告列表
   */
  public NodeAddedSchedulerEvent(RMNode rmNode,
      List<NMContainerStatus> containerReports) {
    super(SchedulerEventType.NODE_ADDED);
    this.rmNode = rmNode;
    this.containerReports = containerReports;
  }

  /**
   * 获取新加入集群的节点对象
   * @return 新增的RMNode节点
   */
  public RMNode getAddedRMNode() {
    return rmNode;
  }

  /**
   * 获取新增节点上已有容器的状态报告列表
   * @return 容器状态报告列表，节点首次加入时为null
   */
  public List<NMContainerStatus> getContainerReports() {
    return containerReports;
  }
}