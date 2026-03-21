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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.common;

import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.scheduler.SchedulerRequestKey;

/**
 * YARN资源调度器中容器的上下文信息包装类，封装调度过程中需要的容器关联信息
 * @param <A> 调度应用尝试类型，继承自SchedulerApplicationAttempt
 * @param <N> 调度节点类型，继承自SchedulerNode
 */
public class SchedulerContainer<A extends SchedulerApplicationAttempt,
    N extends SchedulerNode> {
  private RMContainer rmContainer;
  private String nodePartition;
  private A schedulerApplicationAttempt;
  private N schedulerNode;
  private boolean allocated; // 标记容器状态：已分配为True，已预留为False

  /**
   * 构造调度容器上下文对象
   * @param app 所属的调度应用尝试
   * @param node 容器所在的调度节点
   * @param rmContainer 关联的RM容器对象
   * @param nodePartition 节点分区名称
   * @param allocated 是否已分配标记
   */
  public SchedulerContainer(A app, N node, RMContainer rmContainer,
      String nodePartition, boolean allocated) {
    this.schedulerApplicationAttempt = app;
    this.schedulerNode = node;
    this.rmContainer = rmContainer;
    this.nodePartition = nodePartition;
    this.allocated = allocated;
  }

  public String getNodePartition() {
    return nodePartition;
  }

  public RMContainer getRmContainer() {
    return rmContainer;
  }

  public A getSchedulerApplicationAttempt() {
    return schedulerApplicationAttempt;
  }

  public N getSchedulerNode() {
    return schedulerNode;
  }

  public boolean isAllocated() {
    return allocated;
  }

  /**
   * 根据容器状态获取对应的调度请求键
   * @return 预留状态返回预留调度键，已分配状态返回已分配调度键
   */
  public SchedulerRequestKey getSchedulerRequestKey() {
    if (rmContainer.getState() == RMContainerState.RESERVED) {
      return rmContainer.getReservedSchedulerKey();
    }
    return rmContainer.getAllocatedSchedulerKey();
  }

  @Override
  public String toString() {
    return "(Application=" + schedulerApplicationAttempt
        .getApplicationAttemptId() + "; Node=" + schedulerNode.getNodeID()
        + "; Resource=" + rmContainer.getAllocatedOrReservedResource() + ")";
  }
}