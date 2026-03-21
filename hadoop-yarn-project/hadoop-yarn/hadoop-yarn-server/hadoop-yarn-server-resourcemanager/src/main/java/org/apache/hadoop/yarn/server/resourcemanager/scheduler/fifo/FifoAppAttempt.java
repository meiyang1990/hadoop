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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ActiveUsersManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.ContainerRequest;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;


import org.apache.hadoop.yarn.server.scheduler.SchedulerRequestKey;

/**
 * FIFO调度器的应用尝试实现类，继承通用FiCaSchedulerApp，处理FIFO调度下的容器分配逻辑
 */
public class FifoAppAttempt extends FiCaSchedulerApp {
  private static final Logger LOG =
      LoggerFactory.getLogger(FifoAppAttempt.class);

  /**
   * 构造FIFO应用尝试对象
   * @param appAttemptId 应用尝试ID
   * @param user 提交应用的用户
   * @param queue 应用所属队列
   * @param activeUsersManager 活跃用户管理器
   * @param rmContext RM上下文对象
   */
  FifoAppAttempt(ApplicationAttemptId appAttemptId, String user,
      Queue queue, ActiveUsersManager activeUsersManager,
      RMContext rmContext) {
    super(appAttemptId, user, queue, activeUsersManager, rmContext);
  }

  /**
   * 为当前应用尝试分配容器
   * @param type 节点类型
   * @param node 调度节点对象
   * @param schedulerKey 调度请求键
   * @param container 待分配的容器
   * @return 分配完成的RMContainer对象，分配失败返回null
   */
  public RMContainer allocate(NodeType type, FiCaSchedulerNode node,
      SchedulerRequestKey schedulerKey, Container container) {

    // 获取写锁保证线程安全
    writeLock.lock();
    try {
      // 应用已停止，直接返回
      if (isStopped) {
        return null;
      }

      // 检查是否还有未满足的资源请求，无请求则返回
      // AM可在未获取调度锁时调用allocate更新请求，需要做此检查
      if (getOutstandingAsksCount(schedulerKey) <= 0) {
        return null;
      }

      // 创建RMContainer实例，管理容器生命周期
      RMContainer rmContainer = new RMContainerImpl(container,
          schedulerKey, this.getApplicationAttemptId(), node.getNodeID(),
          appSchedulingInfo.getUser(), this.rmContext, node.getPartition());
      // 设置容器所属队列名称
      ((RMContainerImpl) rmContainer).setQueueName(this.getQueueName());

      // 更新AM容器分配状态诊断信息
      updateAMContainerDiagnostics(AMState.ASSIGNED, null);

      // 将容器加入新分配容器列表，用于后续通知AM
      addToNewlyAllocatedContainers(node, rmContainer);

      ContainerId containerId = container.getId();
      // 将容器加入活跃容器映射表
      liveContainers.put(containerId, rmContainer);

      // 更新调度信息，记录本次分配
      ContainerRequest containerRequest = appSchedulingInfo.allocate(
            type, node, schedulerKey, rmContainer);

      // 增加应用尝试已使用资源统计
      attemptResourceUsage.incUsed(node.getPartition(),
          container.getResource());

      // 关联容器分配对应的资源请求
      ((RMContainerImpl) rmContainer).setContainerRequest(containerRequest);

      // 发送容器启动事件，触发容器状态流转
      rmContainer.handle(
          new RMContainerEvent(containerId, RMContainerEventType.START));

      // 调试日志记录分配信息
      if (LOG.isDebugEnabled()) {
        LOG.debug("allocate: applicationAttemptId=" + containerId
            .getApplicationAttemptId() + " container=" + containerId + " host="
            + container.getNodeId().getHost() + " type=" + type);
      }
      // 为节省审计日志空间，仅非默认分区时记录分区信息
      String partition = null;
      if (appAMNodePartitionName != null &&
            !appAMNodePartitionName.isEmpty()) {
        partition = appAMNodePartitionName;
      }
      // 记录容器分配成功审计日志
      RMAuditLogger.logSuccess(getUser(),
          RMAuditLogger.AuditConstants.ALLOC_CONTAINER, "SchedulerApp",
          getApplicationId(), containerId, container.getResource(),
          getQueueName(), partition);

      return rmContainer;
    } finally {
      // 释放写锁
      writeLock.unlock();
    }
  }
}