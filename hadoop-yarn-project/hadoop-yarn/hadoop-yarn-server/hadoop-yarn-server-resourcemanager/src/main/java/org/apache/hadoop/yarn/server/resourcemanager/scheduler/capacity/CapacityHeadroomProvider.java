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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import java.util.Set;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.util.resource.Resources;

/**
 * 容量调度器可用资源余量计算器，负责计算应用程序可分配的剩余资源额度
 */
public class CapacityHeadroomProvider {
  
  UsersManager.User user;
  AbstractLeafQueue queue;
  FiCaSchedulerApp application;
  AbstractLeafQueue.QueueResourceLimitsInfo queueResourceLimitsInfo;
  
  /**
   * 构造可用资源余量计算器
   * @param user 提交应用的用户
   * @param queue 应用所在的叶子队列
   * @param application 目标应用
   * @param queueResourceLimitsInfo 队列资源限制信息
   */
  public CapacityHeadroomProvider(UsersManager.User user, AbstractLeafQueue queue,
      FiCaSchedulerApp application,
      AbstractLeafQueue.QueueResourceLimitsInfo queueResourceLimitsInfo) {

    this.user = user;
    this.queue = queue;
    this.application = application;
    this.queueResourceLimitsInfo = queueResourceLimitsInfo;
  }
  
  /**
   * 计算应用当前可获得的剩余可用资源
   * @return 剩余可用资源实例
   */
  public Resource getHeadroom() {
    // 当前队列资源限制
    Resource queueCurrentLimit;
    // 集群总资源
    Resource clusterResource;
    // 同步获取队列资源限制信息，保证线程安全
    synchronized (queueResourceLimitsInfo) {
      queueCurrentLimit = queueResourceLimitsInfo.getQueueCurrentLimit();
      clusterResource = queueResourceLimitsInfo.getClusterResource();
    }
    // 获取应用请求的节点标签分区集合
    Set<String> requestedPartitions =
        application.getAppSchedulingInfo().getRequestedPartitions();
    Resource headroom;
    // 判断是否请求无标签节点（默认分区）
    if (requestedPartitions.isEmpty() || (requestedPartitions.size() == 1
        && requestedPartitions.contains(RMNodeLabelsManager.NO_LABEL))) {
      // 直接计算默认分区的可用资源
      headroom = queue.getHeadroom(user, queueCurrentLimit, clusterResource,
          application);
    } else {
      // 多分区场景，初始化可用资源为0
      headroom = Resource.newInstance(0, 0);
      // 遍历所有请求的分区，累加各分区可用资源
      for (String partition : requestedPartitions) {
        Resource partitionHeadRoom = queue.getHeadroom(user, queueCurrentLimit,
            clusterResource, application, partition);
        Resources.addTo(headroom, partitionHeadRoom);
      }
    }
    // 边界处理：防止计算出负数可用资源，校正为0
    if (headroom.getMemorySize() < 0) {
      headroom.setMemorySize(0);
    }
    return headroom;
  }
}