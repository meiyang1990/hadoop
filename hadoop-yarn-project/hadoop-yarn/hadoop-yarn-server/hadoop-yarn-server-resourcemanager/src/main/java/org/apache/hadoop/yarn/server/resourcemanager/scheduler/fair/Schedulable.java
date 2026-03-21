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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;

/**
 * YARN公平调度器可调度实体抽象接口，代表应用或队列等可被调度的实体
 * 提供统一接口让公平共享算法可以统一应用于队列内和跨队列调度
 * 
 * 核心职责包含三部分：
 * 1) 通过{@link #assignContainer}分配资源
 * 2) 向调度器提供应用/队列的相关信息，包括资源需求、最小份额、权重、优先级、启动时间等
 * 3) 存储公平调度分配给该实体的公平份额
 * 
 * 还提供updateDemand方法供调度器定期更新实体资源需求，处理失败任务、推测执行任务等统计
 */
@Private
@Unstable
public interface Schedulable {
  /**
   * 获取作业/队列名称，用于调试和调度顺序确定性破局
   * @return 作业/队列名称
   */
  String getName();

  /**
   * 获取当前可调度实体所需的最大资源总量，等于已使用资源 + 待启动资源（包含未启动和需要推测执行的任务资源）
   * @return 该可调度实体所需资源
   */
  Resource getDemand();

  /**
   * 获取该可调度实体已消耗的总资源量
   * @return 已消耗总资源量
   */
  Resource getResourceUsage();

  /**
   * 获取分配给该可调度实体的最小资源份额
   * @return 最小资源份额
   */
  Resource getMinShare();

  /**
   * 获取分配给该可调度实体的最大资源份额
   * @return 最大资源份额
   */
  Resource getMaxShare();

  /**
   * 获取公平共享中作业/队列的权重
   * 权重是相对值，2.0的权重分配的公平份额是1.0的两倍，1.0为中性权重，0代表无权重
   *
   * @return 权重值
   */
  float getWeight();

  /**
   * 获取FIFO队列中作业的启动时间，对队列实体无意义
   * @return 作业启动时间
   */
  long getStartTime();

  /**
   * 获取FIFO队列中作业的优先级，对队列实体无意义
   * @return 作业优先级
   */
  Priority getPriority();

  /**
   * 刷新当前可调度实体及其子实体（如果有）的资源需求
   */
  void updateDemand();

  /**
   * 在指定节点尝试分配容器，返回实际分配的资源量
   *
   * @param node 目标节点封装对象
   * @return 实际分配的资源量
   */
  Resource assignContainer(FSSchedulerNode node);

  /**
   * 获取分配给该可调度实体的公平份额
   * @return 分配的公平份额
   */
  Resource getFairShare();

  /**
   * 为该可调度实体设置公平份额
   * @param fairShare 要设置的公平份额
   */
  void setFairShare(Resource fairShare);

  /**
   * 检查该可调度实体是否允许被抢占
   * @return <code>true</code> 允许抢占; <code>false</code> 不允许抢占
   */
  boolean isPreemptable();
}