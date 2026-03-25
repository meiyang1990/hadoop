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

package org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;

/**
 * 队列内抢占计算插件接口，定义容量调度器队列内抢占计算的统一扩展点
 * 用于在同一个队列内部计算需要抢占的容器，实现资源在队列内应用间的合理分配
 */
interface IntraQueuePreemptionComputePlugin {

  /**
   * 获取指定队列内各应用的资源总需求
   * @param queueName 队列名称
   * @param partition 资源分区标识
   * @return 应用ID到资源需求的映射
   */
  Map<String, Resource> getResourceDemandFromAppsPerQueue(String queueName,
      String partition);

  /**
   * 计算队列内各应用的理想资源分配，筛选出需要被抢占的容器候选
   * @param clusterResource 集群总资源
   * @param tq 队列分区临时信息对象
   * @param selectedCandidates 输出参数，存储选中的待抢占容器
   * @param totalPreemptedResourceAllowed 允许抢占的总资源上限
   * @param queueTotalUnassigned 队列未分配资源总量
   * @param maxAllowablePreemptLimit 最大允许抢占比例限制
   */
  void computeAppsIdealAllocation(Resource clusterResource,
      TempQueuePerPartition tq,
      Map<ApplicationAttemptId, Set<RMContainer>> selectedCandidates,
      Resource totalPreemptedResourceAllowed, Resource queueTotalUnassigned,
      float maxAllowablePreemptLimit);

  /**
   * 获取指定队列内可被抢占的应用列表
   * @param queueName 队列名称
   * @param partition 资源分区标识
   * @return 可抢占应用集合
   */
  Collection<FiCaSchedulerApp> getPreemptableApps(String queueName,
      String partition);

  /**
   * 根据队列内策略判断是否跳过当前容器（不抢占该容器）
   * @param app 容器所属应用
   * @param clusterResource 集群总资源
   * @param usedResource 应用已使用资源
   * @param c 待判断容器
   * @return true表示跳过不抢占，false表示允许抢占
   */
  boolean skipContainerBasedOnIntraQueuePolicy(FiCaSchedulerApp app,
      Resource clusterResource, Resource usedResource, RMContainer c);
}