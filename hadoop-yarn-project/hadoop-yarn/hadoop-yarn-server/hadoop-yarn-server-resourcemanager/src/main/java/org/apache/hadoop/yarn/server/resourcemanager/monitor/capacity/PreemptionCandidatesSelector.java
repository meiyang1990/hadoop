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

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;

import org.apache.hadoop.classification.VisibleForTesting;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 抢占候选容器选择器抽象基类，为容量调度器抢占策略定义统一接口，负责筛选需要被抢占的容器候选者
 */
public abstract class PreemptionCandidatesSelector {
  // 容量调度器抢占上下文，保存抢占相关的全局状态和配置
  protected CapacitySchedulerPreemptionContext preemptionContext;
  // 资源计算器，用于资源比较和计算
  protected ResourceCalculator rc;
  // 被kill容器最大等待超时时间（毫秒），-1表示使用默认值
  private long maximumKillWaitTime = -1;

  PreemptionCandidatesSelector(
      CapacitySchedulerPreemptionContext preemptionContext) {
    this.preemptionContext = preemptionContext;
    this.rc = preemptionContext.getResourceCalculator();
  }

  /**
   * 从当前资源分配情况结合已有选中候选，筛选需要被抢占的容器候选者
   *
   * @param selectedCandidates 已从其他策略选中的抢占候选容器
   * @param clusterResource 集群总资源
   * @param totalPreemptedResourceAllowed 本轮允许抢占的总资源量，调用后会原地更新剩余可抢占资源
   * @return 合并后的最终抢占候选容器集合，按应用尝试分组
   */
  public abstract Map<ApplicationAttemptId, Set<RMContainer>> selectCandidates(
      Map<ApplicationAttemptId, Set<RMContainer>> selectedCandidates,
      Resource clusterResource, Resource totalPreemptedResourceAllowed);

  /**
   * 对容器列表排序：优先按优先级降序，再按容器ID降序，保证低优先级、晚分配的容器优先被抢占
   *
   * @param containers 需要排序的容器列表
   */
  @VisibleForTesting
  static void sortContainers(List<RMContainer> containers) {
    Collections.sort(containers, new Comparator<RMContainer>() {
      @Override
      public int compare(RMContainer a, RMContainer b) {
        // 比较调度优先级，优先级更高的容器排在前面（后被抢占）
        int schedKeyComp = b.getAllocatedSchedulerKey()
            .compareTo(a.getAllocatedSchedulerKey());
        if (schedKeyComp != 0) {
          return schedKeyComp;
        }
        // 优先级相同时，容器ID更大（分配更晚）的排在前面，优先被抢占
        return b.getContainerId().compareTo(a.getContainerId());
      }
    });
  }

  /**
   * 获取被kill容器最大等待超时时间（毫秒）
   * @return 最大等待超时毫秒数
   */
  public long getMaximumKillWaitTimeMs() {
    if (maximumKillWaitTime > 0) {
      return maximumKillWaitTime;
    }
    // 未配置时返回上下文默认超时时间
    return preemptionContext.getDefaultMaximumKillWaitTimeout();
  }

  /**
   * 设置被kill容器最大等待超时时间
   * @param maximumKillWaitTime 最大等待超时毫秒数
   */
  public void setMaximumKillWaitTime(long maximumKillWaitTime) {
    this.maximumKillWaitTime = maximumKillWaitTime;
  }
}