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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy;

import java.util.*;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * 待处理应用的FIFO排序策略，仅用于排序待调度应用
 * 按照以下优先级对可调度实体排序：
 * <ul>
 * <li>正在恢复的应用优先</li>
 * <li>应用优先级高的优先</li>
 * <li>先提交的应用优先</li>
 * </ul>
 * <p>
 * 示例：若添加了 E1(true,1,1) E2(true,2,2) E3(true,3,3) E4(false,4,4) E5(false,4,5)
 * 排序后迭代顺序为：E3(true,3,3) E2(true,2,2) E1(true,1,1) E5(false,5,5) E4(false,4,4)
 */
public class FifoOrderingPolicyForPendingApps<S extends SchedulableEntity>
    extends AbstractComparatorOrderingPolicy<S> {

  /**
   * 构造FIFO排序策略，初始化多级别比较器和并发排序集合
   */
  public FifoOrderingPolicyForPendingApps() {
    List<Comparator<SchedulableEntity>> comparators =
        new ArrayList<Comparator<SchedulableEntity>>();
    // 第一级：正在恢复的应用优先
    comparators.add(new RecoveryComparator());
    // 第二级：高优先级应用优先
    comparators.add(new PriorityComparator());
    // 第三级：先提交的应用优先（FIFO顺序）
    comparators.add(new FifoComparator());
    this.comparator = new CompoundComparator(comparators);
    this.schedulableEntities = new ConcurrentSkipListSet<S>(comparator);
  }

  @Override
  public String getInfo() {
    return "FifoOrderingPolicyForPendingApps";
  }

  @Override
  public String getConfigName() {
    return CapacitySchedulerConfiguration.FIFO_FOR_PENDING_APPS;
  }

  @Override
  public void configure(Map<String, String> conf) {
  }

  @Override
  public void containerAllocated(S schedulableEntity, RMContainer r) {
  }

  @Override
  public void containerReleased(S schedulableEntity, RMContainer r) {
  }

  @Override
  public void demandUpdated(S schedulableEntity) {
  }

}