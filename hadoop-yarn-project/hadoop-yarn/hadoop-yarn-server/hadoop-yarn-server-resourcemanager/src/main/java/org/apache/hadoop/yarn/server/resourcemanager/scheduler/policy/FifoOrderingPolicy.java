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
import java.util.concurrent.ConcurrentSkipListSet;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;

/**
 * FIFO排序策略，按照先入先出顺序对可调度实体进行排序
 * 先比较优先级，优先级相同则按提交顺序排序
 */
public class FifoOrderingPolicy<S extends SchedulableEntity> extends AbstractComparatorOrderingPolicy<S> {
  
  /**
   * 构造FIFO排序策略，初始化比较器链和可调度实体集合
   */
  public FifoOrderingPolicy() {
    List<Comparator<SchedulableEntity>> comparators =
        new ArrayList<Comparator<SchedulableEntity>>();
    // 先按优先级排序
    comparators.add(new PriorityComparator());
    // 优先级相同按FIFO顺序（提交顺序）排序
    comparators.add(new FifoComparator());
    this.comparator = new CompoundComparator(comparators);
    // 使用跳表集合存储，支持并发访问并保持排序顺序
    this.schedulableEntities = new ConcurrentSkipListSet<S>(comparator);

  }
  
  @Override
  public void configure(Map<String, String> conf) {
    
  }
  
  @Override
  public void containerAllocated(S schedulableEntity, 
    RMContainer r) {
    }

  @Override
  public void containerReleased(S schedulableEntity, 
    RMContainer r) {
    }

  @Override
  public void demandUpdated(S schedulableEntity) {
  }

  @Override
  public String getInfo() {
    return "FifoOrderingPolicy";
  }

  @Override
  public String getConfigName() {
    return CapacitySchedulerConfiguration.FIFO_APP_ORDERING_POLICY;
  }
  
}