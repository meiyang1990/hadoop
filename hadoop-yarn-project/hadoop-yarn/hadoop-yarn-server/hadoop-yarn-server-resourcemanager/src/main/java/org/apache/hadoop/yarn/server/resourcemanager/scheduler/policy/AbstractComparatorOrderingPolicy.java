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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.*;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;

/**
 * YARN调度器可调度实体排序策略的抽象基类，基于比较器实现排序逻辑
 * 为所有基于比较器的排序策略提供公共基础能力，子类只需实现特定业务逻辑
 */
public abstract class AbstractComparatorOrderingPolicy<S extends SchedulableEntity> implements OrderingPolicy<S> {
  
  private static final Logger LOG =
      LoggerFactory.getLogger(OrderingPolicy.class);
                                            
  // 基于跳表实现的并发有序可调度实体集合，保持排序顺序
  protected ConcurrentSkipListSet<S> schedulableEntities;
  // 排序使用的比较器，定义实体间的比较规则
  protected Comparator<SchedulableEntity> comparator;
  // 需要重新排序的实体缓存，key为实体ID，value为实体对象
  protected Map<String, S> entitiesToReorder = new HashMap<String, S>();
  
  public AbstractComparatorOrderingPolicy() { }
  
  @Override
  public Collection<S> getSchedulableEntities() {
    return schedulableEntities;
  }

  @Override
  public Iterator<S> getAssignmentIterator(IteratorSelector sel) {
    // 先完成延迟重排序，再返回迭代器
    reorderScheduleEntities();
    return schedulableEntities.iterator();
  }

  @Override
  public Iterator<S> getPreemptionIterator() {
    // 先完成延迟重排序，抢占使用倒序迭代器
    reorderScheduleEntities();
    return schedulableEntities.descendingIterator();
  }
  
  /**
   * 更新资源使用信息的缓存，更新任意标签的全局资源统计缓存
   */
  public static void updateSchedulingResourceUsage(ResourceUsage ru) {
    ru.setCachedUsed(CommonNodeLabelsManager.ANY, ru.getAllUsed());
    ru.setCachedPending(CommonNodeLabelsManager.ANY, ru.getAllPending());
  }
  
  /**
   * 重新排序单个可调度实体，更新其在有序集合中的位置
   */
  protected void reorderSchedulableEntity(S schedulableEntity) {
    // 先移除旧位置
    schedulableEntities.remove(schedulableEntity);
    // 更新资源使用缓存
    updateSchedulingResourceUsage(
      schedulableEntity.getSchedulingResourceUsage());
    // 重新插入，自动放到正确排序位置
    schedulableEntities.add(schedulableEntity);
  }
  
  /**
   * 批量处理所有需要重排序的实体，采用延迟批量重排序优化性能
   */
  protected void reorderScheduleEntities() {
    synchronized (entitiesToReorder) {
      // 遍历所有待重排序实体
      for (Map.Entry<String, S> entry :
          entitiesToReorder.entrySet()) {
        reorderSchedulableEntity(entry.getValue());
      }
      // 清空待重排序队列
      entitiesToReorder.clear();
    }
  }

  /**
   * 标记实体需要重新排序，加入待重排序缓存（延迟排序）
   */
  protected void entityRequiresReordering(S schedulableEntity) {
    synchronized (entitiesToReorder) {
      entitiesToReorder.put(schedulableEntity.getId(), schedulableEntity);
    }
  }

  public Comparator<SchedulableEntity> getComparator() {
    return comparator; 
  }
  
  @Override
  public void addSchedulableEntity(S s) {
    if (null == s) {
      return;
    }
    schedulableEntities.add(s); 
  }
  
  @Override
  public boolean removeSchedulableEntity(S s) {
    if (null == s) {
      return false;
    }
    // 从待重排序缓存中移除
    synchronized (entitiesToReorder) {
      entitiesToReorder.remove(s.getId());
    }
    return schedulableEntities.remove(s); 
  }
  
  @Override
  public void addAllSchedulableEntities(Collection<S> sc) {
    schedulableEntities.addAll(sc);
  }
  
  @Override
  public int getNumSchedulableEntities() {
    return schedulableEntities.size(); 
  }
  
  @Override
  public abstract void configure(Map<String, String> conf);
  
  @Override
  public abstract void containerAllocated(S schedulableEntity, 
    RMContainer r);
  
  @Override
  public abstract void containerReleased(S schedulableEntity, 
    RMContainer r);
  
  @Override
  public abstract void demandUpdated(S schedulableEntity);

  @Override
  public abstract String getInfo();

  @Override
  public abstract String getConfigName();

}