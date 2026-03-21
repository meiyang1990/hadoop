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

import org.apache.hadoop.classification.VisibleForTesting;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;

/**
 * 公平排序策略，按以下步骤进行可调度实体排序：
 *
 * 1. 基于公平性比较：资源使用率更低的实体优先。如果开启了sizeBasedWeight配置，
 *    高需求的应用会优先于低使用率应用，避免大量小应用持续占用资源导致大应用饿死（默认关闭）
 *
 * 2. 基于作业提交时间比较：提交时间更早的实体优先
 *
 * 3. 基于资源需求比较：有资源需求的实体优先于无需求实体
 */
public class FairOrderingPolicy<S extends SchedulableEntity> extends AbstractComparatorOrderingPolicy<S> {

  /** 开启基于规模权重计算的配置键 */
  public static final String ENABLE_SIZE_BASED_WEIGHT =
        "fair.enable-size-based-weight";

  /** 公平排序比较器实现 */
  protected class FairComparator implements Comparator<SchedulableEntity> {
    @Override
    public int compare(final SchedulableEntity r1, final SchedulableEntity r2) {
      // 第一步：比较公平性权重值，权重小的优先
      int res = (int) Math.signum( getMagnitude(r1) - getMagnitude(r2) );

      if (res == 0) {
        // 公平性相同，比较提交时间，更早的优先
        res = (int) Math.signum(r1.getStartTime() - r2.getStartTime());
      }

      if (res == 0) {
        // 提交时间相同，比较资源需求，有需求的优先
        res = compareDemand(r1, r2);
      }
      return res;
    }

    /**
     * 比较两个实体的资源需求，有需求的实体优先级更高
     */
    private int compareDemand(SchedulableEntity s1, SchedulableEntity s2) {
      int res = 0;
      // 获取两个实体的内存需求大小
      long demand1 = s1.getSchedulingResourceUsage()
          .getCachedDemand(CommonNodeLabelsManager.ANY).getMemorySize();
      long demand2 = s2.getSchedulingResourceUsage()
          .getCachedDemand(CommonNodeLabelsManager.ANY).getMemorySize();

      // s1无需求，s2有需求 -> s1优先级更低
      if ((demand1 == 0) && (demand2 > 0)) {
        res = 1;
      // s2无需求，s1有需求 -> s1优先级更高
      } else if ((demand2 == 0) && (demand1 > 0)) {
        res = -1;
      }

      return res;
    }
  }

  /** 复合比较器，组合多个比较规则 */
  private CompoundComparator fairComparator;

  /** 是否开启基于需求规模的权重调整 */
  private boolean sizeBasedWeight = false;

  /**
   * 构造公平排序策略，初始化比较器链和可调度实体集合
   */
  public FairOrderingPolicy() {
    List<Comparator<SchedulableEntity>> comparators =
      new ArrayList<Comparator<SchedulableEntity>>();
    // 添加公平比较规则
    comparators.add(new FairComparator());
    // 添加FIFO比较规则作为兜底
    comparators.add(new FifoComparator());
    fairComparator = new CompoundComparator(
      comparators
      );
    this.comparator = fairComparator;
    // 基于比较器创建并发跳表存储有序可调度实体
    this.schedulableEntities = new ConcurrentSkipListSet<S>(comparator);
  }

  /**
   * 计算可调度实体的公平排序权重值
   * @param r 可调度实体
   * @return 排序权重值，越小优先级越高
   */
  private double getMagnitude(SchedulableEntity r) {
    // 基础权重为已使用内存大小
    double mag = r.getSchedulingResourceUsage().getCachedUsed(
      CommonNodeLabelsManager.ANY).getMemorySize();
    // 如果开启了规模权重，根据需求大小调整权重
    if (sizeBasedWeight && mag != 0) {
      // 对需求取对数，计算权重系数，需求越大权重系数越大
      double weight = Math.log1p(r.getSchedulingResourceUsage().getCachedDemand(
        CommonNodeLabelsManager.ANY).getMemorySize()) / Math.log(2);
      if (weight != 0) {
        // 权重系数越大，最终权重越小，优先级越高，补偿大应用优先级
        mag = mag / weight;
      }
    }
    return mag;
  }

  @VisibleForTesting
  public boolean getSizeBasedWeight() {
   return sizeBasedWeight;
  }

  @VisibleForTesting
  public void setSizeBasedWeight(boolean sizeBasedWeight) {
   this.sizeBasedWeight = sizeBasedWeight;
  }

  @Override
  public void configure(Map<String, String> conf) {
    // 从配置中读取是否开启基于规模权重的选项
    if (conf.containsKey(ENABLE_SIZE_BASED_WEIGHT)) {
      sizeBasedWeight =
        Boolean.parseBoolean(conf.get(ENABLE_SIZE_BASED_WEIGHT));
    }
  }

  @Override
  public void containerAllocated(S schedulableEntity,
    RMContainer r) {
      // 容器分配后资源使用变化，需要重新排序实体
      entityRequiresReordering(schedulableEntity);
    }

  @Override
  public void containerReleased(S schedulableEntity,
    RMContainer r) {
      // 容器释放后资源使用变化，需要重新排序实体
      entityRequiresReordering(schedulableEntity);
    }

  @Override
  public void demandUpdated(S schedulableEntity) {
    // 开启规模权重时，需求变化会影响排序权重，需要重新排序
    if (sizeBasedWeight) {
      entityRequiresReordering(schedulableEntity);
    }
  }

  @Override
  public String getInfo() {
    String sbw = sizeBasedWeight ? " with sizeBasedWeight" : "";
    return "FairOrderingPolicy" + sbw;
  }

  @Override
  public String getConfigName() {
    // 返回该策略在容量调度器中的配置名称
    return CapacitySchedulerConfiguration.FAIR_APP_ORDERING_POLICY;
  }

}