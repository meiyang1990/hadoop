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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies;

import java.io.Serializable;
import java.util.Collection;
import java.util.Comparator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.Schedulable;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.SchedulingPolicy;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * 公平份额调度策略，通过平衡内存资源分配实现公平调度
 * Makes scheduling decisions by trying to equalize shares of memory.
 */
@Private
@Unstable
public class FairSharePolicy extends SchedulingPolicy {
  @VisibleForTesting
  public static final String NAME = "fair";
  private static final Logger LOG =
      LoggerFactory.getLogger(FairSharePolicy.class);
  // 内存资源类型标识
  private static final String MEMORY = ResourceInformation.MEMORY_MB.getName();
  // 默认资源计算器实例，只统计内存
  private static final DefaultResourceCalculator RESOURCE_CALCULATOR =
      new DefaultResourceCalculator();
  // 公平份额比较器单例
  private static final FairShareComparator COMPARATOR =
          new FairShareComparator();

  @Override
  public String getName() {
    return NAME;
  }

  /**
   * Compare Schedulables mainly via fair share usage to meet fairness.
   * Specifically, it goes through following four steps.
   *
   * 1. Compare demands. Schedulables without resource demand get lower priority
   * than ones who have demands.
   * 
   * 2. Compare min share usage. Schedulables below their min share are compared
   * by how far below it they are as a ratio. For example, if job A has 8 out
   * of a min share of 10 tasks and job B has 50 out of a min share of 100,
   * then job B is scheduled next, because B is at 50% of its min share and A
   * is at 80% of its min share.
   * 
   * 3. Compare fair share usage. Schedulables above their min share are
   * compared by fair share usage by checking (resource usage / weight).
   * If all weights are equal, slots are given to the job with the fewest tasks;
   * otherwise, jobs with more weight get proportionally more slots. If weight
   * equals to 0, we can't compare Schedulables by (resource usage/weight).
   * There are two situations: 1)All weights equal to 0, slots are given
   * to one with less resource usage. 2)Only one of weight equals to 0, slots
   * are given to the one with non-zero weight.
   *
   * 4. Break the tie by compare submit time and job name.
   */
  private static class FairShareComparator implements Comparator<Schedulable>,
      Serializable {
    private static final long serialVersionUID = 5564969375856699313L;

    @Override
    public int compare(Schedulable s1, Schedulable s2) {
      // 第一步：比较资源需求，无需求的排在后面
      int res = compareDemand(s1, s2);

      // 缓存资源使用量，避免重复计算
      Resource resourceUsage1 = null;
      Resource resourceUsage2 = null;

      // 需求相等，第二步：比较最小份额使用率，更缺资源的排在前面
      if (res == 0) {
        resourceUsage1 = s1.getResourceUsage();
        resourceUsage2 = s2.getResourceUsage();
        res = compareMinShareUsage(s1, s2, resourceUsage1, resourceUsage2);
      }

      // 最小份额比较相等，第三步：比较公平份额使用率
      if (res == 0) {
        res = compareFairShareUsage(s1, s2, resourceUsage1, resourceUsage2);
      }

      // 仍相等，按提交时间打破平局，先提交的排在前面
      if (res == 0) {
        res = (int) Math.signum(s1.getStartTime() - s2.getStartTime());
      }

      // 仍相等，按名称字典序打破平局
      if (res == 0) {
        res = s1.getName().compareTo(s2.getName());
      }

      return res;
    }

    /**
     * 比较两个可调度对象的资源需求，保证有需求的比没需求的优先级更高
     */
    private int compareDemand(Schedulable s1, Schedulable s2) {
      int res = 0;
      long demand1 = s1.getDemand().getMemorySize();
      long demand2 = s2.getDemand().getMemorySize();

      // s1无需求，s2有需求 -> s1优先级更低
      if ((demand1 == 0) && (demand2 > 0)) {
        res = 1;
      } 
      // s2无需求，s1有需求 -> s2优先级更低
      else if ((demand2 == 0) && (demand1 > 0)) {
        res = -1;
      }

      return res;
    }

    /**
     * 基于最小份额比较两个可调度对象的优先级
     */
    private int compareMinShareUsage(Schedulable s1, Schedulable s2,
        Resource resourceUsage1, Resource resourceUsage2) {
      int res;
      // 计算实际需要保障的最小份额，不超过当前需求
      long minShare1 = Math.min(s1.getMinShare().getMemorySize(),
          s1.getDemand().getMemorySize());
      long minShare2 = Math.min(s2.getMinShare().getMemorySize(),
          s2.getDemand().getMemorySize());
      // 判断两个对象是否还未满足最小份额
      boolean s1Needy = resourceUsage1.getMemorySize() < minShare1;
      boolean s2Needy = resourceUsage2.getMemorySize() < minShare2;

      // 只有s1未满足最小份额，s1优先级更高
      if (s1Needy && !s2Needy) {
        res = -1;
      } 
      // 只有s2未满足最小份额，s2优先级更高
      else if (s2Needy && !s1Needy) {
        res = 1;
      } 
      // 两个都未满足最小份额，按已用占最小份额的比例比较，比例越低优先级越高
      else if (s1Needy && s2Needy) {
        double minShareRatio1 = (double) resourceUsage1.getMemorySize();
        double minShareRatio2 = (double) resourceUsage2.getMemorySize();

        if (minShare1 > 1) {
          minShareRatio1 /= minShare1;
        }

        if (minShare2 > 1) {
          minShareRatio2 /= minShare2;
        }

        res = (int) Math.signum(minShareRatio1 - minShareRatio2);
      } 
      // 两个都满足最小份额，比较结果相等
      else {
        res = 0;
      }

      return res;
    }

    /**
     * To simplify computation, use weights instead of fair shares to calculate
     * fair share usage.
     * 基于权重计算公平份额使用率，比较两个可调度对象优先级
     */
    private int compareFairShareUsage(Schedulable s1, Schedulable s2,
        Resource resourceUsage1, Resource resourceUsage2) {
      double weight1 = s1.getWeight();
      double weight2 = s2.getWeight();
      double useToWeightRatio1;
      double useToWeightRatio2;

      // 两个权重都大于0，计算资源用量/权重的比值，比值越低优先级越高
      if (weight1 > 0.0 && weight2 > 0.0) {
        useToWeightRatio1 = resourceUsage1.getMemorySize() / weight1;
        useToWeightRatio2 = resourceUsage2.getMemorySize() / weight2;
      } 
      // 权重相等（都为0），直接比较资源用量，用量越少优先级越高
      else if (weight1 == weight2) { // Either weight1 or weight2 equals to 0
        // If they have same weight, just compare usage
        useToWeightRatio1 = resourceUsage1.getMemorySize();
        useToWeightRatio2 = resourceUsage2.getMemorySize();
      } 
      // 只有一个权重为0，非零权重的优先级更高
      else {
        // By setting useToWeightRatios to negative weights, we give the
        // zero-weight one less priority, so the non-zero weight one will
        // be given slots.
        useToWeightRatio1 = -weight1;
        useToWeightRatio2 = -weight2;
      }

      return (int) Math.signum(useToWeightRatio1 - useToWeightRatio2);
    }
  }

  @Override
  public Comparator<Schedulable> getComparator() {
    return COMPARATOR;
  }

  @Override
  public ResourceCalculator getResourceCalculator() {
    return RESOURCE_CALCULATOR;
  }

  /**
   * 计算队列可分配的剩余资源量，不超过公平份额剩余量
   */
  @Override
  public Resource getHeadroom(Resource queueFairShare,
                              Resource queueUsage, Resource maxAvailable) {
    // 计算公平份额剩余内存，不能为负
    long queueAvailableMemory = Math.max(
        queueFairShare.getMemorySize() - queueUsage.getMemorySize(), 0);
    // 取最大可用内存和公平份额剩余内存的较小值，CPU沿用最大可用的CPU数量
    Resource headroom = Resources.createResource(
        Math.min(maxAvailable.getMemorySize(), queueAvailableMemory),
        maxAvailable.getVirtualCores());
    return headroom;
  }

  /**
   * 计算所有可调度对象的公平份额分配
   */
  @Override
  public void computeShares(Collection<? extends Schedulable> schedulables,
      Resource totalResources) {
    ComputeFairShares.computeShares(schedulables, totalResources, MEMORY);
  }

  /**
   * 计算所有队列的稳定公平份额（稳态分配）
   */
  @Override
  public void computeSteadyShares(Collection<? extends FSQueue> queues,
      Resource totalResources) {
    ComputeFairShares.computeSteadyShares(queues, totalResources, MEMORY);
  }

  /**
   * 检查资源用量是否超过公平份额
   */
  @Override
  public boolean checkIfUsageOverFairShare(Resource usage, Resource fairShare) {
    return usage.getMemorySize() > fairShare.getMemorySize();
  }

  /**
   * 检查是否允许设置指定的子队列调度策略
   */
  @Override
  public boolean isChildPolicyAllowed(SchedulingPolicy childPolicy) {
    // 父策略是单资源公平共享，不允许子队列使用 Dominant 资源公平策略
    if (childPolicy instanceof DominantResourceFairnessPolicy) {
      LOG.error("Queue policy can't be " + DominantResourceFairnessPolicy.NAME
          + " if the parent policy is " + getName() + ". Choose " +
          getName() + " or " + FifoPolicy.NAME + " for child queues instead."
          + " Please note that " + FifoPolicy.NAME
          + " is only for leaf queues.");
      return false;
    }
    return true;
  }
}