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
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.Schedulable;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.SchedulingPolicy;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * 公平调度器的FIFO调度策略实现，仅用于叶子队列，按先进先出顺序调度应用
 */
@Private
@Unstable
public class FifoPolicy extends SchedulingPolicy {
  private static final Logger LOG =
      LoggerFactory.getLogger(FifoPolicy.class);

  @VisibleForTesting
  public static final String NAME = "FIFO";
  private static final FifoComparator COMPARATOR = new FifoComparator();
  private static final DefaultResourceCalculator CALCULATOR =
          new DefaultResourceCalculator();

  @Override
  public String getName() {
    return NAME;
  }

  /**
   * FIFO调度可调度对象比较器，先按优先级排序，再按提交时间排序，最后按名称排序保证确定性
   */
  static class FifoComparator implements Comparator<Schedulable>, Serializable {
    private static final long serialVersionUID = -5905036205491177060L;

    @Override
    public int compare(Schedulable s1, Schedulable s2) {
      // 先比较优先级
      int res = s1.getPriority().compareTo(s2.getPriority());
      // 优先级相同则比较提交时间
      if (res == 0) {
        res = (int) Math.signum(s1.getStartTime() - s2.getStartTime());
      }
      // 提交时间也相同则比较名称，保证排序确定性
      if (res == 0) {
        // In the rare case where jobs were submitted at the exact same time,
        // compare them by name (which will be the JobID) to get a deterministic
        // ordering, so we don't alternately launch tasks from different jobs.
        res = s1.getName().compareTo(s2.getName());
      }
      return res;
    }
  }

  @Override
  public Comparator<Schedulable> getComparator() {
    return COMPARATOR;
  }

  @Override
  public ResourceCalculator getResourceCalculator() {
    return CALCULATOR;
  }

  @Override
  public void computeShares(Collection<? extends Schedulable> schedulables,
      Resource totalResources) {
    // 没有可调度对象直接返回
    if (schedulables.isEmpty()) {
      return;
    }

    // 找出提交时间最早的应用
    Schedulable earliest = null;
    for (Schedulable schedulable : schedulables) {
      if (earliest == null ||
          schedulable.getStartTime() < earliest.getStartTime()) {
        earliest = schedulable;
      }
    }

    // 将全部资源的公平份额分配给最早提交的应用，符合FIFO策略
    if (earliest != null) {
      earliest.setFairShare(Resources.clone(totalResources));
    }
  }

  @Override
  public void computeSteadyShares(Collection<? extends FSQueue> queues,
      Resource totalResources) {
    // Nothing needs to do, as leaf queue doesn't have to calculate steady
    // fair shares for applications.
  }

  @Override
  public boolean checkIfUsageOverFairShare(Resource usage, Resource fairShare) {
    throw new UnsupportedOperationException(
        "FifoPolicy doesn't support checkIfUsageOverFairshare operation, " +
            "as FifoPolicy only works for FSLeafQueue.");
  }

  @Override
  public Resource getHeadroom(Resource queueFairShare,
                              Resource queueUsage, Resource maxAvailable) {
    // 计算队列剩余可分配内存
    long queueAvailableMemory = Math.max(
        queueFairShare.getMemorySize() - queueUsage.getMemorySize(), 0);
    // 计算可分配资源，取最大可用和队列剩余的较小值，vcore直接复用最大可用值
    Resource headroom = Resources.createResource(
        Math.min(maxAvailable.getMemorySize(), queueAvailableMemory),
        maxAvailable.getVirtualCores());
    return headroom;
  }

  @Override
  public boolean isChildPolicyAllowed(SchedulingPolicy childPolicy) {
    // FIFO策略仅允许用于叶子队列，父队列不允许使用该策略
    LOG.error(getName() + " policy is only for leaf queues. Please choose "
        + DominantResourceFairnessPolicy.NAME + " or " + FairSharePolicy.NAME
        + " for parent queues.");
    return false;
  }
}