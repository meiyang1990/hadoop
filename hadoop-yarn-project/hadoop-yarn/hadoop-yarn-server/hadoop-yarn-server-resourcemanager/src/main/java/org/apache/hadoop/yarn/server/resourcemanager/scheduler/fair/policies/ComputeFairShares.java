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

import java.util.ArrayList;
import java.util.Collection;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.Schedulable;

import static java.lang.Math.addExact;

/**
 * 公平调度份额计算工具类，包含计算可调度对象公平份额的核心逻辑。
 * 一个可调度对象的公平份额是它有权获得的资源量，与当前集群需求和分配无关。
 * 资源占用不超过公平份额的可调度对象，其容器不会被抢占。
 */
public final class ComputeFairShares {
  
  private static final int COMPUTE_FAIR_SHARES_ITERATIONS = 25;

  private ComputeFairShares() {
  }

  /**
   * 计算给定可调度对象的瞬时公平份额，仅考虑活跃的可调度对象（即正在运行应用的对象）。
   * 
   * @param schedulables 待计算的可调度对象集合
   * @param totalResources 集群总资源
   * @param type 资源类型
   */
  public static void computeShares(
      Collection<? extends Schedulable> schedulables, Resource totalResources,
      String type) {
    computeSharesInternal(schedulables, totalResources, type, false);
  }

  /**
   * 计算给定队列的稳定公平份额，同时考虑活跃和非活跃所有队列。
   *
   * @param queues 待更新份额的队列集合
   * @param totalResources 集群总资源
   * @param type 资源类型
   */
  public static void computeSteadyShares(
      Collection<? extends FSQueue> queues, Resource totalResources,
      String type) {
    computeSharesInternal(queues, totalResources, type, true);
  }

  /**
   * 给定一组可调度对象和总资源量，计算它们带权重的公平份额。
   * 可调度对象的最小、最大份额需提前设置好，本方法会计算满足最小最大约束的最公平分配。
   * <p>
   * 算法核心思路：通过二分查找寻找合适的权重-资源比例R，使得满足以下条件且总分配等于总资源：
   * <ul>
   * <li>最小份额大于 R*权重 的可调度对象，分配其最小份额</li>
   * <li>最大份额小于 R*权重 的可调度对象，分配其最大份额</li>
   * <li>其他可调度对象分配 R*权重 的资源量</li>
   * </ul>
   * <p>
   * 通过不断二分迭代逼近最优R值，最终根据得到的R计算每个可调度对象的公平份额。
   * 算法时间复杂度是线性的，迭代次数固定，保证计算效率。
   */
  private static void computeSharesInternal(
      Collection<? extends Schedulable> allSchedulables,
      Resource totalResources, String type, boolean isSteadyShare) {
    // 存储需要动态计算份额的非固定可调度对象
    Collection<Schedulable> schedulables = new ArrayList<>();
    // 处理已固定份额的对象，返回这些对象占用的总资源
    long takenResources = handleFixedFairShares(
        allSchedulables, schedulables, isSteadyShare, type);

    if (schedulables.isEmpty()) {
      return;
    }
    // 计算所有可调度对象的最大份额总和，用于后续边界截断
    long totalMaxShare = 0;
    for (Schedulable sched : schedulables) {
      long maxShare = sched.getMaxShare().getResourceValue(type);
      totalMaxShare = safeAdd(maxShare, totalMaxShare);
      if (totalMaxShare == Long.MAX_VALUE) {
        break;
      }
    }

    // 计算可供动态分配的剩余资源，确保不小于0、不超过总最大份额
    long totalResource = Math.max((totalResources.getResourceValue(type) -
        takenResources), 0);
    totalResource = Math.min(totalMaxShare, totalResource);

    // 不断翻倍R直到总分配超过剩余资源，得到二分查找的上界
    double rMax = 1.0;
    while (resourceUsedWithWeightToResourceRatio(rMax, schedulables, type)
        < totalResource) {
      rMax *= 2.0;
    }
    // 执行固定次数的二分查找，逼近最优R值
    double left = 0;
    double right = rMax;
    for (int i = 0; i < COMPUTE_FAIR_SHARES_ITERATIONS; i++) {
      double mid = (left + right) / 2.0;
      long plannedResourceUsed = resourceUsedWithWeightToResourceRatio(
          mid, schedulables, type);
      if (plannedResourceUsed == totalResource) {
        right = mid;
        break;
      } else if (plannedResourceUsed < totalResource) {
        // R偏小，需要增大
        left = mid;
      } else {
        // R偏大，需要减小
        right = mid;
      }
    }
    // 根据收敛得到的R，更新每个可调度对象的公平份额
    for (Schedulable sched : schedulables) {
      Resource target;

      if (isSteadyShare) {
        target = ((FSQueue) sched).getSteadyFairShare();
      } else {
        target = sched.getFairShare();
      }

      target.setResourceValue(type, computeShare(sched, right, type));
    }
  }

  /**
   * 给定权重-资源比例R，计算该比例下所有可调度对象的总资源占用，用于二分查找。
   */
  private static long resourceUsedWithWeightToResourceRatio(double w2rRatio,
      Collection<? extends Schedulable> schedulables, String type) {
    long resourcesTaken = 0;
    for (Schedulable sched : schedulables) {
      long share = computeShare(sched, w2rRatio, type);
      resourcesTaken = safeAdd(resourcesTaken, share);
      if (resourcesTaken == Long.MAX_VALUE) {
        break;
      }
    }
    return resourcesTaken;
  }

  /**
   * 给定权重-资源比例R，计算单个可调度对象的分配资源量。
   */
  private static long computeShare(Sched ulable sched, double w2rRatio,
      String type) {
    double share = sched.getWeight() * w2rRatio;
    // 不低于最小份额
    share = Math.max(share, sched.getMinShare().getResourceValue(type));
    // 不高于最大份额
    share = Math.min(share, sched.getMaxShare().getResourceValue(type));
    return (long) share;
  }

  /**
   * 处理固定公平份额的可调度对象，
   * 返回固定份额对象占用的总资源，并将需要动态计算的对象添加到nonFixedSchedulables集合。
   */
  private static long handleFixedFairShares(
      Collection<? extends Schedulable> schedulables,
      Collection<Schedulable> nonFixedSchedulables,
      boolean isSteadyShare, String type) {
    long totalResource = 0;

    for (Schedulable sched : schedulables) {
      long fixedShare = getFairShareIfFixed(sched, isSteadyShare, type);
      if (fixedShare < 0) {
        // 需要动态计算，加入非固定集合
        nonFixedSchedulables.add(sched);
      } else {
        // 已经是固定份额，直接设置并累加资源占用
        Resource target;

        if (isSteadyShare) {
          target = ((FSQueue)sched).getSteadyFairShare();
        } else {
          target = sched.getFairShare();
        }

        target.setResourceValue(type, fixedShare);
        totalResource = safeAdd(totalResource, fixedShare);
      }
    }
    return totalResource;
  }

  /**
   * 如果可调度对象的公平份额是固定的，返回固定值；否则返回-1。
   * 满足以下任意条件即为固定份额：最大份额为0、权重为0，或瞬时公平份额计算时队列不活跃。
   */
  private static long getFairShareIfFixed(Schedulable sched,
      boolean isSteadyShare, String type) {

    // 最大份额为0，固定分配0
    if (sched.getMaxShare().getResourceValue(type) <= 0) {
      return 0;
    }

    // 瞬时份额计算中，不活跃队列固定分配0
    if (!isSteadyShare &&
        (sched instanceof FSQueue) && !((FSQueue)sched).isActive()) {
      return 0;
    }

    // 权重为0，返回最小份额（最小份额小于等于0则返回0）
    if (sched.getWeight() <= 0) {
      long minShare = sched.getMinShare().getResourceValue(type);
      return (minShare <= 0) ? 0 : minShare;
    }

    // 不满足固定条件，返回-1需要动态计算
    return -1;
  }

  /**
   * 安全相加两个long值，溢出时返回Long.MAX_VALUE。
   * @param a 第一个加数
   * @param b 第二个加数
   * @return 相加结果，溢出返回Long.MAX_VALUE
   */
  private static long safeAdd(long a, long b) {
    try {
      return addExact(a, b);
    } catch (ArithmeticException ae) {
      return Long.MAX_VALUE;
    }
  }
}