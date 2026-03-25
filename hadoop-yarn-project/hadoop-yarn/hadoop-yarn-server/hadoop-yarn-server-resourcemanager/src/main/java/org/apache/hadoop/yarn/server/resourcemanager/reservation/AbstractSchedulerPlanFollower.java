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

package org.apache.hadoop.yarn.server.resourcemanager.reservation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.QueueEntitlement;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * YARN资源预留计划同步器抽象基类，负责将规划好的预留计划同步到调度器队列配置中。
 * 核心职责是根据当前时间、已规划的预留信息，动态调整父队列下各个预留子队列的容量配额，
 * 处理过期预留的清理，并在资源不足时触发重规划。
 * 子类需要实现调度器相关的队列操作抽象方法。
 */
public abstract class AbstractSchedulerPlanFollower implements PlanFollower {
  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractSchedulerPlanFollower.class);

  protected Collection<Plan> plans = new ArrayList<Plan>();
  protected YarnScheduler scheduler;
  protected Clock clock;

  @Override
  public void init(Clock clock, ResourceScheduler sched,
      Collection<Plan> plans) {
    this.clock = clock;
    this.scheduler = sched;
    this.plans.addAll(plans);
  }

  @Override
  public synchronized void run() {
    // 遍历所有计划执行同步
    for (Plan plan : plans) {
      synchronizePlan(plan, true);
    }
  }

  @Override
  public synchronized void setPlans(Collection<Plan> plans) {
    this.plans.clear();
    this.plans.addAll(plans);
  }

  @Override
  public synchronized void synchronizePlan(Plan plan, boolean shouldReplan) {
    String planQueueName = plan.getQueueName();
    LOG.debug("Running plan follower edit policy for plan: {}", planQueueName);
    // 对齐计划时间步长，取当前步长的整点时间
    long step = plan.getStep();
    long now = clock.getTime();
    if (now % step != 0) {
      now += step - (now % step);
    }
    // 获取计划对应的父队列
    Queue planQueue = getPlanQueue(planQueueName);
    if (planQueue == null) {
      return;
    }

    // 获取集群总资源
    Resource clusterResources = scheduler.getClusterResource();
    // 计算当前计划可使用的总资源
    Resource planResources =
        getPlanResources(plan, planQueue, clusterResources);
    // 获取当前时间点所有激活的预留
    Set<ReservationAllocation> currentReservations =
        plan.getReservationsAtTime(now);
    Set<String> curReservationNames = new HashSet<String>();
    Resource reservedResources = Resource.newInstance(0, 0);
    // 计算当前所有激活预留的总资源用量
    int numRes = getReservedResources(now, currentReservations,
        curReservationNames, reservedResources);
    // 创建默认预留队列（处理未绑定预留的作业）
    String defReservationId = getReservationIdFromQueueName(planQueueName)
        + ReservationConstants.DEFAULT_QUEUE_SUFFIX;
    String defReservationQueue =
        getReservationQueueName(planQueueName, defReservationId);
    createDefaultReservationQueue(planQueueName, planQueue, defReservationId);
    curReservationNames.add(defReservationId);
    // 检查是否出现计划总资源小于已预留资源的情况
    boolean shouldResize = false;
    if (arePlanResourcesLessThanReservations(plan.getResourceCalculator(),
        clusterResources, planResources, reservedResources)) {
      if (shouldReplan) {
        // 需要重规划，调用重规划器重新分配资源
        try {
          plan.getReplanner().plan(plan, null);
        } catch (PlanningException e) {
          LOG.warn("Exception while trying to replan: {}", planQueueName, e);
        }
      } else {
        // 按比例缩小各个预留的资源分配
        shouldResize = true;
      }
    }
    // 获取父队列下所有已存在的预留子队列，识别过期和新增的预留
    List<? extends Queue> resQueues = getChildReservationQueues(planQueue);
    Set<String> expired = new HashSet<String>();
    for (Queue resQueue : resQueues) {
      String resQueueName = resQueue.getQueueName();
      String reservationId = getReservationIdFromQueueName(resQueueName);
      if (curReservationNames.contains(reservationId)) {
        // 该预留仍然激活，不需要删除
        curReservationNames.remove(reservationId);
      } else {
        // 该预留已经过期，标记为待清理
        expired.add(reservationId);
      }
    }
    // 清理过期预留队列
    cleanupExpiredQueues(planQueueName, plan.getMoveOnExpiry(), expired,
        defReservationQueue);
    // 添加新增预留并更新现有预留的配额
    float totalAssignedCapacity = 0f;
    if (currentReservations != null) {
      // 先清空默认队列容量，后续重新分配
      try {
        setQueueEntitlement(planQueueName, defReservationQueue, 0f, 1.0f);
      } catch (YarnException e) {
        LOG.warn(
            "Exception while trying to release default queue capacity for plan: {}",
            planQueueName, e);
      }
      // 按新增资源量从小到大排序，先处理释放资源再分配新资源，避免临时超过容量上限
      List<ReservationAllocation> sortedAllocations = sortByDelta(
          new ArrayList<ReservationAllocation>(currentReservations), now, plan);
      for (ReservationAllocation res : sortedAllocations) {
        String currResId = res.getReservationId().toString();
        if (curReservationNames.contains(currResId)) {
          // 新增预留，创建对应子队列
          addReservationQueue(planQueueName, planQueue, currResId);
        }
        // 获取当前预留当前时刻需要的资源量
        Resource capToAssign = res.getResourcesAtTime(now);
        float targetCapacity = 0f;
        if (planResources.getMemorySize() > 0
            && planResources.getVirtualCores() > 0) {
          if (shouldResize) {
            // 资源不足，按比例缩小该预留的资源量
            capToAssign = calculateReservationToPlanProportion(
                plan.getResourceCalculator(), planResources, reservedResources,
                capToAssign);
          }
          // 计算该预留在计划总资源中的占比，作为目标容量
          targetCapacity =
              calculateReservationToPlanRatio(plan.getResourceCalculator(),
                  clusterResources, planResources, capToAssign);
        }
        LOG.debug(
              "Assigning capacity of {} to queue {} with target capacity {}",
              capToAssign, currResId, targetCapacity);
        // 帮派调度（必须同时分配所有容器）的预留不允许超额使用，最大容量等于目标容量
        float maxCapacity = 1.0f;
        if (res.containsGangs()) {
          maxCapacity = targetCapacity;
        }
        // 更新预留队列的容量配额
        try {
          setQueueEntitlement(planQueueName, currResId, targetCapacity,
              maxCapacity);
        } catch (YarnException e) {
          LOG.warn("Exception while trying to size reservation for plan: {}",
              currResId, planQueueName, e);
        }
        totalAssignedCapacity += targetCapacity;
      }
    }
    // 剩余容量全部分配给默认队列
    float defQCap = 1.0f - totalAssignedCapacity;
    LOG.debug(
          "PlanFollowerEditPolicyTask: total Plan Capacity: {} "
              + "currReservation: {} default-queue capacity: {}",
          planResources, numRes, defQCap);
    // 更新默认队列容量配额
    try {
      setQueueEntitlement(planQueueName, defReservationQueue, defQCap, 1.0f);
    } catch (YarnException e) {
      LOG.warn(
          "Exception while trying to reclaim default queue capacity for plan: {}",
          planQueueName, e);
    }
    // 归档已完成的预留
    try {
      plan.archiveCompletedReservations(now);
    } catch (PlanningException e) {
      LOG.error("Exception in archiving completed reservations: ", e);
    }
    LOG.info("Finished iteration of plan follower edit policy for plan: "
        + planQueueName);
    // Extension: update plan with app states,
    // useful to support smart replanning
  }

  protected String getReservationIdFromQueueName(String resQueueName) {
    return resQueueName;
  }

  /**
   * 设置指定预留队列的容量配额。
   */
  protected void setQueueEntitlement(String planQueueName, String currResId,
      float targetCapacity, float maxCapacity) throws YarnException {
    String reservationQueueName =
        getReservationQueueName(planQueueName, currResId);
    scheduler.setEntitlement(reservationQueueName,
        new QueueEntitlement(targetCapacity, maxCapacity));
  }

  // Schedulers have different ways of naming queues. See YARN-2773
  protected String getReservationQueueName(String planQueueName,
      String reservationId) {
    return reservationId;
  }

  /**
   * 清理过期预留队列：先将队列配额设为0禁止新应用提交，
   * 如果开启了移动，则将正在运行的应用移动到默认队列，否则杀死所有应用，最后删除队列。
   *
   * @param planQueueName 计划父队列名称
   * @param shouldMove 是否移动正在运行的应用到默认队列
   * @param toRemove 待清理的过期预留ID集合
   * @param defReservationQueue 默认预留队列名称
   */
  protected void cleanupExpiredQueues(String planQueueName, boolean shouldMove,
      Set<String> toRemove, String defReservationQueue) {
    for (String expiredReservationId : toRemove) {
      try {
        // 将配额设置为0，禁止新应用提交
        String expiredReservation =
            getReservationQueueName(planQueueName, expiredReservationId);
        setQueueEntitlement(planQueueName, expiredReservation, 0.0f, 0.0f);
        if (shouldMove) {
          // 移动已有应用到默认队列
          moveAppsInQueueSync(expiredReservation, defReservationQueue);
        }
        List<ApplicationAttemptId> appsInQueue = scheduler.
              getAppsInQueue(expiredReservation);
        int size = (appsInQueue == null ? 0 : appsInQueue.size());
        if (size > 0) {
          // 仍有应用存在，杀死所有应用
          scheduler.killAllAppsInQueue(expiredReservation);
          LOG.info("Killing applications in queue: {}", expiredReservation);
        } else {
          // 没有应用，删除队列
          scheduler.removeQueue(expiredReservation);
          LOG.info("Queue: " + expiredReservation + " removed");
        }
      } catch (YarnException e) {
        LOG.warn("Exception while trying to expire reservation: {}",
            expiredReservationId, e);
      }
    }
  }

  /**
   * 同步将过期队列中所有应用移动到默认预留队列。
   */
  private void moveAppsInQueueSync(String expiredReservation,
      String defReservationQueue) {
    List<ApplicationAttemptId> activeApps =
        scheduler.getAppsInQueue(expiredReservation);
    if (activeApps.isEmpty()) {
      return;
    }
    for (ApplicationAttemptId app : activeApps) {
      // 移动应用到默认队列
      try {
        scheduler.moveApplication(app.getApplicationId(), defReservationQueue);
      } catch (YarnException e) {
        LOG.warn(
            "Encountered unexpected error during migration of application: {}"
                + " from reservation: {}",
            app, expiredReservation, e);
      }
    }
  }

  /**
   * 统计当前激活预留的总资源用量，并收集所有激活预留ID。
   */
  protected int getReservedResources(long now,
      Set<ReservationAllocation> currentReservations,
      Set<String> curReservationNames, Resource reservedResources) {
    int numRes = 0;
    if (currentReservations != null) {
      numRes = currentReservations.size();
      for (ReservationAllocation reservation : currentReservations) {
        curReservationNames.add(reservation.getReservationId().toString());
        Resources.addTo(reservedResources, reservation.getResourcesAtTime(now));
      }
    }
    return numRes;
  }

  /**
   * 按新增资源量从小到大排序预留分配，避免调整过程中临时超过队列容量上限。
   *
   * @param currentReservations 当前激活的预留列表
   * @param now 当前时间
   * @param plan 当前计划
   * @return 排序后的预留列表
   */
  protected List<ReservationAllocation> sortByDelta(
      List<ReservationAllocation> currentReservations, long now, Plan plan) {
    Collections.sort(currentReservations,
        new ReservationAllocationComparator(now, this, plan));
    return currentReservations;
  }

  /**
   * 获取计划对应父队列。
   *
   * @param planQueueName 计划队列名称
   * @return 计划队列对象
   */
  protected abstract Queue getPlanQueue(String planQueueName);

  /**
   * 当总资源不足时，按比例缩小单个预留的资源量。
   */
  private Resource calculateReservationToPlanProportion(
      ResourceCalculator rescCalculator, Resource availablePlanResources,
      Resource totalReservationResources, Resource reservationResources) {
    return Resources.multiply(availablePlanResources, Resources.ratio(
        rescCalculator, reservationResources, totalReservationResources));
  }

  /**
   * 计算单个预留资源占计划总资源的比例，作为队列容量。
   */
  private float calculateReservationToPlanRatio(
      ResourceCalculator rescCalculator, Resource clusterResources,
      Resource planResources, Resource reservationResources) {
    return Resources.divide(rescCalculator, clusterResources,
        reservationResources, planResources);
  }

  /**
   * 检查已预留总资源是否超过计划可分配资源。
   */
  private boolean arePlanResourcesLessThanReservations(
      ResourceCalculator rescCalculator, Resource clusterResources,
      Resource planResources, Resource reservedResources) {
    return Resources.greaterThan(rescCalculator, clusterResources,
        reservedResources, planResources);
  }

  /**
   * 获取计划父队列下所有预留子队列列表。
   *
   * @param planQueue 计划父队列
   * @return 预留子队列列表
   */
  protected abstract List<? extends Queue> getChildReservationQueues(
      Queue planQueue);

  /**
   * 为新增预留创建子队列。
   *
   * @param planQueueName 计划父队列名称
   * @param queue 计划父队列对象
   * @param currResId 当前预留ID
   */
  protected abstract void addReservationQueue(String planQueueName, Queue queue,
      String currResId);

  /**
   * 创建默认预留队列，用于处理未绑定预留的应用。
   *
   * @param planQueueName 计划父队列名称
   * @param queue 计划父队列对象
   * @param defReservationQueue 默认预留队列名称
   */
  protected abstract void createDefaultReservationQueue(String planQueueName,
      Queue queue, String defReservationQueue);

  /**
   * 计算计划当前可分配的总资源量。
   *
   * @param plan 当前计划
   * @param queue 计划父队列
   * @param clusterResources 集群总资源
   * @return 计划可分配总资源
   */
  protected abstract Resource getPlanResources(Plan plan, Queue queue,
      Resource clusterResources);

  /**
   * 如果预留队列已存在，获取其已分配资源，否则返回null。
   *
   * @param plan 当前计划
   * @param reservationId 预留ID
   * @return 已分配资源或null
   */
  protected abstract Resource getReservationQueueResourceIfExists(