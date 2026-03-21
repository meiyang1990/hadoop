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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.UsersManager.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.IntraQueueCandidatesSelector.TAFairOrderingComparator;
import org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.IntraQueueCandidatesSelector.TAPriorityComparator;
import org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.ProportionalCapacityPreemptionPolicy.IntraQueuePreemptionOrderPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceUsage;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AbstractLeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.SchedulingMode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy.FairOrderingPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy.OrderingPolicy;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

/**
 * FIFO策略队列内抢占插件，处理队列内基于优先级和用户限制的抢占逻辑。
 * 当队列总资源不足时，按照优先级从低到高抢占已分配资源，保障高优先级和用户配额内的应用需求。
 */
public class FifoIntraQueuePreemptionPlugin
    implements
      IntraQueuePreemptionComputePlugin {

  /** 抢占上下文，包含全局抢占配置和队列分区信息 */
  protected final CapacitySchedulerPreemptionContext context;
  /** 资源计算器，用于资源比较和计算 */
  protected final ResourceCalculator rc;

  private static final Logger LOG =
      LoggerFactory.getLogger(FifoIntraQueuePreemptionPlugin.class);

  /**
   * 构造FIFO队列内抢占插件实例。
   * @param rc 资源计算器
   * @param preemptionContext 抢占上下文
   */
  public FifoIntraQueuePreemptionPlugin(ResourceCalculator rc,
      CapacitySchedulerPreemptionContext preemptionContext) {
    this.context = preemptionContext;
    this.rc = rc;
  }

  @Override
  public Collection<FiCaSchedulerApp> getPreemptableApps(String queueName,
      String partition) {
    TempQueuePerPartition tq = context.getQueueByPartition(queueName,
        partition);

    List<FiCaSchedulerApp> apps = new ArrayList<FiCaSchedulerApp>();
    for (TempAppPerPartition tmpApp : tq.getApps()) {
      // 跳过没有需要被抢占资源的应用，不加入可抢占应用列表
      if (Resources.equals(tmpApp.getActuallyToBePreempted(),
          Resources.none())) {
        continue;
      }

      apps.add(tmpApp.app);
    }
    return apps;
  }

  @Override
  public Map<String, Resource> getResourceDemandFromAppsPerQueue(
      String queueName, String partition) {

    Map<String, Resource> resToObtainByPartition = new HashMap<>();
    TempQueuePerPartition tq = context
        .getQueueByPartition(queueName, partition);

    Collection<TempAppPerPartition> appsOrderedByPriority = tq.getApps();
    Resource actualPreemptNeeded = resToObtainByPartition.get(partition);

    // 初始化当前分区需要抢占的总资源量
    if (actualPreemptNeeded == null) {
      actualPreemptNeeded = Resources.createResource(0, 0);
      resToObtainByPartition.put(partition, actualPreemptNeeded);
    }

    // 累加所有应用需要被抢占的资源，得到队列总抢占需求
    for (TempAppPerPartition a1 : appsOrderedByPriority) {
      Resources.addTo(actualPreemptNeeded, a1.getActuallyToBePreempted());
    }

    LOG.debug("Selected to preempt {} resource from partition:{}",
        actualPreemptNeeded, partition);
    return resToObtainByPartition;
  }

  @Override
  public void computeAppsIdealAllocation(Resource clusterResource,
      TempQueuePerPartition tq,
      Map<ApplicationAttemptId, Set<RMContainer>> selectedCandidates,
      Resource totalPreemptedResourceAllowed,
      Resource queueReassignableResource, float maxAllowablePreemptLimit) {

    // 1. AM占用的资源视为冻结资源，不参与抢占计算，从可重分配资源中扣除AM资源
    Map<String, Resource> perUserAMUsed = new HashMap<String, Resource>();
    Resource amUsed = calculateUsedAMResourcesPerQueue(tq.partition,
        tq.leafQueue, perUserAMUsed);
    Resources.subtractFrom(queueReassignableResource, amUsed);

    // 2. 获取队列所有应用
    Collection<FiCaSchedulerApp> apps = tq.leafQueue.getAllApplications();

    // 队列只有一个应用时不需要进行队列内抢占
    if (apps.size() == 1) {
      return;
    }

    // 3. 创建临时应用结构，按优先级从高到低排序
    PriorityQueue<TempAppPerPartition> orderedByPriority = createTempAppForResCalculation(
        tq, apps, clusterResource, perUserAMUsed);

    // 4. 按优先级计算每个应用的理想分配资源，返回按优先级从低到高排序的应用集合
    TreeSet<TempAppPerPartition> orderedApps = calculateIdealAssignedResourcePerApp(
        clusterResource, tq, selectedCandidates, queueReassignableResource,
        orderedByPriority);

    // 5. 计算队列内可抢占的最大资源量，基于队列保证容量和配置的抢占比例
    Resource maxIntraQueuePreemptable = Resources.multiply(tq.getGuaranteed(),
        maxAllowablePreemptLimit);
    if (Resources.greaterThan(rc, clusterResource, maxIntraQueuePreemptable,
        tq.getActuallyToBePreempted())) {
      Resources.subtractFrom(maxIntraQueuePreemptable,
          tq.getActuallyToBePreempted());
    } else {
      // 已抢占资源已超过上限，本轮不允许再抢占
      maxIntraQueuePreemptable = Resource.newInstance(0, 0);
    }

    // 6. 取队列内抢占上限和全局总抢占限额的较小值，作为本轮抢占上限
    Resource preemptionLimit = Resources.min(rc, clusterResource,
        maxIntraQueuePreemptable, totalPreemptedResourceAllowed);

    // 7. 从最低优先级应用开始，按需求计算每个应用需要被抢占的资源量
    calculateToBePreemptedResourcePerApp(clusterResource, orderedApps,
        Resources.clone(preemptionLimit));

    // 将排序后的应用保存到临时队列，供后续流程使用
    tq.addAllApps(orderedApps);

    // 8. 处理同优先级下的抢占场景，过滤不符合规则的同优先级抢占
    validateOutSameAppPriorityFromDemand(clusterResource,
        (TreeSet<TempAppPerPartition>) orderedApps, tq.getUsersPerPartition(),
        context.getIntraQueuePreemptionOrderPolicy());

    if (LOG.isDebugEnabled()) {
      LOG.debug("Queue Name:" + tq.queueName + ", partition:" + tq.partition);
      for (TempAppPerPartition tmpApp : tq.getApps()) {
        LOG.debug(tmpApp.toString());
      }
    }
  }

  /**
   * 从低优先级到高优先级遍历，计算每个应用需要被抢占的资源量。
   * @param clusterResource 集群总资源
   * @param orderedApps 按优先级从低到高排序的应用集合
   * @param preemptionLimit 本轮抢占总限额
   */
  private void calculateToBePreemptedResourcePerApp(Resource clusterResource,
      TreeSet<TempAppPerPartition> orderedApps, Resource preemptionLimit) {

    for (TempAppPerPartition tmpApp : orderedApps) {
      // 抢占限额已用完或应用无已用资源，跳过该应用
      if (Resources.lessThanOrEqual(rc, clusterResource, preemptionLimit,
          Resources.none())
          || Resources.lessThanOrEqual(rc, clusterResource, tmpApp.getUsed(),
              Resources.none())) {
        continue;
      }

      // 计算当前应用可被抢占的资源：已用资源 - 理想分配 - 已选中待抢占资源 - AM资源
      Resource preemtableFromApp = Resources.subtract(tmpApp.getUsed(),
          tmpApp.idealAssigned);
      Resources.subtractFromNonNegative(preemtableFromApp, tmpApp.selected);
      Resources.subtractFromNonNegative(preemtableFromApp, tmpApp.getAMUsed());

      // 优先保障用户配额策略下，保留队列最小分配资源不被抢占
      if (context.getIntraQueuePreemptionOrderPolicy()
            .equals(IntraQueuePreemptionOrderPolicy.USERLIMIT_FIRST)) {
        Resources.subtractFromNonNegative(preemtableFromApp,
          tmpApp.getFiCaSchedulerApp().getCSLeafQueue().getMinimumAllocation());
      }

      // 计算最终需要抢占的资源，不超过当前剩余抢占限额，且不小于0
      tmpApp.toBePreempted = Resources.min(rc, clusterResource, Resources
          .max(rc, clusterResource, preemtableFromApp, Resources.none()),
          Resources.clone(preemptionLimit));

      // 扣除已分配的抢占额度，继续处理下一个应用
      preemptionLimit = Resources.subtractFromNonNegative(preemptionLimit,
          tmpApp.toBePreempted);
    }
  }

  /**
   * 按优先级从高到低计算每个应用的理想分配资源。
   * 算法逻辑：高优先级应用优先分配，直到满足自身需求或达到用户配额，剩余资源留给低优先级应用。
   * @param clusterResource 集群总资源
   * @param tq 临时队列分区信息
   * @param selectedCandidates 已选中的待抢占容器集合
   * @param queueReassignableResource 队列可重分配资源总量
   * @param orderedByPriority 按优先级从高到低排序的优先队列
   * @return 按优先级从低到高排序的应用集合
   */
  private TreeSet<TempAppPerPartition> calculateIdealAssignedResourcePerApp(
      Resource clusterResource, TempQueuePerPartition tq,
      Map<ApplicationAttemptId, Set<RMContainer>> selectedCandidates,
      Resource queueReassignableResource,
      PriorityQueue<TempAppPerPartition> orderedByPriority) {

    Comparator<TempAppPerPartition> reverseComp;
    OrderingPolicy<FiCaSchedulerApp> queueOrderingPolicy =
        tq.leafQueue.getOrderingPolicy();
    // 根据队列排序策略和抢占顺序策略选择排序比较器，结果按优先级从低到高排序
    if (queueOrderingPolicy instanceof FairOrderingPolicy
        && (context.getIntraQueuePreemptionOrderPolicy()
            == IntraQueuePreemptionOrderPolicy.USERLIMIT_FIRST)) {
      reverseComp = Collections.reverseOrder(
          new TAFairOrderingComparator(this.rc, clusterResource));
    } else {
      reverseComp = Collections.reverseOrder(new TAPriorityComparator());
    }
    TreeSet<TempAppPerPartition> orderedApps = new TreeSet<>(reverseComp);

    String partition = tq.partition;
    Map<String, TempUserPerPartition> usersPerPartition = tq.getUsersPerPartition();

    while (!orderedByPriority.isEmpty()) {
      // 取出当前优先级最高的应用处理
      TempAppPerPartition tmpApp = orderedByPriority.remove();
      orderedApps.add(tmpApp);

      // 可重分配资源已用完，后续应用理想分配直接保持0
      if (Resources.lessThanOrEqual(rc, clusterResource,
          queueReassignableResource, Resources.none()) ||
          (rc.isAnyMajorResourceZeroOrNegative(queueReassignableResource)
              && context.getInQueuePreemptionConservativeDRF())) {
        continue;
      }

      String userName = tmpApp.app.getUser();
      TempUserPerPartition tmpUser = usersPerPartition.get(userName);
      Resource userLimitResource = tmpUser.getUserLimit();
      Resource idealAssignedForUser = tmpUser.idealAssigned;

      // 统计该应用已被选中的待抢占资源总量
      getAlreadySelectedPreemptionCandidatesResource(selectedCandidates, tmpApp,
          tmpUser, partition);

      // 计算应用自身需求的理想资源：已用（扣除AM）+ 待分配 - 已选中抢占资源
      Resource appIdealAssigned = Resources.add(tmpApp.getUsedDeductAM(),
          tmpApp.getPending());
      Resources.subtractFrom(appIdealAssigned, tmpApp.selected);

      // 用户还未达到配额上限，分配资源给该应用
      if (Resources.lessThan(rc, clusterResource, idealAssignedForUser,
          userLimitResource)) {
        // 理想分配不超过用户剩余配额，也不超过队列剩余可重分配资源
        Resource idealAssigned = Resources.min(rc, clusterResource,
            appIdealAssigned,
            Resources.subtract(userLimitResource, idealAssignedForUser));
        tmpApp.idealAssigned = Resources.clone(Resources.min(rc,
            clusterResource, queueReassignableResource, idealAssigned));
        // 更新该用户已分配的理想资源
        Resources.addTo(idealAssignedForUser, tmpApp.idealAssigned);
      } else {
        // 用户已达到配额上限，不分配资源，跳过
        continue;
      }

      // 如果应用理想分配大于当前已用（扣除已选中抢占），记录需要从其他应用抢占的资源量
      Resource appUsedExcludedSelected = Resources
          .subtract(tmpApp.getUsedDeductAM(), tmpApp.selected);
      if (Resources.greaterThan(rc, clusterResource, tmpApp.idealAssigned,
          appUsedExcludedSelected)) {
        tmpApp.setToBePreemptFromOther(
            Resources.subtract(tmpApp.idealAssigned, appUsedExcludedSelected));
      }

      // 从队列可重分配资源中扣除已分配给该应用的理想资源
      Resources.subtractFromNonNegative(queueReassignableResource,
          tmpApp.idealAssigned);
    }

    return orderedApps;
  }

  /*
   * 统计应用中已被选为抢占候选的容器总资源量。
   */
  private void getAlreadySelectedPreemptionCandidatesResource(
      Map<ApplicationAttemptId, Set<RMContainer>> selectedCandidates,
      TempAppPerPartition tmpApp, TempUserPerPartition tmpUser,
      String partition) {
    tmpApp.selected = Resources.createResource(0, 0);
    Set<RMContainer> containers = selectedCandidates
        .get(tmpApp.app.getApplicationAttemptId());

    if (containers == null) {
      return;
    }

    // 累加当前分区内已选中容器的资源量
    for (RMContainer cont : containers) {
      if (partition.equals(cont.getNodeLabelExpression())) {
        Resources.addTo(tmpApp.selected, cont.getAllocatedResource());
        Resources.addTo(tmpUser.selected, cont.getAllocatedResource());
      }
    }
  }

  /**
   * 为队列中每个应用创建临时计算结构，并按优先级排序。
   * @param tq 临时队列分区信息
   * @param apps 队列所有应用
   * @param clusterResource 集群总资源
   * @param perUserAMUsed 按用户统计的AM已用资源
   * @return 按优先级排序的临时应用优先队列
   */
  private PriorityQueue<TempAppPerPartition> createTempAppForResCalculation(
      TempQueuePerPartition tq, Collection<FiCaSchedulerApp> apps,
      Resource clusterResource,
      Map<String, Resource> perUserAMUsed) {
    Comparator<TempAppPerPartition> taComparator;
    OrderingPolicy<FiCaSchedulerApp> orderingPolicy =
        tq.leafQueue.getOrderingPolicy();
    // 根据排序策略选择对应的比较器
    if (orderingPolicy instanceof FairOrderingPolicy
        && (context.getIntraQueuePreemptionOrderPolicy()
            == IntraQueuePreemptionOrderPolicy.USERLIMIT_FIRST)) {
      taComparator = new TAFairOrderingComparator(this.rc, clusterResource);
    } else {
       taComparator = new TAPriorityComparator();
    }
    PriorityQueue<TempAppPerPartition> orderedByPriority = new PriorityQueue<>(
        100, taComparator);