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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.TreeSet;

import org.apache.hadoop.classification.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ActiveUsersManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerAppUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.util.resource.Resources;

import static org.apache.hadoop.yarn.util.resource.Resources.none;

/**
 * 公平调度器叶子队列，实际承载运行应用的队列，不包含子队列
 * 负责管理本队列内所有应用尝试的生命周期、资源分配、抢占计算等工作
 */
@Private
@Unstable
public class FSLeafQueue extends FSQueue {
  private static final Logger LOG = LoggerFactory.
      getLogger(FSLeafQueue.class.getName());
  private static final List<FSQueue> EMPTY_LIST = Collections.emptyList;

  private FSContext context;

  // 可运行状态的应用尝试列表
  private final List<FSAppAttempt> runnableApps = new ArrayList<>();
  // 不可运行状态的应用尝试列表
  private final List<FSAppAttempt> nonRunnableApps = new ArrayList<>();
  // 已分配到该队列但尚未创建应用尝试的应用集合
  private final Set<ApplicationId> assignedApps = new HashSet<>();
  // 读写锁，采用公平排序策略保护应用列表更新
  private final ReadWriteLock rwl = new ReentrantReadWriteLock(true);
  private final Lock readLock = rwl.readLock();
  private final Lock writeLock = rwl.writeLock();
  
  // 队列总资源需求
  private Resource demand = Resources.createResource(0);
  
  // 抢占相关：队列上次满足最小资源份额的时间戳
  private long lastTimeAtMinShare;

  // 队列 Application Master 资源使用量
  private Resource amResourceUsage;

  private final ActiveUsersManager activeUsersManager;

  /**
   * 构造叶子队列实例
   * @param name 队列名称
   * @param scheduler 公平调度器实例
   * @param parent 父队列
   */
  public FSLeafQueue(String name, FairScheduler scheduler,
      FSParentQueue parent) {
    super(name, scheduler, parent);
    this.context = scheduler.getContext();
    this.lastTimeAtMinShare = scheduler.getClock().getTime();
    activeUsersManager = new ActiveUsersManager(getMetrics());
    amResourceUsage = Resource.newInstance(0, 0);
    getMetrics().setAMResourceUsage(amResourceUsage);
  }
  
  /**
   * 将应用尝试添加到队列
   * @param app 应用尝试实例
   * @param runnable 是否可运行
   */
  void addApp(FSAppAttempt app, boolean runnable) {
    writeLock.lock();
    try {
      if (runnable) {
        runnableApps.add(app);
      } else {
        nonRunnableApps.add(app);
      }
      // 应用尝试创建完成，从未创建尝试的已分配应用列表移除
      assignedApps.remove(app.getApplicationId());
      incUsedResource(app.getResourceUsage());
    } finally {
      writeLock.unlock();
    }
  }
  
  /**
   * 从队列移除指定应用尝试
   * @return 应用移除前是否为可运行状态
   */
  boolean removeApp(FSAppAttempt app) {
    boolean runnable = false;

    // 持有写锁时从可运行/不可运行列表移除应用
    writeLock.lock();
    try {
      runnable = runnableApps.remove(app);
      if (!runnable) {
        // 不在可运行列表，尝试从不可运行列表移除
        if (!removeNonRunnableApp(app)) {
          throw new IllegalStateException("Given app to remove " + app +
              " does not exist in queue " + this);
        }
      }
    } finally {
      writeLock.unlock();
    }

    // 如果应用可运行且AM正在运行，更新AM资源使用量
    if (runnable && app.isAmRunning()) {
      Resources.subtractFrom(amResourceUsage, app.getAMResource());
      getMetrics().setAMResourceUsage(amResourceUsage);
    }

    decUsedResource(app.getResourceUsage());
    return runnable;
  }

  /**
   * 从不可运行列表移除指定应用尝试
   * @param app 应用尝试实例
   * @return 是否成功移除
   */
  boolean removeNonRunnableApp(FSAppAttempt app) {
    writeLock.lock();
    try {
      return nonRunnableApps.remove(app);
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * 判断应用尝试是否在可运行列表中
   * @param attempt 应用尝试实例
   * @return 是否可运行
   */
  boolean isRunnableApp(FSAppAttempt attempt) {
    readLock.lock();
    try {
      return runnableApps.contains(attempt);
    } finally {
      readLock.unlock();
    }
  }

  /**
   * 判断应用尝试是否在不可运行列表中
   * @param attempt 应用尝试实例
   * @return 是否不可运行
   */
  boolean isNonRunnableApp(FSAppAttempt attempt) {
    readLock.lock();
    try {
      return nonRunnableApps.contains(attempt);
    } finally {
      readLock.unlock();
    }
  }

  /**
   * 获取不可运行应用尝试的拷贝列表
   * @return 不可运行应用尝试列表拷贝
   */
  List<FSAppAttempt> getCopyOfNonRunnableAppSchedulables() {
    List<FSAppAttempt> appsToReturn = new ArrayList<>();
    readLock.lock();
    try {
      appsToReturn.addAll(nonRunnableApps);
    } finally {
      readLock.unlock();
    }
    return appsToReturn;
  }

  @Override
  public void collectSchedulerApplications(
      Collection<ApplicationAttemptId> apps) {
    readLock.lock();
    try {
      // 收集所有可运行应用尝试ID
      for (FSAppAttempt appSched : runnableApps) {
        apps.add(appSched.getApplicationAttemptId());
      }
      // 收集所有不可运行应用尝试ID
      for (FSAppAttempt appSched : nonRunnableApps) {
        apps.add(appSched.getApplicationAttemptId());
      }
    } finally {
      readLock.unlock();
    }
  }

  @Override
  void updateInternal() {
    readLock.lock();
    try {
      // 重新计算队列内所有可运行应用的公平份额
      policy.computeShares(runnableApps, getFairShare());
    } finally {
      readLock.unlock();
    }
  }

  /**
   * 计算应用集合的公平份额饥饿总量，并将饥饿应用加入全局饥饿列表
   * @param appsWithDemand 按饥饿程度排序的有需求应用集合
   * @return 所有应用的公平份额饥饿总和
   */
  private Resource updateStarvedAppsFairshare(
      TreeSet<FSAppAttempt> appsWithDemand) {
    Resource fairShareStarvation = Resources.clone(none());
    // 遍历按饥饿程度排序的应用
    for (FSAppAttempt app : appsWithDemand) {
      Resource appStarvation = app.fairShareStarvation();
      if (!Resources.isNone(appStarvation))  {
        // 应用存在公平份额饥饿，加入全局饥饿列表
        context.getStarvedApps().addStarvedApp(app);
        // 累加饥饿总量
        Resources.addTo(fairShareStarvation, appStarvation);
      } else {
        // 遇到第一个无饥饿的应用，后续应用都不会饥饿，直接中断
        break;
      }
    }
    return fairShareStarvation;
  }

  /**
   * 将队列最小份额饥饿分配给各个有需求的应用
   * @param appsWithDemand 有需求应用集合
   * @param minShareStarvation 队列总最小份额饥饿量
   */
  private void updateStarvedAppsMinshare(
      final TreeSet<FSAppAttempt> appsWithDemand,
      final Resource minShareStarvation) {
    Resource pending = Resources.clone(minShareStarvation);

    // 持续分配直到饥饿全部分配完成
    for (FSAppAttempt app : appsWithDemand) {
      if (!Resources.isNone(pending)) {
        // 获取应用未满足需求，扣除已通过公平份额饥饿获得的部分
        Resource appMinShare = app.getPendingDemand();
        Resources.subtractFromNonNegative(
            appMinShare, app.getFairshareStarvation());

        // 如果应用剩余需求超过待分配饥饿量，只分配剩余待分配部分
        if (Resources.greaterThan(policy.getResourceCalculator(),
            scheduler.getClusterResource(), appMinShare, pending)) {
          Resources.subtractFromNonNegative(appMinShare, pending);
          pending = none();
        } else {
          // 分配应用全部剩余需求，扣除待分配总量
          Resources.subtractFromNonNegative(pending, appMinShare);
        }
        // 设置应用最小份额饥饿量，加入全局饥饿列表
        app.setMinshareStarvation(appMinShare);
        context.getStarvedApps().addStarvedApp(app);
      } else {
        // 饥饿已分配完成，重置其他应用的最小份额饥饿
        app.resetMinshareStarvation();
      }
    }
  }

  /**
   * 更新队列饥饿应用列表，识别处于饥饿状态的应用，用于抢占计算
   * 仅可在{@link #updateInternal}方法执行完毕、应用份额更新完成后调用
   */
  void updateStarvedApps() {
    // 获取所有有未满足需求的应用
    TreeSet<FSAppAttempt> appsWithDemand = fetchAppsWithDemand(false);

    // 处理公平份额饥饿
    Resource fairShareStarvation = updateStarvedAppsFairshare(appsWithDemand);

    // 计算队列总最小份额饥饿
    Resource minShareStarvation = minShareStarvation();

    // 扣除已被公平份额饥饿覆盖的部分，得到剩余需要分配的最小份额饥饿
    Resources.subtractFromNonNegative(minShareStarvation, fairShareStarvation);

    // 将剩余最小份额饥饿分配给各个应用
    updateStarvedAppsMinshare(appsWithDemand, minShareStarvation);
  }

  @Override
  public Resource getDemand() {
    return demand;
  }

  Resource getAmResourceUsage() {
    return amResourceUsage;
  }

  @Override
  public void updateDemand() {
    // 临时变量存储计算得到的总需求
    Resource tmpDemand = Resources.createResource(0);
    readLock.lock();
    try {
      // 累加所有可运行应用的需求
      for (FSAppAttempt sched : runnableApps) {
        sched.updateDemand();
        Resources.addTo(tmpDemand, sched.getDemand());
      }
      // 累加所有不可运行应用的需求
      for (FSAppAttempt sched : nonRunnableApps) {
        sched.updateDemand();
        Resources.addTo(tmpDemand, sched.getDemand());
      }
    } finally {
      readLock.unlock();
    }
    // 总需求不超过队列最大份额，做截断处理
    demand = Resources.componentwiseMin(tmpDemand, getMaxShare());
    if (LOG.isDebugEnabled()) {
      LOG.debug("The updated demand for " + getName() + " is " + demand
          + "; the max is " + getMaxShare());
      LOG.debug("The updated fairshare for " + getName() + " is "
          + getFairShare());
    }
  }

  @Override
  public Resource assignContainer(FSSchedulerNode node) {
    Resource assigned = none();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Node " + node.getNodeName() + " offered to queue: " +
          getName() + " fairShare: " + getFairShare());
    }

    // 容器分配前置检查，不通过则直接返回
    if (!assignContainerPreCheck(node)) {
      return assigned;
    }

    // 按调度顺序遍历所有有需求的应用，尝试分配容器
    for (FSAppAttempt sched : fetchAppsWithDemand(true)) {
      // 应用将该节点列入黑名单，跳过
      if (SchedulerAppUtils.isPlaceBlacklisted(sched, node, LOG)) {
        continue;
      }
      // 应用尝试分配容器
      assigned = sched.assignContainer(node);

      // 判断是否完成分配或预约
      boolean isContainerAssignedOrReserved = !assigned.equals(none());
      boolean isContainerReserved =
                assigned.equals(FairScheduler.CONTAINER_RESERVED);

      // 分配或预约成功，中断循环返回
      if (isContainerAssignedOrReserved) {
        // 仅实际分配容器时打日志，预约不打日志
        if (!isContainerReserved && LOG.isDebugEnabled()) {
          LOG.debug("Assigned container in queue:{} container:{}",
              getName(), assigned);
        }
        break;
      }
    }
    return assigned;
  }

  /**
   * 获取所有有未满足需求的应用集合，按调度策略排序
   * @param assignment true表示用于容器分配流程，false表示用于抢占计算
   * @return 排序后的有需求应用集合
   */
  private TreeSet<FSAppAttempt> fetchAppsWithDemand(boolean assignment) {
    TreeSet<FSAppAttempt> pendingForResourceApps =
        new TreeSet<>(policy.getComparator());
    readLock.lock();
    try {
      // 遍历所有可运行应用
      for (FSAppAttempt app : runnableApps) {
        // 应用有未满足需求，且（用于分配 或 应用需要检查饥饿）则加入集合
        if (!Resources.isNone(app.getPendingDemand()) &&
            (assignment || app.shouldCheckForStarvation())) {
          pendingForResourceApps.add(app);
        }
      }
    } finally {
      readLock.unlock();
    }
    return pendingForResourceApps;
  }

  @Override
  public List<FSQueue> getChildQueues() {
    return EMPTY_LIST;
  }
  
  @Override
  public List<QueueUserACLInfo> getQueueUserAclInfo(UserGroupInformation user) {
    QueueUserACLInfo userAclInfo =
      recordFactory.newRecordInstance(QueueUserACLInfo.class);
    List<QueueACL> operations = new ArrayList<>();
    // 遍历所有队列操作权限，收集用户拥有的权限
    for (QueueACL operation : QueueACL.values()) {
      if (hasAccess(operation, user)) {
        operations.add(operation);
      }
    }

    userAclInfo.setQueueName(getQueueName());
    userAclInfo.setUserAcls(operations);
    return Collections.singletonList(userAclInfo);
  }
  
  private void setLastTimeAtMinShare(long lastTimeAtMinShare) {
    this.lastTimeAtMinShare = lastTimeAtMinShare;
  }

  @Override
  public int getNumRunnableApps() {
    readLock.lock();
    try {
      return runnableApps.size();
    } finally {
      readLock.unlock();
    }
  }

  int getNumNonRunnableApps() {
    readLock.lock();
    try {
      return nonRunnableApps.size();
    } finally {
      readLock.unlock();
    }
  }

  public int getNumPendingApps() {
    int numPendingApps = 0;
    readLock.lock();
    try {
      // 统计可运行列表中处于pending状态的应用
      for (FSAppAttempt attempt : runnableApps) {
        if (attempt.isPending()) {
          numPendingApps++;
        }
      }
      // 不可运行列表所有应用都算作pending
      numPendingApps += nonRunnableApps.size();
    } finally {
      readLock.un