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
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ActiveUsersManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;

/**
 * 公平调度器中的父队列实现，负责管理子队列的资源分配、调度和状态更新
 */
@Private
@Unstable
public class FSParentQueue extends FSQueue {
  private static final Logger LOG = LoggerFactory.getLogger(
      FSParentQueue.class.getName());

  private final List<FSQueue> childQueues = new ArrayList<>();
  private Resource demand = Resources.createResource(0);
  private int runnableApps;

  private ReadWriteLock rwLock = new ReentrantReadWriteLock();
  private Lock readLock = rwLock.readLock();
  private Lock writeLock = rwLock.writeLock();

  /**
   * 构造父队列实例
   * @param name 队列名称
   * @param scheduler 公平调度器实例
   * @param parent 父队列，root队列父队列为null
   */
  public FSParentQueue(String name, FairScheduler scheduler,
      FSParentQueue parent) {
    super(name, scheduler, parent);
  }

  @Override
  public Resource getMaximumContainerAllocation() {
    // root队列直接返回自身配置
    if (getName().equals("root")) {
      return maxContainerAllocation;
    }
    // 当前未配置最大分配且存在父队列，继承父队列配置
    if (maxContainerAllocation.equals(Resources.unbounded())
        && getParent() != null) {
      return getParent().getMaximumContainerAllocation();
    } else {
      return maxContainerAllocation;
    }
  }

  /**
   * 添加子队列，写锁保护并发安全
   * @param child 要添加的子队列
   */
  void addChildQueue(FSQueue child) {
    writeLock.lock();
    try {
      childQueues.add(child);
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * 移除子队列，写锁保护并发安全
   * @param child 要移除的子队列
   */
  void removeChildQueue(FSQueue child) {
    writeLock.lock();
    try {
      childQueues.remove(child);
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  void updateInternal() {
    readLock.lock();
    try {
      // 根据当前父队列公平份额，计算所有子队列的即时公平份额
      policy.computeShares(childQueues, getFairShare());
      for (FSQueue childQueue : childQueues) {
        // 更新指标系统中的公平份额
        childQueue.getMetrics().setFairShare(childQueue.getFairShare());
        // 递归更新子队列内部
        childQueue.updateInternal();
      }
    } finally {
      readLock.unlock();
    }
  }

  /**
   * 重新计算所有子队列的稳态公平份额，稳态份额是基于权重计算的理论份额
   */
  void recomputeSteadyShares() {
    readLock.lock();
    try {
      // 根据当前父队列稳态份额，计算所有子队列的稳态公平份额
      policy.computeSteadyShares(childQueues, getSteadyFairShare());
      for (FSQueue childQueue : childQueues) {
        // 更新指标系统中的稳态公平份额
        childQueue.getMetrics()
            .setSteadyFairShare(childQueue.getSteadyFairShare());
        // 递归重算子父队列的稳态份额
        if (childQueue instanceof FSParentQueue) {
          ((FSParentQueue) childQueue).recomputeSteadyShares();
        }
      }
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public Resource getDemand() {
    readLock.lock();
    try {
      // 返回需求资源副本，避免外部修改内部状态
      return Resource.newInstance(demand.getMemorySize(), demand.getVirtualCores());
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public void updateDemand() {
    // 遍历所有子队列累加计算总需求，最终截断到最大份额不超过自身maxShare
    writeLock.lock();
    try {
      demand = Resources.createResource(0);
      for (FSQueue childQueue : childQueues) {
        // 先更新子队列需求
        childQueue.updateDemand();
        Resource toAdd = childQueue.getDemand();
        demand = Resources.add(demand, toAdd);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Counting resource from " + childQueue.getName() + " " +
              toAdd + "; Total resource demand for " + getName() +
              " now " + demand);
        }
      }
      // 将总需求截断到不超过自身最大份额，避免需求超过队列上限
      demand = Resources.componentwiseMin(demand, getMaxShare());
    } finally {
      writeLock.unlock();
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("The updated demand for " + getName() + " is " + demand +
          "; the max is " + getMaxShare());
    }    
  }
  
  /**
   * 获取当前用户对该队列的ACL权限信息
   * @param user 对应用户
   * @return 该队列的用户ACL信息
   */
  private QueueUserACLInfo getUserAclInfo(UserGroupInformation user) {
    List<QueueACL> operations = new ArrayList<>();
    for (QueueACL operation : QueueACL.values()) {
      if (hasAccess(operation, user)) {
        operations.add(operation);
      } 
    }
    return QueueUserACLInfo.newInstance(getQueueName(), operations);
  }
  
  @Override
  public List<QueueUserACLInfo> getQueueUserAclInfo(UserGroupInformation user) {
    List<QueueUserACLInfo> userAcls = new ArrayList<>();
    
    // 添加当前队列自身的ACL信息
    userAcls.add(getUserAclInfo(user));
    
    // 递归添加所有子队列的ACL信息
    readLock.lock();
    try {
      for (FSQueue child : childQueues) {
        userAcls.addAll(child.getQueueUserAclInfo(user));
      }
    } finally {
      readLock.unlock();
    }
 
    return userAcls;
  }

  @Override
  public Resource assignContainer(FSSchedulerNode node) {
    Resource assigned = Resources.none();

    // 预检查：如果超过资源限制直接拒绝分配
    if (!assignContainerPreCheck(node)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Assign container precheck for queue " + getName() +
            " on node " + node.getNodeName() + " failed");
      }
      return assigned;
    }

    // 按调度策略排序子队列，仅持有当前父队列读锁
    // 不需要锁定每个子队列，空队列才能被删除，且分配和删除都需要全局调度锁，不会并发冲突
    TreeSet<FSQueue> sortedChildQueues = new TreeSet<>(policy.getComparator());
    readLock.lock();
    try {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Node " + node.getNodeName() + " offered to parent queue: " +
            getName() + " visiting " + childQueues.size() + " children");
      }
      // 将子队列添加到排序集合，按调度策略自动排序
      sortedChildQueues.addAll(childQueues);
      // 按优先级顺序遍历子队列分配容器
      for (FSQueue child : sortedChildQueues) {
        assigned = child.assignContainer(node);
        // 分配成功则跳出循环
        if (!Resources.equals(assigned, Resources.none())) {
          break;
        }
      }
    } finally {
      readLock.unlock();
    }
    return assigned;
  }

  @Override
  public List<FSQueue> getChildQueues() {
    readLock.lock();
    try {
      // 返回不可变副本，避免外部修改内部子队列列表
      return ImmutableList.copyOf(childQueues);
    } finally {
      readLock.unlock();
    }
  }

  /**
   * 增加可运行应用计数，写锁保护并发安全
   */
  void incrementRunnableApps() {
    writeLock.lock();
    try {
      runnableApps++;
    } finally {
      writeLock.unlock();
    }
  }
  
  /**
   * 减少可运行应用计数，写锁保护并发安全
   */
  void decrementRunnableApps() {
    writeLock.lock();
    try {
      runnableApps--;
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public int getNumRunnableApps() {
    readLock.lock();
    try {
      return runnableApps;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public boolean isEmpty() {
    readLock.lock();
    try {
      // 只要任意子队列非空，当前队列就非空
      for (FSQueue queue: childQueues) {
        if (!queue.isEmpty()) {
          return false;
        }
      }
    } finally {
      readLock.unlock();
    }
    return true;
  }

  @Override
  public void collectSchedulerApplications(
      Collection<ApplicationAttemptId> apps) {
    readLock.lock();
    try {
      // 递归收集所有子队列中的应用
      for (FSQueue childQueue : childQueues) {
        childQueue.collectSchedulerApplications(apps);
      }
    } finally {
      readLock.unlock();
    }
  }
  
  @Override
  public ActiveUsersManager getAbstractUsersManager() {
    // 所有应用都提交到叶子队列，父队列不会调用该方法，返回null
    return null;
  }

  @Override
  public void recoverContainer(Resource clusterResource,
      SchedulerApplicationAttempt schedulerAttempt, RMContainer rmContainer) {
    // TODO Auto-generated method stub
    
  }

  @Override
  protected void dumpStateInternal(StringBuilder sb) {
    // 将当前队列状态信息追加到字符串构建器
    sb.append("{Name: " + getName() +
        ", Weight: " + weights +
        ", Policy: " + policy.getName() +
        ", FairShare: " + getFairShare() +
        ", SteadyFairShare: " + getSteadyFairShare() +
        ", MaxShare: " + getMaxShare() +
        ", MinShare: " + minShare +
        ", ResourceUsage: " + getResourceUsage() +
        ", Demand: " + getDemand() +
        ", MaxAMShare: " + maxAMShare +
        ", Runnable: " + getNumRunnableApps() +
        "}");

    // 递归追加所有子队列状态
    for(FSQueue child : getChildQueues()) {
      sb.append(", ");
      child.dumpStateInternal(sb);
    }
  }
}