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
package org.apache.hadoop.yarn.server.resourcemanager.webapp.dao;

import java.util.ArrayList;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceUsage;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AbstractLeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity
    .AutoCreatedLeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.UserInfo;

/**
 * 容量调度器叶子队列信息数据访问对象，封装叶子队列的统计信息和配置，供Web UI展示
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class CapacitySchedulerLeafQueueInfo extends CapacitySchedulerQueueInfo {

  // 活跃应用数量
  protected int numActiveApplications;
  // 待运行应用数量
  protected int numPendingApplications;
  // 已分配容器数量
  protected int numContainers;
  // 队列最大允许应用数
  protected int maxApplications;
  // 单用户最大允许应用数
  protected int maxApplicationsPerUser;
  // 用户资源限制比例
  protected float userLimit;
  // 用户信息集合，用于XML层级展示
  protected UsersInfo users;
  // 用户资源限制系数
  protected float userLimitFactor;
  // 配置的AM资源最大占比
  protected float configuredMaxAMResourceLimit;
  // AM资源限额
  protected ResourceInfo AMResourceLimit;
  // 已使用AM资源量
  protected ResourceInfo usedAMResource;
  // 用户AM资源限额
  protected ResourceInfo userAMResourceLimit;
  // 是否禁用抢占
  protected boolean preemptionDisabled;
  // 是否禁用队列内抢占
  protected boolean intraQueuePreemptionDisabled;
  // 默认应用优先级
  protected int defaultPriority;
  // 是否为自动创建的叶子队列
  protected boolean isAutoCreatedLeafQueue;
  // 最大应用生命周期
  protected long maxApplicationLifetime;
  // 默认应用生命周期
  protected long defaultApplicationLifetime;

  @XmlTransient
  // 排序策略显示名称
  protected String orderingPolicyDisplayName;

  CapacitySchedulerLeafQueueInfo() {
  }

  /**
   * 构造方法，从叶子队列对象提取信息构建DAO对象
   * @param cs 容量调度器实例
   * @param q 抽象叶子队列对象
   */
  CapacitySchedulerLeafQueueInfo(CapacityScheduler cs, AbstractLeafQueue q) {
    super(cs, q);
    numActiveApplications = q.getNumActiveApplications();
    numPendingApplications = q.getNumPendingApplications();
    numContainers = q.getNumContainers();
    maxApplications = q.getMaxApplications();
    maxApplicationsPerUser = q.getMaxApplicationsPerUser();
    userLimit = q.getUserLimit();
    users = new UsersInfo(q.getUsersManager().getUsersInfo());
    userLimitFactor = q.getUserLimitFactor();
    configuredMaxAMResourceLimit = q.getMaxAMResourcePerQueuePercent();
    AMResourceLimit = new ResourceInfo(q.getAMResourceLimit());
    usedAMResource = new ResourceInfo(q.getQueueResourceUsage().getAMUsed());
    preemptionDisabled = q.getPreemptionDisabled();
    intraQueuePreemptionDisabled = q.getIntraQueuePreemptionDisabled();
    orderingPolicyDisplayName = q.getOrderingPolicy().getInfo();
    orderingPolicyInfo = q.getOrderingPolicy().getConfigName();
    defaultPriority = q.getDefaultApplicationPriority().getPriority();
    ArrayList<UserInfo> usersList = users.getUsersList();
    // 无用户时使用队列AM限额，否则取第一个用户的AM限额
    if (usersList.isEmpty()) {
      // If no users are present, consider AM Limit for that queue.
      userAMResourceLimit = resources.getPartitionResourceUsageInfo(
          RMNodeLabelsManager.NO_LABEL).getAMLimit();
    } else {
      userAMResourceLimit = usersList.get(0).getResourceUsageInfo()
          .getPartitionResourceUsageInfo(RMNodeLabelsManager.NO_LABEL)
          .getAMLimit();
    }

    // 判断是否为自动创建的叶子队列
    if ( q instanceof AutoCreatedLeafQueue) {
      isAutoCreatedLeafQueue = true;
    }
    defaultApplicationLifetime = q.getDefaultApplicationLifetime();
    maxApplicationLifetime = q.getMaximumApplicationLifetime();
  }

  @Override
  /**
   * 填充队列资源使用信息
   * @param queueResourceUsage 队列资源使用对象
   */
  protected void populateQueueResourceUsage(ResourceUsage queueResourceUsage) {
    resources = new ResourcesInfo(queueResourceUsage);
  }

  @Override
  /**
   * 填充队列容量信息
   * @param queue 队列对象
   */
  protected void populateQueueCapacities(CSQueue queue) {
    capacities = new QueueCapacitiesInfo(queue, true);
  }

  public int getNumActiveApplications() {
    return numActiveApplications;
  }

  public int getNumPendingApplications() {
    return numPendingApplications;
  }

  public int getNumContainers() {
    return numContainers;
  }

  public int getMaxApplications() {
    return maxApplications;
  }

  public int getMaxApplicationsPerUser() {
    return maxApplicationsPerUser;
  }

  public float getUserLimit() {
    return userLimit;
  }

  //Placing here because of JERSEY-1199
  public UsersInfo getUsers() {
    return users;
  }

  public float getUserLimitFactor() {
    return userLimitFactor;
  }

  public float getConfiguredMaxAMResourceLimit() {
    return configuredMaxAMResourceLimit;
  }

  public ResourceInfo getAMResourceLimit() {
    return AMResourceLimit;
  }

  public ResourceInfo getUsedAMResource() {
    return usedAMResource;
  }

  public ResourceInfo getUserAMResourceLimit() {
    return userAMResourceLimit;
  }

  public boolean getPreemptionDisabled() {
    return preemptionDisabled;
  }

  public boolean getIntraQueuePreemptionDisabled() {
    return intraQueuePreemptionDisabled;
  }

  public String getOrderingPolicyDisplayName() {
    return orderingPolicyDisplayName;
  }

  public int getDefaultApplicationPriority() {
    return defaultPriority;
  }

  public boolean isAutoCreatedLeafQueue() {
    return isAutoCreatedLeafQueue;
  }

  public long getDefaultApplicationLifetime() {
    return defaultApplicationLifetime;
  }

  public long getMaxApplicationLifetime() {
    return maxApplicationLifetime;
  }

}