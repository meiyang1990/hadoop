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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractUsersManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceUsage;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * 容量调度器叶队列的用户管理器，负责跟踪队列中所有用户的资源使用、应用状态，并计算用户资源限额
 * 实现了用户级资源限制，保障队列内多个用户的公平资源分配
 */
@Private
public class UsersManager implements AbstractUsersManager {

  private static final Logger LOG =
      LoggerFactory.getLogger(UsersManager.class);

  /*
   * Member declaration for UsersManager class.
   */
  // 所属的叶队列
  private final AbstractLeafQueue lQueue;
  // 节点标签管理器，用于获取分区资源总量
  private final RMNodeLabelsManager labelManager;
  // 资源计算器，用于资源大小计算和比较
  private final ResourceCalculator resourceCalculator;
  // 存储所有用户对象，key为用户名
  private Map<String, User> users = new ConcurrentHashMap<>();

  // 活跃用户总资源使用统计
  private ResourceUsage totalResUsageForActiveUsers = new ResourceUsage();
  // 非活跃用户总资源使用统计
  private ResourceUsage totalResUsageForNonActiveUsers = new ResourceUsage();
  // 活跃用户集合（有运行中应用的用户）
  private Set<String> activeUsersSet = new HashSet<String>();
  // 非活跃用户集合（仅存在挂起应用或无运行应用的用户）
  private Set<String> nonActiveUsersSet = new HashSet<String>();

  // 队列所有用户资源使用率总和，按节点标签分区存储
  private UsageRatios qUsageRatios;

  // 用户状态版本号，用于判断用户限额缓存是否需要重新计算
  // 用户数量变化后版本号递增，缓存版本不匹配则重新计算
  private long latestVersionOfUsersState = 0;
  // 活跃用户限额缓存的版本，按节点分区、调度模式存储
  private Map<String, Map<SchedulingMode, Long>> localVersionOfActiveUsersState =
      new HashMap<String, Map<SchedulingMode, Long>>();
  // 所有用户限额缓存的版本，按节点分区、调度模式存储
  private Map<String, Map<SchedulingMode, Long>> localVersionOfAllUsersState =
      new HashMap<String, Map<SchedulingMode, Long>>();

  // 配置的单个用户最大资源占队列容量百分比
  private volatile float userLimit;
  // 配置的单个用户资源限额系数，用于上限计算
  private volatile float userLimitFactor;

  private WriteLock writeLock;
  private ReadLock readLock;

  // 队列指标统计
  private final QueueMetrics metrics;
  // 活跃用户总数（有运行中应用的用户）
  private AtomicInteger activeUsers = new AtomicInteger(0);
  // 仅存在挂起应用的活跃用户数
  private AtomicInteger activeUsersWithOnlyPendingApps = new AtomicInteger(0);
  // 用户拥有的应用集合，key为用户名
  private Map<String, Set<ApplicationId>> usersApplications =
      new HashMap<String, Set<ApplicationId>>();

  // 预计算的活跃用户资源限额缓存，按节点分区、调度模式存储
  @VisibleForTesting
  Map<String, Map<SchedulingMode, Resource>> preComputedActiveUserLimit =
      new HashMap<>();
  // 预计算的所有用户资源限额缓存，按节点分区、调度模式存储
  @VisibleForTesting
  Map<String, Map<SchedulingMode, Resource>> preComputedAllUserLimit =
      new HashMap<>();

  // 活跃用户权重总和，用于加权均分计算
  private float activeUsersTimesWeights = 0.0f;
  // 所有用户权重总和，用于加权均分计算
  private float allUsersTimesWeights = 0.0f;

  /**
   * 存储按节点标签分区的用户资源使用率总和，线程安全
   */
  static private class UsageRatios {
    // 按节点标签存储使用率总和
    private Map<String, Float> usageRatios;
    private ReadLock readLock;
    private WriteLock writeLock;

    public UsageRatios() {
      ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
      readLock = lock.readLock();
      writeLock = lock.writeLock();
      usageRatios = new HashMap<String, Float>();
    }

    /**
     * 增加指定分区的使用率总和
     * @param label 节点分区标签
     * @param delta 增量值
     */
    private void incUsageRatio(String label, float delta) {
      writeLock.lock();
      try {
        float usage = 0f;
        if (usageRatios.containsKey(label)) {
          usage = usageRatios.get(label);
        }
        usage += delta;
        usageRatios.put(label, usage);
      } finally {
        writeLock.unlock();
      }
    }

    /**
     * 获取指定分区的使用率总和
     * @param label 节点分区标签
     * @return 使用率总和
     */
    private float getUsageRatio(String label) {
      readLock.lock();
      try {
        Float f = usageRatios.get(label);
        if (null == f) {
          return 0.0f;
        }
        return f;
      } finally {
        readLock.unlock();
      }
    }

    /**
     * 设置指定分区的使用率总和
     * @param label 节点分区标签
     * @param ratio 使用率值
     */
    private void setUsageRatio(String label, float ratio) {
      writeLock.lock();
      try {
        usageRatios.put(label, ratio);
      } finally {
        writeLock.unlock();
      }
    }
  } /* End of UserRatios class */

  /**
   * 单个用户的资源使用和应用状态存储，记录用户的资源使用、应用数量、资源限额等信息
   */
  @VisibleForTesting
  public static class User {
    // 用户资源使用统计
    ResourceUsage userResourceUsage = new ResourceUsage();
    // 用户名
    String userName = null;
    // 计算得到的用户资源限额
    volatile Resource userResourceLimit = Resource.newInstance(0, 0);
    // 用户挂起应用数量
    private volatile AtomicInteger pendingApplications = new AtomicInteger(0);
    // 用户活跃（运行中）应用数量
    private volatile AtomicInteger activeApplications = new AtomicInteger(0);

    // 用户各分区资源使用率
    private UsageRatios userUsageRatios = new UsageRatios();
    private WriteLock writeLock;
    // 用户权重，用于加权资源分配
    private float weight;

    public User(String name) {
      ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
      // Nobody uses read-lock now, will add it when necessary
      writeLock = lock.writeLock();

      this.userName = name;
    }

    public ResourceUsage getResourceUsage() {
      return userResourceUsage;
    }

    /**
     * 重置并更新用户资源使用率，返回相对于队列总使用率的增量
     * @param resourceCalculator 资源计算器
     * @param resource 分区总资源
     * @param nodePartition 节点分区
     * @return 增量值
     */
    public float setAndUpdateUsageRatio(ResourceCalculator resourceCalculator,
        Resource resource, String nodePartition) {
      writeLock.lock();
      try {
        userUsageRatios.setUsageRatio(nodePartition, 0);
        return updateUsageRatio(resourceCalculator, resource, nodePartition);
      } finally {
        writeLock.unlock();
      }
    }

    /**
     * 更新用户资源使用率，返回相对于队列总使用率的增量
     * @param resourceCalculator 资源计算器
     * @param resource 分区总资源
     * @param nodePartition 节点分区
     * @return 增量值
     */
    public float updateUsageRatio(ResourceCalculator resourceCalculator,
        Resource resource, String nodePartition) {
      writeLock.lock();
      try {
        float delta;
        float newRatio = Resources.ratio(resourceCalculator,
            getUsed(nodePartition), resource);
        delta = newRatio - userUsageRatios.getUsageRatio(nodePartition);
        userUsageRatios.setUsageRatio(nodePartition, newRatio);
        return delta;
      } finally {
        writeLock.unlock();
      }
    }

    public Resource getUsed() {
      return userResourceUsage.getUsed();
    }

    public Resource getAllUsed() {
      return userResourceUsage.getAllUsed();
    }

    public Resource getUsed(String label) {
      return userResourceUsage.getUsed(label);
    }

    public int getPendingApplications() {
      return pendingApplications.get();
    }

    public int getActiveApplications() {
      return activeApplications.get();
    }

    public Resource getConsumedAMResources() {
      return userResourceUsage.getAMUsed();
    }

    public Resource getConsumedAMResources(String label) {
      return userResourceUsage.getAMUsed(label);
    }

    public int getTotalApplications() {
      return getPendingApplications() + getActiveApplications();
    }

    /**
     * 提交新应用，增加挂起应用计数
     */
    public void submitApplication() {
      pendingApplications.incrementAndGet();
    }

    /**
     * 激活应用，从挂起转为活跃
     */
    public void activateApplication() {
      pendingApplications.decrementAndGet();
      activeApplications.incrementAndGet();
    }

    /**
     * 完成应用，减少对应状态应用计数
     * @param wasActive 应用是否为活跃状态
     */
    public void finishApplication(boolean wasActive) {
      if (wasActive) {
        activeApplications.decrementAndGet();
      } else {
        pendingApplications.decrementAndGet();
      }
    }

    public Resource getUserResourceLimit() {
      return userResourceLimit;
    }

    public void setUserResourceLimit(Resource userResourceLimit) {
      this.userResourceLimit = userResourceLimit;
    }

    public String getUserName() {
      return this.userName;
    }

    @VisibleForTesting
    public void setResourceUsage(ResourceUsage resourceUsage) {
      this.userResourceUsage = resourceUsage;
    }

    /**
     * @return the weight
     */
    public float getWeight() {
      return weight;
    }

    /**
     * @param weight the weight to set
     */
    public void setWeight(float weight) {
      this.weight = weight;
    }
  } /* End of User class */

  /**
   * 构造用户管理器，绑定所属叶队列和依赖组件
   *
   * @param metrics 队列指标统计
   * @param lQueue 所属叶队列
   * @param labelManager 节点标签管理器
   * @param resourceCalculator 资源计算器
   */
  public UsersManager(QueueMetrics metrics, AbstractLeafQueue lQueue,
      RMNodeLabelsManager labelManager, ResourceCalculator resourceCalculator) {
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    this.lQueue = lQueue;
    this.labelManager = labelManager;
    this.resourceCalculator = resourceCalculator;
    this.qUsageRatios = new UsageRatios();
    this.metrics = metrics;

    this.writeLock = lock.writeLock();
    this.readLock = lock.readLock();
  }

  /**
   * 获取配置的单用户资源占比上限百分比
   * @return 用户资源限制百分比
   */
  public float getUserLimit() {
    return userLimit;
  }

  /**
   * 设置配置的单用户资源占比上限百分比
   * @param userLimit 用户资源限制百分比
   */
  public void setUserLimit(float userLimit) {
    this.userLimit = userLimit;
  }

  /**
   * 获取配置的单用户资源限额系数
   * @return 用户资源限额系数
   */
  public float getUserLimitFactor() {
    return userLimitFactor;
  }

  /**
   * 设置配置的单用户资源限额系数
   * @param userLimitFactor 用户资源限额系数
   */
  public void setUserLimitFactor(float userLimitFactor) {
    this.userLimitFactor = userLimitFactor;
  }

  @VisibleForTesting
  public float getUsageRatio(String label) {
    return qUsageRatios.getUsageRatio(label);
  }

  /**
   * 标记用户状态已变更，触发用户限额缓存重新计算
   * 处理版本号溢出问题，溢出后重置为0
   */
  public void userLimitNeedsRecompute() {

    // If latestVersionOfUsersState is negative due to overflow, ideally we need
    // to reset it. This method is invoked from UsersManager and LeafQueue and
    // all is happening within write/readLock. Below logic can help to set 0.
    writeLock.lock();
    try {

      long value = ++latestVersionOfUsersState;
      if (value < 0) {
        latestVersionOfUsersState = 0;
      }
    } finally {
      writeLock.unlock();
    }
  }

  /*
   * Get all users of queue.
   */
  public Map<String, User> getUsers() {
    return users;
  }

  /**
   * 根据用户名获取用户对象
   *
   * @param userName 用户名
   * @return 用户对象，不存在返回null
   */
  public User getUser(String userName) {
    return users.get(userName);
  }

  /**
   * 移除指定用户
   *
   * @param userName 用户名
   */
  public void removeUser(String userName) {
    writeLock.lock();
    try {
      this.users.remove(userName);

      // 同时从活跃/非活跃集合中移除
      activeUsersSet.remove(userName);
      nonActiveUsersSet.remove(userName);
      // 重新计算权重总和
      activeUsersTimesWeights = sumActiveUsersTimesWeights();
      allUsersTimesWeights = sumAllUsersTimesWeights();
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * 获取用户对象，不存在则创建添加
   *
   * @param userName 用户名
   * @return 用户对象
   */
  public User getUserAndAddIfAbsent(String userName) {
    writeLock.lock();
    try {
      User u = getUser(userName);
      if (null == u) {
        u = new User(userName);
        addUser(userName, u);

        // 新用户默认添加到非活跃列表，跟踪资源使用
        if (!nonActiveUsersSet.contains(userName)) {
          nonActiveUsersSet.add(userName);
        }
      }
      return u;
    } finally {
      writeLock.unlock();
    }
  }

  /*
   * Add a new user
   */
  private void addUser(String userName, User user) {
    this.users.put(userName, user);
    // 从队列配置加载用户权重
    user.setWeight(getUserWeightFromQueue(userName));
    // 重新计算所有用户权重总和
    allUsersTimesWeights = sumAllUsersTimesWeights();
  }

  /**
   * 获取队列所有用户的信息列表，用于UI展示
   * @return 用户信息列表
   */
  public ArrayList<UserInfo> getUsersInfo() {
    readLock.lock();
    try {
      ArrayList<UserInfo> usersToReturn = new ArrayList<UserInfo>();
      for (Map.Entry<String, User> entry : getUsers().entrySet()) {
        User user = entry.getValue();
        usersToReturn.add(
            new UserInfo(entry.getKey(), Resources.clone(user.getAllUsed()),
                user.getActiveApplications(), user.getPendingApplications(),
                Resources.clone(user.getConsumedAMResources()),
                Resources.clone(user.getUserResourceLimit()),
                user.getResourceUsage(), user.getWeight(),
                activeUsersSet.contains(user.userName)));
      }
      return usersToReturn;
    } finally