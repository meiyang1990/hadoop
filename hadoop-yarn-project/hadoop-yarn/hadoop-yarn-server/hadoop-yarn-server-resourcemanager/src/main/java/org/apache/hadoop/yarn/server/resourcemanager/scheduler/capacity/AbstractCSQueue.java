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

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Sets;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.security.AccessRequest;
import org.apache.hadoop.yarn.security.AccessType;
import org.apache.hadoop.yarn.security.PrivilegedEntity;
import org.apache.hadoop.yarn.security.PrivilegedEntity.EntityType;
import org.apache.hadoop.yarn.security.YarnAuthorizationProvider;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueResourceQuotas;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceLimits;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceUsage;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivitiesManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.AbsoluteResourceType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.ContainerAllocationProposal;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.ResourceCommitRequest;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.SchedulerContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.SimpleCandidateNodeSet;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager.NO_LABEL;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.DOT;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.ResourceUnitCapacityType.PERCENTAGE;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.ResourceUnitCapacityType.WEIGHT;

/**
 * 文件说明：容量调度器抽象队列基类，提供所有队列类型共有的核心实现
 * 为容量调度器中不同类型的队列提供通用方法的默认实现，定义队列公共属性和基础逻辑
 */
public abstract class AbstractCSQueue implements CSQueue {
  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractCSQueue.class);
  // 队列分配设置（存储最小/最大容器分配限制）
  protected final QueueAllocationSettings queueAllocationSettings;
  // 父队列引用
  volatile CSQueue parent;
  // 队列路径对象（存储完整队列路径信息）
  protected final QueuePath queuePath;
  // 队列节点标签配置
  protected QueueNodeLabelsSettings queueNodeLabelsSettings;
  // 队列应用生命周期限制配置
  private volatile QueueAppLifetimeAndLimitSettings queueAppLifetimeSettings;
  // 队列抢占配置
  private CSQueuePreemptionSettings preemptionSettings;

  // 队列当前状态
  private volatile QueueState state = null;
  // 权限认证用队列实体对象
  protected final PrivilegedEntity queueEntity;

  // 资源计算器
  final ResourceCalculator resourceCalculator;
  // 支持的资源类型集合
  Set<String> resourceTypes;
  // 节点标签管理器
  final RMNodeLabelsManager labelManager;
  // 多节点排序策略类名
  private String multiNodeSortingPolicyClassName = null;

  // 队列访问控制列表，按访问类型存储
  Map<AccessType, AccessControlList> acls =
      new HashMap<AccessType, AccessControlList>();
  // 达到预留限额后是否继续搜索节点
  volatile boolean reservationsContinueLooking;

  // 存储队列各种容量信息（容量、最大容量、绝对容量等）
  QueueCapacities queueCapacities;
  // 队列资源使用追踪器
  CSQueueUsageTracker usageTracker;

  public enum CapacityConfigType {
    NONE, PERCENTAGE, ABSOLUTE_RESOURCE
  };

  protected CapacityConfigType capacityConfigType =
      CapacityConfigType.NONE;

  // 已配置的各标签对应容量向量
  protected Map<String, QueueCapacityVector> configuredCapacityVectors;
  // 已配置的各标签对应最大容量向量
  protected Map<String, QueueCapacityVector> configuredMaxCapacityVectors;

  private final RecordFactory recordFactory =
      RecordFactoryProvider.getRecordFactory(null);
  // 容量调度器队列上下文
  protected CapacitySchedulerQueueContext queueContext;
  // YARN权限认证提供者
  protected YarnAuthorizationProvider authorizer = null;

  // 调度活动日志管理器
  protected ActivitiesManager activitiesManager;

  // 读写锁-读锁
  protected ReentrantReadWriteLock.ReadLock readLock;
  // 读写锁-写锁
  protected ReentrantReadWriteLock.WriteLock writeLock;

  // 队列优先级
  volatile Priority priority = Priority.newInstance(0);
  // 队列用户权重
  private UserWeights userWeights = UserWeights.createEmpty();

  // 是否是动态创建队列
  private boolean dynamicQueue = false;

  /**
   * 构造抽象队列，初始化基础属性
   * @param queueContext 队列上下文
   * @param queueName 队列名称
   * @param parent 父队列
   * @param old 旧队列（用于重建队列时复用指标）
   */
  public AbstractCSQueue(CapacitySchedulerQueueContext queueContext, String queueName,
      CSQueue parent, CSQueue old) {
    this.parent = parent;
    this.queuePath = createQueuePath(parent, queueName);

    this.queueContext = queueContext;
    this.resourceCalculator = queueContext.getResourceCalculator();
    this.activitiesManager = queueContext.getActivitiesManager();
    this.labelManager = queueContext.getLabelManager();

    // 必须在父队列和队列名称设置后调用
    CSQueueMetrics metrics = old != null ?
        (CSQueueMetrics) old.getMetrics() :
        CSQueueMetrics.forQueue(getQueuePath(), parent,
            queueContext.getConfiguration().getEnableUserMetrics(),
            queueContext.getConfiguration());
    this.usageTracker = new CSQueueUsageTracker(metrics);

    this.queueCapacities = new QueueCapacities(parent == null);
    this.queueAllocationSettings = new QueueAllocationSettings(queueContext.getMinimumAllocation());

    this.queueEntity = new PrivilegedEntity(EntityType.QUEUE, getQueuePath());

    this.resourceTypes = new HashSet<>();
    // 初始化所有绝对资源类型
    for (AbsoluteResourceType type : AbsoluteResourceType.values()) {
      this.resourceTypes.add(type.toString().toLowerCase());
    }

    // 初始化读写锁
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    this.readLock = lock.readLock();
    this.writeLock = lock.writeLock();

    LOG.debug("Initialized {}: name={}, fullname={}", this.getClass().getSimpleName(),
        queueName, getQueuePath());
  }

  /**
   * 创建队列路径对象，拼接完整路径
   */
  private static QueuePath createQueuePath(CSQueue parent, String queueName) {
    if (parent == null) {
      return new QueuePath(null, queueName);
    }
    return new QueuePath(parent.getQueuePath(), queueName);
  }

  /**
   * 从配置加载容量和权重值
   */
  protected void setupConfigurableCapacities() {
    CSQueueUtils.loadCapacitiesByLabelsFromConf(queuePath, queueCapacities,
        queueContext.getConfiguration(), this.queueNodeLabelsSettings.getConfiguredNodeLabels());
  }

  @Override
  public String getQueuePath() {
    return queuePath.getFullPath();
  }

  @Override
  public QueuePath getQueuePathObject() {
    return this.queuePath;
  }

  @Override
  public float getCapacity() {
    return queueCapacities.getCapacity();
  }

  @Override
  public float getAbsoluteCapacity() {
    return queueCapacities.getAbsoluteCapacity();
  }

  @Override
  public float getAbsoluteMaximumCapacity() {
    return queueCapacities.getAbsoluteMaximumCapacity();
  }

  @Override
  public float getAbsoluteUsedCapacity() {
    return queueCapacities.getAbsoluteUsedCapacity();
  }

  @Override
  public float getMaximumCapacity() {
    return queueCapacities.getMaximumCapacity();
  }

  @Override
  public float getUsedCapacity() {
    return queueCapacities.getUsedCapacity();
  }

  @Override
  public Resource getUsedResources() {
    return usageTracker.getQueueUsage().getUsed();
  }

  public int getNumContainers() {
    return usageTracker.getNumContainers();
  }

  @Override
  public QueueState getState() {
    return state;
  }

  @Override
  public CSQueueMetrics getMetrics() {
    return usageTracker.getMetrics();
  }

  @Override
  public String getQueueShortName() {
    return queuePath.getLeafName();
  }

  @Override
  public String getQueueName() {
    return this.queuePath.getLeafName();
  }

  @Override
  public CSQueue getParent() {
    return parent;
  }

  @Override
  public void setParent(CSQueue newParentQueue) {
    this.parent = newParentQueue;
    getMetrics().setParentQueue(newParentQueue);
  }

  @Override
  public PrivilegedEntity getPrivilegedEntity() {
    return queueEntity;
  }

  public CapacitySchedulerQueueContext getQueueContext() {
    return queueContext;
  }

  public Set<String> getAccessibleNodeLabels() {
    return queueNodeLabelsSettings.getAccessibleNodeLabels();
  }

  /**
   * 检查用户是否拥有指定队列ACL权限
   * @param acl 需要检查的访问类型
   * @param user 用户UGI信息
   * @return true 用户有权限，false 无权限
   */
  @Override
  public boolean hasAccess(QueueACL acl, UserGroupInformation user) {
    return authorizer.checkPermission(
        new AccessRequest(queueEntity, user, SchedulerUtils.toAccessType(acl),
            null, null, Server.getRemoteAddress(), null));
  }

  /**
   * 设置空节点标签的最大容量
   * @param maximumCapacity 新的最大容量
   */
  @VisibleForTesting
  void setMaxCapacity(float maximumCapacity) {
    internalSetMaximumCapacity(maximumCapacity, NO_LABEL);
  }

  /**
   * 设置指定节点标签的最大容量
   * @param maximumCapacity 新的最大容量
   */
  void setMaxCapacity(String nodeLabel, float maximumCapacity) {
    internalSetMaximumCapacity(maximumCapacity, nodeLabel);
  }

  /**
   * 内部方法：设置指定标签的最大容量
   */
  private void internalSetMaximumCapacity(float maximumCapacity, String nodeLabel) {
    writeLock.lock();
    try {
      // 合法性检查：最大容量不得小于容量
      CSQueueUtils.checkMaxCapacity(this.queuePath,
          queueCapacities.getCapacity(nodeLabel), maximumCapacity);
      // 计算绝对最大容量（相对于集群总容量）
      float absMaxCapacity = CSQueueUtils.computeAbsoluteMaximumCapacity(
          maximumCapacity, parent);
      // 合法性检查：绝对最大容量不得小于绝对容量
      CSQueueUtils.checkAbsoluteCapacity(this.queuePath,
          queueCapacities.getAbsoluteCapacity(nodeLabel), absMaxCapacity);

      // 更新容量信息
      queueCapacities.setMaximumCapacity(maximumCapacity);
      queueCapacities.setAbsoluteMaximumCapacity(absMaxCapacity);
      configuredMaxCapacityVectors.put(NO_LABEL, QueueCapacityVector.of(
                    maximumCapacity * 100, PERCENTAGE));
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public String getDefaultNodeLabelExpression() {
    return this.queueNodeLabelsSettings.getDefaultLabelExpression();
  }

  /**
   * 初始化基于配置的队列各项属性
   * @param clusterResource 集群总资源
   * @throws IOException 配置不合法时抛出异常
   */
  protected void setupQueueConfigs(Resource clusterResource) throws
      IOException {

    writeLock.lock();
    try {
      CapacitySchedulerConfiguration configuration = queueContext.getConfiguration();
      // 加载访问控制列表
      this.acls = configuration.getAcls(getQueuePathObject());

      // 动态队列或自动创建队列处理模板配置
      if (isDynamicQueue() || this instanceof AbstractAutoCreatedLeafQueue) {
        parseAndSetDynamicTemplates();
        setDynamicQueueACLProperties();
      }

      // 加载并设置节点标签配置
      this.queueNodeLabelsSettings = new QueueNodeLabelsSettings(configuration, parent,
          queuePath, queueContext.getQueueManager().getConfiguredNodeLabelsForAllQueues());

      // 初始化队列容量
      setupConfigurableCapacities();
      // 更新绝对容量（基于父队列容量计算）
      updateAbsoluteCapacities();

      // 加载配置的最小/最大资源限制
      updateConfigurableResourceLimits(clusterResource);

      // 根据全局和父队列设置，初始化队列最大分配限制
      this.queueAllocationSettings.setupMaximumAllocation(configuration, getQueuePathObject(),
          parent);

      // 根据之前状态、配置状态和父队列状态，确定当前队列状态
      QueueStateHelper.setQueueState(this);

      // 初始化权限认证提供者
      authorizer = YarnAuthorizationProvider.getInstance(configuration);

      // 从层级继承用户权重
      this.userWeights = getUserWeightsFromHierarchy();

      // 加载预留配置：达到预留限额后是否继续搜索节点
      this.reservationsContinueLooking =
          configuration.getReservationContinueLook();

      // 解析配置的容量向量
      this.configuredCapacityVectors = configuration
          .parseConfiguredResourceVector(queuePath,
              this.queueNodeLabelsSettings.getConfiguredNodeLabels());
      // 解析配置的最大容量向量
      this.configuredMaxCapacityVectors = configuration
          .parseConfiguredMaximumCapacityVector(queuePath,
              this.queueNodeLabelsSettings.getConfiguredNodeLabels(),
              QueueCapacityVector.newInstance());

      // 遍历所有配置的节点标签，处理特殊队列的容量向量覆盖
      for (final String label : queueNodeLabelsSettings.getConfiguredNodeLabels()) {
        // 为特殊队列覆盖容量向量：
        // 1. 没有通过模板配置容量向量的动态队列
        // 2. 预留队列和计划队列，重启时需要保留容量设置
        overrideCapacityVectorsForSpecialQueues(label);

        // 混合容量类型时重新调整权重，所有资源都为同一权重时设置队列权重
        final QueueCapacityVector capacityVector = configuredCapacityVectors.get(label);
        final Set<QueueCapacityVector.ResourceUnitCapacityType> definedCapacityTypes =
            capacityVector.getDefinedCapacityTypes();
        if (definedCapacityTypes.size() == 1 && definedCapacityTypes.iterator().next() == WEIGHT) {
          Set<Double> weights = new HashSet<>();
          for (String resourceName : capacityVector.getResourceNames()) {
            weights.add(capacityVector.getResource(resourceName).getResourceValue());
          }
          if (weights.size() == 1) {
            queueCapacities.setWeight(label, weights.iterator().next().floatValue());
          }
        }
      }

      // 更新容量配置类型（百分比/绝对资源）
      updateCapacityConfigType();

      // 更新队列统计指标
      CSQueueUtils.updateQueueStatistics(resourceCalculator,