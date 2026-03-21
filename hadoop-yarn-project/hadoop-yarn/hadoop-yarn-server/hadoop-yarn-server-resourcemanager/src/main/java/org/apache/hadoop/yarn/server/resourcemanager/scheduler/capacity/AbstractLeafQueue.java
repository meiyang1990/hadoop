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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.util.Sets;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.security.AccessType;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.*;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivityDiagnosticConstant;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivitiesLogger;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivityState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt.AMState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.UsersManager.User;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.preemption.KillableContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.ContainerAllocationProposal;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.ResourceCommitRequest;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.SchedulerContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.CandidateNodeSet;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.CandidateNodeSetUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy.FifoOrderingPolicyForPendingApps;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy.IteratorSelector;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy.OrderingPolicy;
import org.apache.hadoop.yarn.server.utils.Lock;
import org.apache.hadoop.yarn.server.utils.Lock.NoLock;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.hadoop.yarn.util.resource.Resources;

import org.apache.hadoop.classification.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.getACLsForFlexibleAutoCreatedLeafQueue;

import static org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager.NO_LABEL;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueCapacityVector.ResourceUnitCapacityType.PERCENTAGE;

/**
 * 容量调度器叶队列抽象基类，叶队列是实际运行应用的队列，不可再嵌套子队列
 * 负责管理队列内应用生命周期、资源分配、用户资源限制等核心调度逻辑
 */
public class AbstractLeafQueue extends AbstractCSQueue {
  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractLeafQueue.class);

  private float absoluteUsedCapacity = 0.0f;

  // TODO the max applications should consider label
  protected int maxApplications;
  protected volatile int maxApplicationsPerUser;

  private float maxAMResourcePerQueuePercent;

  private volatile int nodeLocalityDelay;
  private volatile int rackLocalityAdditionalDelay;
  private volatile boolean rackLocalityFullReset;

  // 存储当前队列所有应用尝试上下文，键为应用尝试ID
  Map<ApplicationAttemptId, FiCaSchedulerApp> applicationAttemptMap =
      new ConcurrentHashMap<>();

  private Priority defaultAppPriorityPerQueue;

  private final OrderingPolicy<FiCaSchedulerApp> pendingOrderingPolicy;

  private volatile float minimumAllocationFactor;

  private final RecordFactory recordFactory =
      RecordFactoryProvider.getRecordFactory(null);

  private final UsersManager usersManager;

  // 缓存上一次集群资源，用于计算队列实际容量
  private Resource lastClusterResource = Resources.none();

  // 队列资源限制信息，用于计算应用空闲资源
  private final QueueResourceLimitsInfo queueResourceLimitsInfo =
      new QueueResourceLimitsInfo();

  private volatile ResourceLimits cachedResourceLimitsForHeadroom = null;

  private volatile OrderingPolicy<FiCaSchedulerApp> orderingPolicy = null;

  // Map<Partition, Map<SchedulingMode, Map<User, CachedUserLimit>>>
  // 非线程安全：仅最内层是ConcurrentMap
  @VisibleForTesting
  Map<String, Map<SchedulingMode, ConcurrentMap<String, CachedUserLimit>>>
      userLimitsCache = new HashMap<>();

  // 非线程安全，用户限制缓存当前版本号，用于缓存失效
  @VisibleForTesting
  long currentUserLimitCacheVersion = 0;

  // 记录所有忽略分区排他性的容器，用于抢占，key是容器分配所在的分区
  private Map<String, TreeSet<RMContainer>> ignorePartitionExclusivityRMContainers =
      new ConcurrentHashMap<>();

  // 优先级ACL配置列表
  List<AppPriorityACLGroup> priorityAcls =
      new ArrayList<AppPriorityACLGroup>();

  private final List<FiCaSchedulerApp> runnableApps = new ArrayList<>();
  private final List<FiCaSchedulerApp> nonRunnableApps = new ArrayList<>();

  /**
   * 构造叶队列，默认非动态队列.
   */
  public AbstractLeafQueue(CapacitySchedulerQueueContext queueContext,
      String queueName, CSQueue parent, CSQueue old) throws IOException {
    this(queueContext, queueName, parent, old, false);
  }

  /**
   * 构造叶队列，支持指定是否为动态创建队列.
   */
  public AbstractLeafQueue(CapacitySchedulerQueueContext queueContext,
      String queueName, CSQueue parent, CSQueue old, boolean isDynamic) throws
      IOException {
    super(queueContext, queueName, parent, old);
    setDynamicQueue(isDynamic);

    this.usersManager = new UsersManager(usageTracker.getMetrics(), this, labelManager,
        resourceCalculator);

    // 挂起应用FIFO排序策略仅需初始化一次
    this.pendingOrderingPolicy = new FifoOrderingPolicyForPendingApps<>();
  }

  @SuppressWarnings("checkstyle:nowhitespaceafter")
  /**
   * 初始化队列配置，加载所有调度相关参数.
   */
  protected void setupQueueConfigs(Resource clusterResource) throws
      IOException {
    writeLock.lock();
    try {
      CapacitySchedulerConfiguration configuration = queueContext.getConfiguration();
      super.setupQueueConfigs(clusterResource);

      this.lastClusterResource = clusterResource;

      this.cachedResourceLimitsForHeadroom = new ResourceLimits(
          clusterResource);

      // 初始化空闲资源信息，也用于计算AM资源限制
      // 初始化时所有队列可能未完成加载，因此使用绝对最大容量
      // 后续分配过程中会替换为更准确的绝对可用最大容量
      setQueueResourceLimitsInfo(clusterResource);

      setOrderingPolicy(
          configuration.<FiCaSchedulerApp>getAppOrderingPolicy(getQueuePathObject()));

      usersManager.setUserLimit(configuration.getUserLimit(getQueuePathObject()));
      usersManager.setUserLimitFactor(configuration.getUserLimitFactor(getQueuePathObject()));

      maxAMResourcePerQueuePercent =
          configuration.getMaximumApplicationMasterResourcePerQueuePercent(
              getQueuePathObject());

      maxApplications = configuration.getMaximumApplicationsPerQueue(getQueuePathObject());
      if (maxApplications < 0) {
        int maxGlobalPerQueueApps =
            configuration.getGlobalMaximumApplicationsPerQueue();
        if (maxGlobalPerQueueApps > 0) {
          maxApplications = maxGlobalPerQueueApps;
        }
      }

      priorityAcls = configuration.getPriorityAcls(getQueuePathObject(),
          configuration.getClusterLevelApplicationMaxPriority());

      Set<String> accessibleNodeLabels = this.queueNodeLabelsSettings.getAccessibleNodeLabels();
      // 验证默认标签表达式是否合法（队列有权访问所有包含的标签）
      if (!SchedulerUtils.checkQueueLabelExpression(accessibleNodeLabels,
          this.queueNodeLabelsSettings.getDefaultLabelExpression(), null)) {
        throw new IOException(
            "Invalid default label expression of " + " queue=" + getQueuePath()
                + " doesn't have permission to access all labels "
                + "in default label expression. labelExpression of resource request="
                + getDefaultNodeLabelExpressionStr() + ". Queue labels=" + (
                getAccessibleNodeLabels() == null ?
                    "" :
                    StringUtils
                        .join(getAccessibleNodeLabels().iterator(), ',')));
      }

      nodeLocalityDelay = configuration.getNodeLocalityDelay();
      rackLocalityAdditionalDelay = configuration
          .getRackLocalityAdditionalDelay();
      rackLocalityFullReset = configuration
          .getRackLocalityFullReset();

      // 最大最小分配改变后重新计算最小分配因子
      this.minimumAllocationFactor = Resources.ratio(resourceCalculator,
          Resources.subtract(
              queueAllocationSettings.getMaximumAllocation(),
              queueAllocationSettings.getMinimumAllocation()),
          queueAllocationSettings.getMaximumAllocation());

      StringBuilder aclsString = new StringBuilder();
      for (Map.Entry<AccessType, AccessControlList> e : acls.entrySet()) {
        aclsString.append(e.getKey() + ":" + e.getValue().getAclString());
      }

      StringBuilder labelStrBuilder = new StringBuilder();
      if (accessibleNodeLabels != null) {
        for (String nodeLabel : accessibleNodeLabels) {
          labelStrBuilder.append(nodeLabel).append(",");
        }
      }

      defaultAppPriorityPerQueue = Priority.newInstance(
          configuration.getDefaultApplicationPriorityConfPerQueue(getQueuePathObject()));

      // 验证叶队列用户权重配置合法性
      float queueUserLimit = Math.min(100.0f, configuration.getUserLimit(getQueuePathObject()));
      getUserWeights().validateForLeafQueue(queueUserLimit, getQueuePath());
      usersManager.updateUserWeights();

      LOG.info(
          "Initializing " + getQueuePath() + "\n" +
              getExtendedCapacityOrWeightString() + "\n"
              + "absoluteCapacity = " + queueCapacities.getAbsoluteCapacity()
              + " [= parentAbsoluteCapacity * capacity ]" + "\n"
              + "maxCapacity = " + queueCapacities.getMaximumCapacity()
              + " [= configuredMaxCapacity ]" + "\n" + "absoluteMaxCapacity = "
              + queueCapacities.getAbsoluteMaximumCapacity()
              + " [= 1.0 maximumCapacity undefined, "
              + "(parentAbsoluteMaxCapacity * maximumCapacity) / 100 otherwise ] \n"
              + "capacityVector = " + configuredCapacityVectors + "\n"
              + "maxCapacityVector = " + configuredMaxCapacityVectors + "\n"
              + "effectiveMinResource=" +
              getEffectiveCapacity(CommonNodeLabelsManager.NO_LABEL)
              + " effectiveMaxResource=" +
              getEffectiveMaxCapacity(CommonNodeLabelsManager.NO_LABEL)
              + "\n" + "userLimit = " + usersManager.getUserLimit()
              + " [= configuredUserLimit ]" + "\n" + "userLimitFactor = "
              + usersManager.getUserLimitFactor()
              + " [= configuredUserLimitFactor ]" + "\n" + "maxApplications = "
              + maxApplications
              + " [= configuredMaximumSystemApplicationsPerQueue or"
              + " (int)(configuredMaximumSystemApplications * absoluteCapacity)]"
              + "\n" + "maxApplicationsPerUser = " + maxApplicationsPerUser
              + " [= (int)(maxApplications * (userLimit / 100.0f) * "
              + "userLimitFactor) ]" + "\n"
              + "maxParallelApps = " + getMaxParallelApps() + "\n"
              + "usedCapacity = " +
              + queueCapacities.getUsedCapacity() + " [= usedResourcesMemory / "
              + "(clusterResourceMemory * absoluteCapacity)]" + "\n"
              + "absoluteUsedCapacity = " + absoluteUsedCapacity
              + " [= usedResourcesMemory / clusterResourceMemory]" + "\n"
              + "maxAMResourcePerQueuePercent = " + maxAMResourcePerQueuePercent
              + " [= configuredMaximumAMResourcePercent ]" + "\n"
              + "minimumAllocationFactor = " + minimumAllocationFactor
              + " [= (float)(maximumAllocationMemory - minimumAllocationMemory) / "
              + "maximumAllocationMemory ]" + "\n" + "maximumAllocation = "
              + queueAllocationSettings.getMaximumAllocation() +
              " [= configuredMaxAllocation ]" + "\n"
              + "numContainers = " + usageTracker.getNumContainers()
              + " [= currentNumContainers ]" + "\n" + "state = " + getState()
              + " [= configuredState ]" + "\n" + "acls = " + aclsString
              + " [= configuredAcls ]" + "\n"
              + "nodeLocalityDelay = " + nodeLocalityDelay + "\n"
              + "rackLocalityAdditionalDelay = "
              + rackLocalityAdditionalDelay + "\n"
              + "labels=" + labelStrBuilder.toString() + "\n"
              + "reservationsContinueLooking = "
              + reservationsContinueLooking + "\n" + "preemptionDisabled = "
              + getPreemptionDisabled() + "\n" + "defaultAppPriorityPerQueue = "
              + defaultAppPriorityPerQueue + "\npriority = " + priority
              + "\nmaxLifetime = " + getMaximumApplicationLifetime()
              + " seconds" + "\ndefaultLifetime = "
              + getDefaultApplicationLifetime() + " seconds");
    } finally {
      writeLock.unlock();
    }
  }

  private String getDefaultNodeLabelExpressionStr() {
    String defaultLabelExpression = queueNodeLabelsSettings.getDefaultLabelExpression();
    return defaultLabelExpression == null ? "" : defaultLabelExpression;
  }

  /**
   * Used only by tests.
   * @return minimumAllocationFactor.
   */
  @Private
  public float getMinimumAllocationFactor() {
    return minimumAllocationFactor;
  }

  /**
   * Used only by tests.
   * @return maxAMResourcePerQueuePercent.
   */
  @Private
  public float getMaxAMResourcePerQueuePercent() {
    return maxAMResourcePerQueuePercent;
  }

  /**
   * 获取队列最大允许运行应用总数.
   */
  public int getMaxApplications() {
    return maxApplications;
  }

  /**
   * 获取队列单个用户最大允许运行应用数.
   */
  public int getMaxApplicationsPerUser() {
    return maxApplicationsPerUser;
  }

  /**
   *
   * @return UsersManager instance.
   */
  public UsersManager getUsersManager() {
    return usersManager;
  }

  @Override
  public AbstractUsersManager getAbstractUsersManager() {
    return usersManager;
  }

  @Override
  public List<CSQueue> getChildQueues() {
    return null;
  }

  @Override
  public List<CSQueue> getChildQueuesByTryLock() {
    return null;
  }

  /**
   * Set user limit.
   * @param userLimit new user limit
   */
  @VisibleForTesting
  void setUserLimit(float userLimit) {
    usersManager.setUserLimit(userLimit);
    usersManager.userLimitNe