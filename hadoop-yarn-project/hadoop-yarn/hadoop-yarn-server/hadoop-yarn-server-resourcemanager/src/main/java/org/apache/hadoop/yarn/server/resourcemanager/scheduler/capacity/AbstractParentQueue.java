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
 * http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.util.Sets;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerDynamicEditException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.security.AccessType;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ActiveUsersManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceLimits;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivitiesLogger;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivityDiagnosticConstant;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.ActivityState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities.AllocationState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.policy.QueueOrderingPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.ContainerAllocationProposal;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.ResourceCommitRequest;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.SchedulerContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.CandidateNodeSet;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.CandidateNodeSetUtils;
import org.apache.hadoop.yarn.util.UnitsConversionUtil;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.apache.hadoop.yarn.util.resource.Resources;

import static org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager.NO_LABEL;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.getACLsForFlexibleAutoCreatedParentQueue;

/**
 * 容量调度器父队列抽象基类，定义了父队列的通用行为和属性，是所有父队列类型的公共父类
 * 负责管理子队列，实现容器分配、配置初始化、动态队列增删等核心能力
 */
public abstract class AbstractParentQueue extends AbstractCSQueue {
  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractParentQueue.class);

  protected final List<CSQueue> childQueues;
  private final boolean rootQueue;
  private AtomicInteger numApplications = new AtomicInteger(0);

  private final RecordFactory recordFactory =
      RecordFactoryProvider.getRecordFactory(null);

  private QueueOrderingPolicy queueOrderingPolicy;

  private long lastSkipQueueDebugLoggingTimestamp = -1;

  private int runnableApps;

  private final boolean allowZeroCapacitySum;

  private AutoCreatedQueueTemplate autoCreatedQueueTemplate;

  // 当前队列有效最小资源占所有子队列配置最小资源总和的比例，按标签和资源类型分别存储
  private final Map<String, Map<String, Float>> effectiveMinResourceRatio =
      new ConcurrentHashMap<>();

  /**
   * 构造父队列对象
   */
  public AbstractParentQueue(CapacitySchedulerQueueContext queueContext,
      String queueName, CSQueue parent, CSQueue old)
      throws IOException {
    this(queueContext, queueName, parent, old, false);
  }

  /**
   * 构造父队列对象，支持标记是否为动态队列
   */
  public AbstractParentQueue(CapacitySchedulerQueueContext queueContext,
      String queueName, CSQueue parent, CSQueue old, boolean isDynamic) throws
      IOException {

    super(queueContext, queueName, parent, old);
    setDynamicQueue(isDynamic);
    this.rootQueue = (parent == null);

    float rawCapacity = queueContext.getConfiguration()
          .getNonLabeledQueueCapacity(this.queuePath);

    if (rootQueue &&
          (rawCapacity != CapacitySchedulerConfiguration.MAXIMUM_CAPACITY_VALUE)) {
      throw new IllegalArgumentException("Illegal " +
            "capacity of " + rawCapacity + " for queue " + queueName +
            ". Must be " + CapacitySchedulerConfiguration.MAXIMUM_CAPACITY_VALUE);
    }

    this.childQueues = new ArrayList<>();
    this.allowZeroCapacitySum =
          queueContext.getConfiguration()
              .getAllowZeroCapacitySum(getQueuePathObject());

  }

  /**
   * 获取队列排序策略的配置名称
   */
  // returns what is configured queue ordering policy
  private String getQueueOrderingPolicyConfigName() {
    return queueOrderingPolicy == null ?
        null :
        queueOrderingPolicy.getConfigName();
  }

  /**
   * 初始化队列配置，加载ACL、标签、排序策略等配置信息
   * @param clusterResource 集群总资源
   * @throws IOException 配置初始化异常
   */
  protected void setupQueueConfigs(Resource clusterResource)
      throws IOException {
    writeLock.lock();
    try {
      CapacitySchedulerConfiguration configuration = queueContext.getConfiguration();
      // 初始化自动创建队列模板
      autoCreatedQueueTemplate = new AutoCreatedQueueTemplate(
          configuration, this.queuePath);
      super.setupQueueConfigs(clusterResource);
      // 拼接ACL信息用于日志输出
      StringBuilder aclsString = new StringBuilder();
      for (Map.Entry<AccessType, AccessControlList> e : getACLs().entrySet()) {
        aclsString.append(e.getKey()).append(":")
            .append(e.getValue().getAclString());
      }

      // 拼接节点标签信息用于日志输出
      StringBuilder labelStrBuilder = new StringBuilder();
      if (getAccessibleNodeLabels() != null) {
        for (String nodeLabel : getAccessibleNodeLabels()) {
          labelStrBuilder.append(nodeLabel).append(",");
        }
      }

      // 初始化队列排序策略，继承父队列策略或使用自身配置
      queueOrderingPolicy = configuration.getQueueOrderingPolicy(
          getQueuePathObject(), parent == null ?
              null :
              ((AbstractParentQueue) parent).getQueueOrderingPolicyConfigName());
      queueOrderingPolicy.setQueues(childQueues);

      LOG.info(getQueueName() + ", " + getCapacityOrWeightString()
          + ", absoluteCapacity=" + getAbsoluteCapacity()
          + ", maxCapacity=" + getMaximumCapacity()
          + ", absoluteMaxCapacity=" + getAbsoluteMaximumCapacity()
          + ", state=" + getState() + ", acls="
          + aclsString + ", labels=" + labelStrBuilder + "\n"
          + ", reservationsContinueLooking=" + isReservationsContinueLooking()
          + ", orderingPolicy=" + getQueueOrderingPolicyConfigName()
          + ", priority=" + getPriority()
          + ", allowZeroCapacitySum=" + allowZeroCapacitySum);
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  protected void setDynamicQueueACLProperties() {
    super.setDynamicQueueACLProperties();

    // 动态自动创建父队列从父模板继承ACL
    if (parent instanceof AbstractParentQueue) {
      acls.putAll(getACLsForFlexibleAutoCreatedParentQueue(
          ((AbstractParentQueue) parent).getAutoCreatedQueueTemplate()));
    }
  }

  private static float PRECISION = 0.0005f; // 0.05% 精度误差允许范围

  /**
   * 检查子队列容量配置类型，判断使用权重、百分比还是绝对资源，混合配置会抛出异常
   * @param queues 待检查的子队列集合
   * @return 容量配置类型
   * @throws IOException 混合配置非法时抛出异常
   */
  // Check weight configuration, throw exception when configuration is invalid
  // return true when all children use weight mode.
  public QueueCapacityType getCapacityConfigurationTypeForQueues(
      Collection<CSQueue> queues) throws IOException {
    // 标记是否有队列使用百分比模式
    boolean percentageIsSet = false;

    // 标记是否有队列使用权重模式
    boolean weightIsSet = false;

    // 标记是否有队列使用绝对资源模式
    boolean absoluteMinResSet = false;

    StringBuilder diagMsg = new StringBuilder();

    // 遍历所有子队列和标签检查配置类型
    for (CSQueue queue : queues) {
      for (String nodeLabel : queueCapacities.getExistingNodeLabels()) {
        float capacityByLabel = queue.getQueueCapacities().getCapacity(nodeLabel);
        if (capacityByLabel > 0) {
          percentageIsSet = true;
        }
        float weightByLabel = queue.getQueueCapacities().getWeight(nodeLabel);
        // 默认权重为-1，>=0代表配置了权重
        if (weightByLabel >= 0) {
          weightIsSet = true;
          diagMsg.append(
              "{Queue=" + queue.getQueuePath() + ", label=" + nodeLabel
                  + " uses weight mode}. ");
        }
        if (checkConfigTypeIsAbsoluteResource(queue.getQueuePathObject(), nodeLabel)) {
          absoluteMinResSet = true;
          // 绝对资源配置场景下，容量仅为UI/指标展示使用，因此取消百分比标记
          percentageIsSet = false;
          diagMsg.append(
              "{Queue=" + queue.getQueuePath() + ", label=" + nodeLabel
                  + " uses absolute mode}. ");
        }
        if (percentageIsSet) {
          diagMsg.append(
              "{Queue=" + queue.getQueuePath() + ", label=" + nodeLabel
                  + " uses percentage mode}. ");
        }
      }
    }
    // 根队列是例外，不检查配置混合
    if (queues.iterator().hasNext() &&
        !queues.iterator().next().getQueuePath().equals(
        CapacitySchedulerConfiguration.ROOT) &&
        (percentageIsSet ? 1 : 0) + (weightIsSet ? 1 : 0) + (absoluteMinResSet ?
            1 :
            0) > 1) {
      throw new IOException("Parent queue '" + getQueuePath()
          + "' have children queue used mixed of "
          + " weight mode, percentage and absolute mode, it is not allowed, please "
          + "double check, details:" + diagMsg.toString());
    }

    // 返回检测出的配置类型
    if (weightIsSet || queues.isEmpty()) {
      return QueueCapacityType.WEIGHT;
    } else if (absoluteMinResSet) {
      return QueueCapacityType.ABSOLUTE_RESOURCE;
    } else {
      return QueueCapacityType.PERCENT;
    }
  }

  /**
   * 队列容量配置模式枚举
   */
  public enum QueueCapacityType {
    WEIGHT, ABSOLUTE_RESOURCE, PERCENT;
  }

  /**
   * 设置子队列列表，并验证容量配置合法性
   * 根据不同配置模式检查子队列总和是否符合规则，非法配置抛出异常
   * @param childQueues 子队列集合
   * @throws IOException 配置非法时抛出异常
   */
  void setChildQueues(Collection<CSQueue> childQueues) throws IOException {
    writeLock.lock();
    try {
      boolean isLegacyQueueMode = queueContext.getConfiguration().isLegacyQueueMode();
      if (isLegacyQueueMode) {
        QueueCapacityType childrenCapacityType =
            getCapacityConfigurationTypeForQueues(childQueues);
        QueueCapacityType parentCapacityType =
            getCapacityConfigurationTypeForQueues(ImmutableList.of(this));

        // 绝对资源模式必须父子队列同时使用，不允许混合
        if (childrenCapacityType == QueueCapacityType.ABSOLUTE_RESOURCE
            || parentCapacityType == QueueCapacityType.ABSOLUTE_RESOURCE) {
          if (childrenCapacityType != parentCapacityType && !this.getQueuePath()
              .equals(CapacitySchedulerConfiguration.ROOT)) {
            throw new IOException("Parent=" + this.getQueuePath()
                + ": When absolute minResource is used, we must make sure both "
                + "parent and child all use absolute minResource");
          }

          // 检查父队列最小资源 >= 所有子队列最小资源之和
          for (String nodeLabel : queueCapacities.getExistingNodeLabels()) {
            Resource minRes = Resources.createResource(0, 0);
            for (CSQueue queue : childQueues) {
              // 累加所有子队列的配置最小资源
              Resources.addTo(minRes, queue.getQueueResourceQuotas()
                  .getConfiguredMinResource(nodeLabel));
            }
            Resource resourceByLabel = labelManager.getResourceByLabel(nodeLabel,
                queueContext.getClusterResource());
            Resource parentMinResource =
                usageTracker.getQueueResourceQuotas().getConfiguredMinResource(nodeLabel);
            if (!parentMinResource.equals(Resources.none()) && Resources.lessThan(
                resourceCalculator, resourceByLabel, parentMinResource, minRes)) {
              throw new IOException(
                  "Parent Queues" + " capacity: " + parentMinResource
                      + " is less than" + " to its children:" + minRes
                      + " for queue:" + getQueueName());
            }
          }
        }

        // 百分比模式检查总和必须为0或1
        if (childrenCapacityType == QueueCapacityType.PERCENT) {
          float childrenPctSum = 0;
          // 按标签分别检查子队列容量总和
          for (String nodeLabel : queueCapacities.getExistingNodeLabels()) {
            childrenPctSum = 0;
            for (CSQueue queue : childQueues) {
              childrenPctSum += queue.getQueueCapacities().getCapacity(nodeLabel);
            }

            if (Math.abs(1 - childrenPctSum) > PRECISION) {
              // 总和不等于1，检查是否等于0
              if (Math.abs(childrenPctSum) > PRECISION) {
                // 既不等于0也不等于1，非法配置
                throw new IOException(
                    "Illegal" + " capacity sum of " + childrenPctSum
                        + " for children of queue " + getQueueName() + " for label="
                        + nodeLabel + ". It should be either 0 or 1.0");
              } else {
                // 总和等于0，仅允许满足条件的场景
                // - 父队列使用权重模式，或者
                // - 父队列使用百分比模式，且父容量为0 或 允许子队列容量总和为0
                if (parentCapacityType == QueueCapacityType.PERCENT) {
                  if ((Math.abs(queueCapacities.getCapacity(nodeLabel))
                      > PRECISION) && (!allowZeroCapacitySum)) {
                    throw new IOException(
                        "Illegal" + " capacity sum of " + childrenPctSum
                            + " for children of queue " + getQueueName()
                            + " for label=" + nodeLabel
                            + ". It is set to 0, but parent percent != 0, and "
                            + "doesn't allow children capacity to set to 0");
                  }
                }
              }
            } else {
              // 总和等于1，检查父队列容量不为0，否则非法
              if (parentCapacityType == QueueCapacityType.PERCENT && Math.abs(
                  queueCap