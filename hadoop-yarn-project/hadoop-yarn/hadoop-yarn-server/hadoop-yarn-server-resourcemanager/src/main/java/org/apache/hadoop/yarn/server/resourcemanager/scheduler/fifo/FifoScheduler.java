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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo;

import org.apache.hadoop.classification.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.SchedulingRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.api.protocolrecords.NMContainerStatus;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.RMState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ActiveUsersManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ContainerUpdates;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerAppUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplication;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.ContainerExpiredSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeResourceUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.ReleaseContainerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.PendingAsk;
import org.apache.hadoop.yarn.server.scheduler.SchedulerRequestKey;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.server.utils.Lock;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * FIFO调度器实现，YARN最简单的调度器，所有应用按照提交顺序排队，先到先得分配资源
 * 是YARN内置的基础调度器，适合单队列、简单场景使用
 */
@LimitedPrivate("yarn")
@Evolving
@SuppressWarnings("unchecked")
public class FifoScheduler extends
    AbstractYarnScheduler<FifoAppAttempt, FiCaSchedulerNode> implements
    Configurable {

  private static final Logger LOG =
      LoggerFactory.getLogger(FifoScheduler.class);

  private static final RecordFactory recordFactory = 
    RecordFactoryProvider.getRecordFactory(null);

  Configuration conf;

  private boolean usePortForNodeName;

  private ActiveUsersManager activeUsersManager;

  private static final String DEFAULT_QUEUE_NAME = "default";
  private QueueMetrics metrics;
  
  private final ResourceCalculator resourceCalculator = new DefaultResourceCalculator();

  /**
   * 默认队列实现，FIFO调度器只有一个全局默认队列
   */
  private final Queue DEFAULT_QUEUE = new Queue() {
    @Override
    public String getQueueName() {
      return DEFAULT_QUEUE_NAME;
    }

    @Override
    public QueueMetrics getMetrics() {
      return metrics;
    }

    @Override
    public QueueInfo getQueueInfo( 
        boolean includeChildQueues, boolean recursive) {
      QueueInfo queueInfo = recordFactory.newRecordInstance(QueueInfo.class);
      queueInfo.setQueueName(DEFAULT_QUEUE.getQueueName());
      // FIFO整个集群资源都属于默认队列，容量100%
      queueInfo.setCapacity(1.0f);
      Resource clusterResource = getClusterResource();
      if (clusterResource.getMemorySize() == 0) {
        queueInfo.setCurrentCapacity(0.0f);
      } else {
        // 计算当前已使用容量占比
        queueInfo.setCurrentCapacity((float) usedResource.getMemorySize()
            / clusterResource.getMemorySize());
      }
      queueInfo.setMaximumCapacity(1.0f);
      queueInfo.setChildQueues(new ArrayList<QueueInfo>());
      queueInfo.setQueueState(QueueState.RUNNING);
      return queueInfo;
    }

    public Map<QueueACL, AccessControlList> getQueueAcls() {
      Map<QueueACL, AccessControlList> acls =
        new HashMap<QueueACL, AccessControlList>();
      // 允许所有用户所有权限，FIFO默认开全部权限
      for (QueueACL acl : QueueACL.values()) {
        acls.put(acl, new AccessControlList("*"));
      }
      return acls;
    }

    @Override
    public List<QueueUserACLInfo> getQueueUserAclInfo(
        UserGroupInformation unused) {
      QueueUserACLInfo queueUserAclInfo = 
        recordFactory.newRecordInstance(QueueUserACLInfo.class);
      queueUserAclInfo.setQueueName(DEFAULT_QUEUE_NAME);
      queueUserAclInfo.setUserAcls(Arrays.asList(QueueACL.values()));
      return Collections.singletonList(queueUserAclInfo);
    }

    @Override
    public boolean hasAccess(QueueACL acl, UserGroupInformation user) {
      // 检查用户对队列的ACL权限
      return getQueueAcls().get(acl).isUserAllowed(user);
    }
    
    @Override
    public ActiveUsersManager getAbstractUsersManager() {
      return activeUsersManager;
    }

    @Override
    public void recoverContainer(Resource clusterResource,
        SchedulerApplicationAttempt schedulerAttempt, RMContainer rmContainer) {
      // 已完成容器不需要恢复，直接返回
      if (rmContainer.getState().equals(RMContainerState.COMPLETED)) {
        return;
      }
      // 恢复已分配容器的资源统计
      increaseUsedResources(rmContainer);
      // 更新应用可分配资源量
      updateAppHeadRoom(schedulerAttempt);
      // 更新队列可用资源指标
      updateAvailableResourcesMetrics();
    }

    @Override
    public Set<String> getAccessibleNodeLabels() {
      // TODO add implementation for FIFO scheduler
      return null;
    }

    @Override
    public String getDefaultNodeLabelExpression() {
      // TODO add implementation for FIFO scheduler
      return null;
    }

    @Override
    public void incPendingResource(String nodeLabel, Resource resourceToInc) {
    }

    @Override
    public void decPendingResource(String nodeLabel, Resource resourceToDec) {
    }

    @Override
    public Priority getDefaultApplicationPriority() {
      // TODO add implementation for FIFO scheduler
      return null;
    }

    @Override
    public void incReservedResource(String partition, Resource reservedRes) {
      // TODO add implementation for FIFO scheduler

    }

    @Override
    public void decReservedResource(String partition, Resource reservedRes) {
      // TODO add implementation for FIFO scheduler

    }
  };

  /**
   * 构造FIFO调度器实例
   */
  public FifoScheduler() {
    super(FifoScheduler.class.getName());
  }

  /**
   * 初始化FIFO调度器核心配置和数据结构
   */
  private synchronized void initScheduler(Configuration conf) {
    validateConf(conf);
    // 使用跳表保证应用按顺序排序，保证FIFO顺序
    this.applications =
        new ConcurrentSkipListMap<>();
    this.minimumAllocation = super.getMinimumAllocation();
    initMaximumResourceCapability(super.getMaximumAllocation());
    this.usePortForNodeName = conf.getBoolean(
        YarnConfiguration.RM_SCHEDULER_INCLUDE_PORT_IN_NODE_NAME,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_USE_PORT_FOR_NODE_NAME);
    // 初始化默认队列指标
    this.metrics = QueueMetrics.forQueue(DEFAULT_QUEUE_NAME, null, false,
        conf);
    this.activeUsersManager = new ActiveUsersManager(metrics);
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    initScheduler(conf);
    super.serviceInit(conf);

    // 初始化调度监控管理器
    schedulingMonitorManager.initialize(rmContext, conf);
  }

  @Override
  public void serviceStart() throws Exception {
    super.serviceStart();
  }

  @Override
  public void serviceStop() throws Exception {
    super.serviceStop();
  }

  @Override
  public synchronized void setConf(Configuration conf) {
    this.conf = conf;
  }
  
  /**
   * 验证调度器配置合法性检查，检查最小最大分配配置是否合法
   */
  private void validateConf(Configuration conf) {
    // 验证调度器内存分配配置检查
    int minMem = conf.getInt(
      YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
      YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB);
    int maxMem = conf.getInt(
      YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
      YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB);

    if (minMem <= 0 || minMem > maxMem) {
      throw new YarnRuntimeException("Invalid resource scheduler memory"
        + " allocation configuration"
        + ", " + YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB
        + "=" + minMem
        + ", " + YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB
        + "=" + maxMem + ", min and max should be greater than 0"
        + ", max should be no smaller than min.");
    }
  }
  
  @Override
  public synchronized Configuration getConf() {
    return conf;
  }

  @Override
  public int getNumClusterNodes() {
    return nodeTracker.nodeCount();
  }

  @Override
  public synchronized void setRMContext(RMContext rmContext) {
    this.rmContext = rmContext;
  }

  @Override
  public synchronized void
      reinitialize(Configuration conf, RMContext rmContext) throws IOException
  {
    setConf(conf);
    super.reinitialize(conf, rmContext);
  }

  /**
   * 处理应用分配请求，更新请求、释放容器、返回分配结果
   */
  @Override
  public Allocation allocate(ApplicationAttemptId applicationAttemptId,
      List<ResourceRequest> ask, List<SchedulingRequest> schedulingRequests,
      List<ContainerId> release, List<String> blacklistAdditions, List<String> blacklistRemovals,
      ContainerUpdates updateRequests) {
    FifoAppAttempt application = getApplicationAttempt(applicationAttemptId);
    if (application == null) {
      LOG.error("Calling allocate on removed or non existent application " +
          applicationAttemptId.getApplicationId());
      return EMPTY_ALLOCATION;
    }

    // 避免之前尝试的残留请求会影响当前尝试，需要二次检查
    // 外部预检查可能结果已过时，此处二次检查是必须的
    if (!application.getApplicationAttemptId().equals(applicationAttemptId)) {
      LOG.error("Calling allocate on previous or removed " +
          "or non existent application attempt " + applicationAttemptId);
      return EMPTY_ALLOCATION;
    }

    // 规范化资源请求，统一处理
    normalizeResourceRequests(ask);

    // 处理需要释放的容器
    releaseContainers(release, application);

    synchronized (application) {

      // 检查应用是否已经停止，停止应用不处理分配
      if (application.isStopped()) {
        LOG.info("Calling allocate on a stopped " +
            "application " + applicationAttemptId);
        return EMPTY_ALLOCATION;
      }

      if (!ask.isEmpty()) {
        LOG.debug("allocate: pre-update" +
            " applicationId=" + applicationAttemptId + 
            " application=" + application);
        application.showRequests();

        // 更新应用的资源请求
        application.updateResourceRequests(ask);

        LOG.debug("allocate: post-update" +
            " applicationId=" + applicationAttemptId + 
            " application=" + application);
        application.showRequests();

        LOG.debug("allocate:" +
            " applicationId=" + applicationAttemptId +
            " #ask=" + ask.size());
      }

      // 更新节点黑名单
      application.updateBlacklist(blacklistAdditions, blacklistRemovals);

      // 计算应用可用空闲资源
      Resource headroom = application.getHeadroom();
      // 更新指标
      application.setApplicationHeadroomForMetrics(headroom);
      // 返回分配结果，包含新分配容器、可用资源等
      return new Allocation(application.pullNewlyAllocatedContainers(),
          headroom, null, null, null, application.pullUpdatedNMTokens());
    }
  }

  /**
   * 添加新应用到调度器，仅用于测试暴露
   */
  @VisibleForTesting
  public synchronized void addApplication(ApplicationId applicationId,
      String queue, String user, boolean isAppRecovering,
      boolean unmanagedAM) {
    // 新建调度器应用实例，FIFO所有应用都在默认队列
    SchedulerApplication<FifoAppAttempt> application =
        new SchedulerApplication<>(DEFAULT_QUEUE, user, unmanagedAM);
    applications.put(applicationId, application);

    // 更新指标统计
    metrics.submitApp(user, unmanagedAM);
    LOG.info("Accepted application " + applicationId + " from user: " + user
        + ", currently num of applications: " + applications.size());
    if (isAppRecovering) {
      LOG.debug("{} is recovering. Skip notifying APP_ACCEPTED",
          applicationId);
    } else {
      // 通知RM应用已接受，触发后续流程
      rmContext.getDispatcher().getEventHandler()
        .handle(new RMAppEvent(applicationId, RMAppEventType.APP_ACCEPTED));
    }
  }

  /**
   * 添加新应用尝试到调度