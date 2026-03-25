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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Lists;
import org.apache.hadoop.util.concurrent.SubjectInheritingThread;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.RMWSConsts;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ActivitiesInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppActivitiesInfo;
import org.apache.hadoop.yarn.util.SystemClock;

import org.apache.hadoop.classification.VisibleForTesting;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.*;
import java.util.stream.Collectors;

/**
 * YARN ResourceManager 调度活动记录管理器，负责存储节点和应用的分配活动记录
 * 主要提供分配活动的开始、添加、更新和完成全生命周期管理，供Web UI查询调度过程
 */
public class ActivitiesManager extends AbstractService {
  private static final Logger LOG =
      LoggerFactory.getLogger(ActivitiesManager.class);
  // 空节点ID占位符，批量记录多节点分配活动时使用
  public static final NodeId EMPTY_NODE_ID = NodeId.newInstance("", 0);
  public static final char DIAGNOSTICS_DETAILS_SEPARATOR = '\n';
  public static final String EMPTY_DIAGNOSTICS = "";
  // 当前线程正在记录的各节点分配活动集合
  private ThreadLocal<Map<NodeId, List<NodeAllocation>>>
      recordingNodesAllocation;
  @VisibleForTesting
  // 已完成的节点分配活动存储，按节点ID分组
  ConcurrentMap<NodeId, List<NodeAllocation>> completedNodeAllocations;
  // 需要记录活动的活跃节点集合
  private Set<NodeId> activeRecordedNodes;
  // 指定时间范围内需要记录活动的应用，value为结束时间戳
  private ConcurrentMap<ApplicationId, Long>
      recordingAppActivitiesUntilSpecifiedTime;
  // 当前线程正在记录的各应用分配活动集合
  private ThreadLocal<Map<ApplicationId, AppAllocation>>
      appsAllocation;
  @VisibleForTesting
  // 已完成的应用分配活动存储，按应用ID分组
  ConcurrentMap<ApplicationId, Queue<AppAllocation>> completedAppAllocations;
  // 待记录的节点活动计数器，用于批量活动查询
  private AtomicInteger recordCount = new AtomicInteger(0);
  // 最近一次节点分配活动缓存
  private List<NodeAllocation> lastAvailableNodeActivities = null;
  // 过期活动清理线程
  private Thread cleanUpThread;
  // 活动清理间隔（毫秒）
  private long activitiesCleanupIntervalMs;
  // 节点调度活动存活时间（毫秒）
  private long schedulerActivitiesTTL;
  // 应用分配活动存活时间（毫秒）
  private long appActivitiesTTL;
  // 应用活动队列最大长度（动态调整）
  private volatile int appActivitiesMaxQueueLength;
  // 配置文件中指定的应用活动队列最大长度
  private int configuredAppActivitiesMaxQueueLength;
  // RM上下文引用
  private final RMContext rmContext;
  // 服务停止标记
  private volatile boolean stopped;
  // 当前线程诊断信息收集器管理器
  private ThreadLocal<DiagnosticsCollectorManager> diagnosticCollectorManager;
  // 最近N次节点分配活动队列，用于批量查询
  private volatile ConcurrentLinkedDeque<Pair<NodeId, List<NodeAllocation>>>
      lastNActivities;

  /**
   * 构造活动管理器，初始化各类存储结构
   * @param rmContext ResourceManager上下文
   */
  public ActivitiesManager(RMContext rmContext) {
    super(ActivitiesManager.class.getName());
    recordingNodesAllocation = ThreadLocal.withInitial(() -> new HashMap());
    completedNodeAllocations = new ConcurrentHashMap<>();
    appsAllocation = ThreadLocal.withInitial(() -> new HashMap());
    completedAppAllocations = new ConcurrentHashMap<>();
    activeRecordedNodes = Collections.newSetFromMap(new ConcurrentHashMap<>());
    recordingAppActivitiesUntilSpecifiedTime = new ConcurrentHashMap<>();
    diagnosticCollectorManager = ThreadLocal.withInitial(
        () -> new DiagnosticsCollectorManager(
            new GenericDiagnosticsCollector()));
    this.rmContext = rmContext;
    if (rmContext.getYarnConfiguration() != null) {
      setupConfForCleanup(rmContext.getYarnConfiguration());
    }
    lastNActivities = new ConcurrentLinkedDeque<>();
  }

  /**
   * 从配置加载清理相关参数
   * @param conf YARN配置
   */
  private void setupConfForCleanup(Configuration conf) {
    activitiesCleanupIntervalMs = conf.getLong(
        YarnConfiguration.RM_ACTIVITIES_MANAGER_CLEANUP_INTERVAL_MS,
        YarnConfiguration.
            DEFAULT_RM_ACTIVITIES_MANAGER_CLEANUP_INTERVAL_MS);
    schedulerActivitiesTTL = conf.getLong(
        YarnConfiguration.RM_ACTIVITIES_MANAGER_SCHEDULER_ACTIVITIES_TTL_MS,
        YarnConfiguration.
            DEFAULT_RM_ACTIVITIES_MANAGER_SCHEDULER_ACTIVITIES_TTL_MS);
    appActivitiesTTL = conf.getLong(
        YarnConfiguration.RM_ACTIVITIES_MANAGER_APP_ACTIVITIES_TTL_MS,
        YarnConfiguration.
            DEFAULT_RM_ACTIVITIES_MANAGER_APP_ACTIVITIES_TTL_MS);
    configuredAppActivitiesMaxQueueLength = conf.getInt(YarnConfiguration.
            RM_ACTIVITIES_MANAGER_APP_ACTIVITIES_MAX_QUEUE_LENGTH,
        YarnConfiguration.
            DEFAULT_RM_ACTIVITIES_MANAGER_APP_ACTIVITIES_MAX_QUEUE_LENGTH);
    appActivitiesMaxQueueLength = configuredAppActivitiesMaxQueueLength;
  }

  /**
   * 获取指定应用的分配活动信息，供Web UI查询
   * @param applicationId 应用ID
   * @param requestPriorities 优先级过滤条件
   * @param allocationRequestIds 分配请求ID过滤条件
   * @param groupBy 分组方式
   * @param limit 返回最大条数
   * @param summarize 是否汇总最近一段时间的活动
   * @param maxTimeInSeconds 汇总时间范围（秒）
   * @return 应用活动信息DAO对象
   */
  public AppActivitiesInfo getAppActivitiesInfo(ApplicationId applicationId,
      Set<Integer> requestPriorities, Set<Long> allocationRequestIds,
      RMWSConsts.ActivitiesGroupBy groupBy, int limit, boolean summarize,
      double maxTimeInSeconds) {
    RMApp app = rmContext.getRMApps().get(applicationId);
    if (app != null && app.getFinalApplicationStatus()
        == FinalApplicationStatus.UNDEFINED) {
      Queue<AppAllocation> curAllocations =
          completedAppAllocations.get(applicationId);
      List<AppAllocation> allocations = null;
      if (curAllocations != null) {
        if (CollectionUtils.isNotEmpty(requestPriorities) || CollectionUtils
            .isNotEmpty(allocationRequestIds)) {
          allocations = curAllocations.stream().map(e -> e
              .filterAllocationAttempts(requestPriorities,
                  allocationRequestIds))
              .filter(e -> !e.getAllocationAttempts().isEmpty())
              .collect(Collectors.toList());
        } else {
          allocations = new ArrayList(curAllocations);
        }
      }
      if (summarize && allocations != null) {
        AppAllocation summaryAppAllocation =
            getSummarizedAppAllocation(allocations, maxTimeInSeconds);
        if (summaryAppAllocation != null) {
          allocations = Lists.newArrayList(summaryAppAllocation);
        }
      }
      if (allocations != null && limit > 0 && limit < allocations.size()) {
        allocations =
            allocations.subList(allocations.size() - limit, allocations.size());
      }
      return new AppActivitiesInfo(allocations, applicationId, groupBy);
    } else {
      return new AppActivitiesInfo(
          "fail to get application activities after finished",
          applicationId.toString());
    }
  }

  /**
   * 汇总多个分配记录生成最近时间段的分配摘要：
   * 1. 收集指定时间范围内各节点上最新的分配尝试
   * 2. 从最后一条分配记录复制其他基础信息
   * @param allocations 原始分配记录列表
   * @param maxTimeInSeconds 汇总时间范围（秒）
   * @return 汇总后的分配记录
   */
  private AppAllocation getSummarizedAppAllocation(
      List<AppAllocation> allocations, double maxTimeInSeconds) {
    if (allocations == null || allocations.isEmpty()) {
      return null;
    }
    long startTime = allocations.get(allocations.size() - 1).getTime()
        - (long) (maxTimeInSeconds * 1000);
    Map<String, ActivityNode> nodeActivities = new HashMap<>();
    for (int i = allocations.size() - 1; i >= 0; i--) {
      AppAllocation appAllocation = allocations.get(i);
      if (startTime > appAllocation.getTime()) {
        break;
      }
      List<ActivityNode> activityNodes = appAllocation.getAllocationAttempts();
      for (ActivityNode an : activityNodes) {
        nodeActivities.putIfAbsent(
            an.getRequestPriority() + "_" + an.getAllocationRequestId() + "_"
                + an.getNodeId(), an);
      }
    }
    AppAllocation lastAppAllocation = allocations.get(allocations.size() - 1);
    AppAllocation summarizedAppAllocation =
        new AppAllocation(lastAppAllocation.getPriority(), null,
            lastAppAllocation.getQueueName());
    summarizedAppAllocation.updateAppContainerStateAndTime(null,
        lastAppAllocation.getActivityState(), lastAppAllocation.getTime(),
        lastAppAllocation.getDiagnostic());
    summarizedAppAllocation
        .setAllocationAttempts(new ArrayList<>(nodeActivities.values()));
    return summarizedAppAllocation;
  }

  /**
   * 获取指定节点的调度活动信息，供Web UI查询
   * @param nodeId 节点ID，null表示获取最近一次
   * @param groupBy 分组方式
   * @return 节点活动信息DAO对象
   */
  public ActivitiesInfo getActivitiesInfo(String nodeId,
      RMWSConsts.ActivitiesGroupBy groupBy) {
    List<NodeAllocation> allocations;
    if (nodeId == null) {
      allocations = lastAvailableNodeActivities;
    } else {
      allocations = completedNodeAllocations.get(NodeId.fromString(nodeId));
    }
    return new ActivitiesInfo(allocations, nodeId, groupBy);
  }


  /**
   * 记录并批量获取最近指定数量的节点调度活动
   * @param activitiesCount 需要获取的活动数量
   * @param groupBy 分组方式
   * @return 批量活动信息列表
   * @throws InterruptedException 线程休眠被中断时抛出
   */
  public List<ActivitiesInfo> recordAndGetBulkActivitiesInfo(
      int activitiesCount, RMWSConsts.ActivitiesGroupBy groupBy)
      throws InterruptedException {
    recordCount.set(activitiesCount);
    while (recordCount.get() > 0) {
      Thread.sleep(1);
    }
    Iterator<Pair<NodeId, List<NodeAllocation>>> ite =
        lastNActivities.iterator();
    List<ActivitiesInfo> outList = new ArrayList<>();
    while (ite.hasNext()) {
      Pair<NodeId, List<NodeAllocation>> pair = ite.next();
      outList.add(new ActivitiesInfo(pair.getRight(),
          pair.getLeft().toString(), groupBy));
    }
    // 重置最近活动队列，为下一次批量查询做准备
    lastNActivities = new ConcurrentLinkedDeque<>();
    return outList;
  }

  /**
   * 标记下一次节点更新需要记录活动
   * @param nodeId 节点ID，null表示记录批量多节点活动
   */
  public void recordNextNodeUpdateActivities(String nodeId) {
    if (nodeId == null) {
      recordCount.compareAndSet(0, 1);
    } else {
      activeRecordedNodes.add(NodeId.fromString(nodeId));
    }
  }

  /**
   * 开启指定应用的活动记录，持续指定时长
   * @param applicationId 应用ID
   * @param maxTime 记录持续时长（秒）
   */
  public void turnOnAppActivitiesRecording(ApplicationId applicationId,
      double maxTime) {
    long startTS = SystemClock.getInstance().getTime();
    long endTS = startTS + (long) (maxTime * 1000);
    recordingAppActivitiesUntilSpecifiedTime.put(applicationId, endTS);
  }

  /**
   * 根据集群状态动态调整应用活动队列最大长度
   * 禁用多节点放置时，根据集群节点数和异步调度线程数自动增大队列长度
   */
  private void dynamicallyUpdateAppActivitiesMaxQueueLengthIfNeeded() {
    if (rmContext.getRMNodes() == null) {
      return;
    }
    if (rmContext.getScheduler() instanceof CapacityScheduler) {
      CapacityScheduler cs = (CapacityScheduler) rmContext.getScheduler();
      if (!cs.isMultiNodePlacementEnabled()) {
        int numNodes = rmContext.getRMNodes().size();
        int newAppActivitiesMaxQueueLength;
        int numAsyncSchedulerThreads = cs.getNumAsyncSchedulerThreads();
        if (numAsyncSchedulerThreads > 0) {
          newAppActivitiesMaxQueueLength =
              Math.max(configuredAppActivitiesMaxQueueLength,
                  numNodes * numAsyncSchedulerThreads);
        } else {
          newAppActivitiesMaxQueueLength =
              Math.max(configuredAppActivitiesMaxQueueLength,
                  (int) (numNodes * 1.2));
        }
        if (appActivitiesMaxQueueLength != newAppActivitiesMaxQueueLength) {
          LOG.info("Update max queue length of app activities from {} to {},"
                  + " configured={}, numNodes={}, numAsyncSchedulerThreads={}"
                  + " when multi-node placement disabled.",
              appActivitiesMaxQueueLength, newAppActivitiesMaxQueueLength,
              configuredAppActivitiesMaxQueueLength, numNodes,
              numAsyncSchedulerThreads);
          appActivitiesMaxQueueLength = newAppActivitiesMaxQueueLength;
        }
      } else if (appActivitiesMaxQueueLength
          != configuredAppActivitiesMaxQueueLength) {
        LOG.info("Update max queue length of app activities from {} to {}"
                + " when multi-node placement enabled.",
            appActivitiesMaxQueueLength, configuredAppActivitiesMaxQueueLength);
        appActivitiesMaxQueueLength = configuredAppActivitiesMaxQueueLength;
      }
    }
  }

  @Override
  protected void serviceStart() throws Exception {
    // 启动后台清理线程，定期清理过期活动
    cleanUpThread = new SubjectInheritingThread(new Runnable() {
      @Override
      public void run() {
        while (!stopped && !Thread.currentThread().isInterrupted()) {
          // 清理过期节点活动
          Iterator<Map.Entry<NodeId, List<NodeAllocation>>> ite =
              completedNodeAllocations.entrySet().iterator();
          long curTS = SystemClock.getInstance().getTime();
          while (ite.hasNext()) {
            Map.Entry<NodeId, List<NodeAllocation>> nodeAllocation = ite.next();
            List<NodeAllocation> allocations = nodeAllocation.getValue();
            if (allocations.size() > 0
                && curTS - allocations.get(0).getTimestamp()
                > schedulerActivitiesTTL) {
              ite.remove();
            }
          }

          // 清理过期应用活动
          Iterator<Map.Entry<ApplicationId, Queue<AppAllocation>>> iteApp =
              completedAppAllocations