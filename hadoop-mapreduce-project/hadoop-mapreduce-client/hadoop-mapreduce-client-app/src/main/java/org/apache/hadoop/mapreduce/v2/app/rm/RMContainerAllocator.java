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

package org.apache.hadoop.mapreduce.v2.app.rm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobCounter;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryEvent;
import org.apache.hadoop.mapreduce.jobhistory.NormalizedResourceEvent;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.MRAppMaster;
import org.apache.hadoop.mapreduce.v2.app.client.ClientService;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobCounterUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobDiagnosticsUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobUpdatedNodesEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptContainerAssignedEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptDiagnosticsUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskAttemptKillEvent;
import org.apache.hadoop.mapreduce.v2.app.rm.preemption.AMPreemptionPolicy;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringInterner;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.concurrent.SubjectInheritingThread;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NMToken;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.PreemptionMessage;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.client.api.NMTokenCache;
import org.apache.hadoop.yarn.exceptions.ApplicationAttemptNotFoundException;
import org.apache.hadoop.yarn.exceptions.ApplicationMasterNotRegisteredException;
import org.apache.hadoop.yarn.exceptions.InvalidLabelResourceRequestException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.RackResolver;
import org.apache.hadoop.yarn.util.resource.Resources;

import org.apache.hadoop.classification.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MapReduce ApplicationMaster向YARN ResourceManager申请容器的核心实现类，负责Map和Reduce任务的容器调度分配。
 * 实现了慢启动、抢占式调度、数据本地性分配等策略，合理管理集群资源使用。
 */
public class RMContainerAllocator extends RMContainerRequestor
    implements ContainerAllocator {

  private static final Logger LOG = LoggerFactory.getLogger(RMContainerAllocator.class);
  
  public static final 
  float DEFAULT_COMPLETED_MAPS_PERCENT_FOR_REDUCE_SLOWSTART = 0.05f;
  
  static final Priority PRIORITY_FAST_FAIL_MAP;
  static final Priority PRIORITY_REDUCE;
  static final Priority PRIORITY_MAP;
  static final Priority PRIORITY_OPPORTUNISTIC_MAP;

  @VisibleForTesting
  public static final String RAMPDOWN_DIAGNOSTIC = "Reducer preempted "
      + "to make room for pending map attempts";

  private SubjectInheritingThread eventHandlingThread;
  private final AtomicBoolean stopped;

  static {
    // 初始化不同任务类型的调度优先级：优先级数值越小优先级越低
    PRIORITY_FAST_FAIL_MAP = RecordFactoryProvider.getRecordFactory(null).newRecordInstance(Priority.class);
    PRIORITY_FAST_FAIL_MAP.setPriority(5);
    PRIORITY_REDUCE = RecordFactoryProvider.getRecordFactory(null).newRecordInstance(Priority.class);
    PRIORITY_REDUCE.setPriority(10);
    PRIORITY_MAP = RecordFactoryProvider.getRecordFactory(null).newRecordInstance(Priority.class);
    PRIORITY_MAP.setPriority(20);
    PRIORITY_OPPORTUNISTIC_MAP =
        RecordFactoryProvider.getRecordFactory(null).newRecordInstance(
            Priority.class);
    PRIORITY_OPPORTUNISTIC_MAP.setPriority(19);
  }
  
  /*
  Vocabulary Used: 
  pending -> requests which are NOT yet sent to RM
  scheduled -> requests which are sent to RM but not yet assigned
  assigned -> requests which are assigned to a container
  completed -> request corresponding to which container has completed
  
  Lifecycle of map
  scheduled->assigned->completed
  
  Lifecycle of reduce
  pending->scheduled->assigned->completed
  
  Maps are scheduled as soon as their requests are received. Reduces are 
  added to the pending and are ramped up (added to scheduled) based 
  on completed maps and current availability in the cluster.
  */
  
  // 等待调度的Reduce容器请求队列
  private final LinkedList<ContainerRequest> pendingReduces = 
    new LinkedList<ContainerRequest>();

  // 已分配容器信息管理，按任务尝试分组保存
  private final AssignedRequests assignedRequests;

  // 已发送给RM但尚未分配的容器请求管理
  private final ScheduledRequests scheduledRequests = new ScheduledRequests();

  private int containersAllocated = 0;
  private int containersReleased = 0;
  private int hostLocalAssigned = 0;
  private int rackLocalAssigned = 0;
  private int lastCompletedTasks = 0;
  
  // 标记是否需要重新计算Reduce调度计划
  private boolean recalculateReduceSchedule = false;
  // Map任务单个容器资源请求量
  private Resource mapResourceRequest = Resources.none();
  // Reduce任务单个容器资源请求量
  private Resource reduceResourceRequest = Resources.none();
  
  private boolean reduceStarted = false;
  private float maxReduceRampupLimit = 0;
  private float maxReducePreemptionLimit = 0;

  // mapper分配超时时间，超时后强制抢占reducer容器
  private long reducerUnconditionalPreemptionDelayMs;

  // 无可用资源时，抢占reducer前的等待时间
  private long reducerNoHeadroomPreemptionDelayMs = 0;

  // Reduce慢启动阈值：完成map占比达到该值才开始调度reduce
  private float reduceSlowStart = 0;
  // 最大同时运行map任务数
  private int maxRunningMaps = 0;
  // 最大同时运行reduce任务数
  private int maxRunningReduces = 0;
  // RM连接重试间隔
  private long retryInterval;
  // 重试开始时间
  private long retrystartTime;
  // 时钟工具
  private Clock clock;

  // 容器抢占策略
  private final AMPreemptionPolicy preemptionPolicy;

  @VisibleForTesting
  protected BlockingQueue<ContainerAllocatorEvent> eventQueue
    = new LinkedBlockingQueue<ContainerAllocatorEvent>();

  // 调度统计信息
  private ScheduleStats scheduleStats = new ScheduleStats();

  // Map任务节点标签表达式
  private String mapNodeLabelExpression;

  // Reduce任务节点标签表达式
  private String reduceNodeLabelExpression;

  /**
   * 构造函数，初始化容器分配器
   * @param clientService 客户端服务
   * @param context MR应用上下文
   * @param preemptionPolicy 容器抢占策略
   */
  public RMContainerAllocator(ClientService clientService, AppContext context,
      AMPreemptionPolicy preemptionPolicy) {
    super(clientService, context);
    this.preemptionPolicy = preemptionPolicy;
    this.stopped = new AtomicBoolean(false);
    this.clock = context.getClock();
    this.assignedRequests = createAssignedRequests();
  }

  /**
   * 创建已分配容器请求管理对象
   * @return 已分配容器请求管理对象
   */
  protected AssignedRequests createAssignedRequests() {
    return new AssignedRequests();
  }

  @Override
  /**
   * 服务初始化，从配置加载调度参数
   * @param conf 配置对象
   * @throws Exception 初始化异常
   */
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    reduceSlowStart = conf.getFloat(
        MRJobConfig.COMPLETED_MAPS_FOR_REDUCE_SLOWSTART, 
        DEFAULT_COMPLETED_MAPS_PERCENT_FOR_REDUCE_SLOWSTART);
    maxReduceRampupLimit = conf.getFloat(
        MRJobConfig.MR_AM_JOB_REDUCE_RAMPUP_UP_LIMIT, 
        MRJobConfig.DEFAULT_MR_AM_JOB_REDUCE_RAMP_UP_LIMIT);
    maxReducePreemptionLimit = conf.getFloat(
        MRJobConfig.MR_AM_JOB_REDUCE_PREEMPTION_LIMIT,
        MRJobConfig.DEFAULT_MR_AM_JOB_REDUCE_PREEMPTION_LIMIT);
    reducerUnconditionalPreemptionDelayMs = 1000 * conf.getInt(
        MRJobConfig.MR_JOB_REDUCER_UNCONDITIONAL_PREEMPT_DELAY_SEC,
        MRJobConfig.DEFAULT_MR_JOB_REDUCER_UNCONDITIONAL_PREEMPT_DELAY_SEC);
    reducerNoHeadroomPreemptionDelayMs = conf.getInt(
        MRJobConfig.MR_JOB_REDUCER_PREEMPT_DELAY_SEC,
        MRJobConfig.DEFAULT_MR_JOB_REDUCER_PREEMPT_DELAY_SEC) * 1000;//sec -> ms
    maxRunningMaps = conf.getInt(MRJobConfig.JOB_RUNNING_MAP_LIMIT,
        MRJobConfig.DEFAULT_JOB_RUNNING_MAP_LIMIT);
    maxRunningReduces = conf.getInt(MRJobConfig.JOB_RUNNING_REDUCE_LIMIT,
        MRJobConfig.DEFAULT_JOB_RUNNING_REDUCE_LIMIT);
    RackResolver.init(conf);
    retryInterval = getConfig().getLong(MRJobConfig.MR_AM_TO_RM_WAIT_INTERVAL_MS,
                                MRJobConfig.DEFAULT_MR_AM_TO_RM_WAIT_INTERVAL_MS);
    mapNodeLabelExpression = conf.get(MRJobConfig.MAP_NODE_LABEL_EXP);
    reduceNodeLabelExpression = conf.get(MRJobConfig.REDUCE_NODE_LABEL_EXP);
    // 初始化重试开始时间，首次联系RM成功后会重置
    retrystartTime = System.currentTimeMillis();
    this.scheduledRequests.setNumOpportunisticMapsPercent(
        conf.getInt(MRJobConfig.MR_NUM_OPPORTUNISTIC_MAPS_PERCENT,
            MRJobConfig.DEFAULT_MR_NUM_OPPORTUNISTIC_MAPS_PERCENT));
    LOG.info(this.scheduledRequests.getNumOpportunisticMapsPercent() +
        "% of the mappers will be scheduled using OPPORTUNISTIC containers");
  }

  @Override
  /**
   * 启动服务，启动事件处理线程
   * @throws Exception 启动异常
   */
  protected void serviceStart() throws Exception {
    this.eventHandlingThread = new SubjectInheritingThread() {
      @SuppressWarnings("unchecked")
      @Override
      public void work() {

        ContainerAllocatorEvent event;

        while (!stopped.get() && !Thread.currentThread().isInterrupted()) {
          try {
            // 从事件队列阻塞获取事件
            event = RMContainerAllocator.this.eventQueue.take();
          } catch (InterruptedException e) {
            if (!stopped.get()) {
              LOG.error("Returning, interrupted : " + e);
            }
            return;
          }

          try {
            // 处理容器分配事件
            handleEvent(event);
          } catch (Throwable t) {
            LOG.error("Error in handling event type " + event.getType()
                + " to the ContainreAllocator", t);
            // 事件处理异常，终止应用
            eventHandler.handle(new JobEvent(getJob().getID(),
              JobEventType.INTERNAL_ERROR));
            return;
          }
        }
      }
    };
    this.eventHandlingThread.start();
    super.serviceStart();
  }

  @Override
  /**
   * 心跳处理，每次心跳触发一次调度计算
   * @throws Exception 心跳处理异常
   */
  protected synchronized void heartbeat() throws Exception {
    scheduleStats.updateAndLogIfChanged("Before Scheduling: ");
    // 获取RM新分配的容器
    List<Container> allocatedContainers = getResources();
    if (allocatedContainers != null && allocatedContainers.size() > 0) {
      // 将分配到的容器分配给待调度任务
      scheduledRequests.assign(allocatedContainers);
    }

    // 获取已完成任务数
    int completedMaps = getJob().getCompletedMaps();
    int completedTasks = completedMaps + getJob().getCompletedReduces();
    if ((lastCompletedTasks != completedTasks) ||
          (scheduledRequests.maps.size() > 0)) {
      // 任务完成数变化或还有待调度map，需要重新计算reduce调度
      lastCompletedTasks = completedTasks;
      recalculateReduceSchedule = true;
    }

    if (recalculateReduceSchedule) {
      // 如果需要，抢占已运行reduce容器为pending map腾出空间
      boolean reducerPreempted = preemptReducesIfNeeded();

      if (!reducerPreempted) {
        // 本次心跳未发生抢占，才调度新的reduce
        scheduleReduces(getJob().getTotalMaps(), completedMaps,
            scheduledRequests.maps.size(), scheduledRequests.reduces.size(),
            assignedRequests.maps.size(), assignedRequests.reduces.size(),
            mapResourceRequest, reduceResourceRequest, pendingReduces.size(),
            maxReduceRampupLimit, reduceSlowStart);
      }

      recalculateReduceSchedule = false;
    }

    scheduleStats.updateAndLogIfChanged("After Scheduling: ");
  }

  @Override
  /**
   * 停止服务，中断事件处理线程
   * @throws Exception 停止异常
   */
  protected void serviceStop() throws Exception {
    if (stopped.getAndSet(true)) {
      // 已停止直接返回
      return;
    }
    if (eventHandlingThread != null) {
      eventHandlingThread.interrupt();
    }
    super.serviceStop();
    // 打印最终统计信息
    scheduleStats.log("Final Stats: ");
  }

  @Private
  @VisibleForTesting
  AssignedRequests getAssignedRequests() {
    return assignedRequests;
  }

  @Private
  @VisibleForTesting
  ScheduledRequests getScheduledRequests() {
    return scheduledRequests;
  }

  @Private
  @VisibleForTesting
  int getNumOfPendingReduces() {
    return pendingReduces.size();
  }

  /**
   * 获取reduce是否已开始调度
   * @return reduce是否已开始调度
   */
  public boolean getIsReduceStarted() {
    return reduceStarted;
  }
  
  /**
   * 设置reduce开始调度标记
   * @param reduceStarted 是否开始
   */
  public void setIsReduceStarted(boolean reduceStarted) {
    this.reduceStarted = reduceStarted; 
  }

  @Override
  /**
   * 接收容器分配事件，放入事件队列异步处理
   * @param event 容器分配事件
   */
  public void handle(ContainerAllocatorEvent event) {
    int qSize = eventQueue.size();
    if (qSize != 0 && qSize % 1000 == 0) {
      LOG.info("Size of event-queue in RMContainerAllocator is " + qSize);
    }
    int remCapacity = eventQueue.remainingCapacity();
    if (remCapacity < 1000) {
      LOG.warn("Very low remaining capacity in the event-queue "
          + "