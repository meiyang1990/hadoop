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

package org.apache.hadoop.yarn.server.resourcemanager;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.yarn.server.metrics.OpportunisticSchedulerMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.distributed.CentralizedOpportunisticContainerAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.yarn.ams.ApplicationMasterServiceContext;
import org.apache.hadoop.yarn.ams.ApplicationMasterServiceProcessor;
import org.apache.hadoop.yarn.ams.ApplicationMasterServiceUtils;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocolPB;
import org.apache.hadoop.yarn.api.protocolrecords
    .FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.api.DistributedSchedulingAMProtocol;
import org.apache.hadoop.yarn.api.impl.pb.service.ApplicationMasterProtocolPBServiceImpl;

import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.DistributedSchedulingAllocateRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.DistributedSchedulingAllocateResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterDistributedSchedulingAMResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;

import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.proto.ApplicationMasterProtocol.ApplicationMasterProtocolService;

import org.apache.hadoop.yarn.server.api.protocolrecords.RemoteNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.distributed.NodeQueueLoadMonitor;


import org.apache.hadoop.yarn.server.resourcemanager.scheduler.distributed.QueueLimitCalculator;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeResourceUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.security.AMRMTokenSecretManager;

import org.apache.hadoop.yarn.server.scheduler.OpportunisticContainerAllocator;
import org.apache.hadoop.yarn.server.scheduler.OpportunisticContainerContext;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * 机会容器分配应用主服务：当YARN集群启用机会调度（集中式或分布式）时，替代普通ApplicationMasterService启动。
 * 扩展了ApplicationMasterService功能，支持分布式调度协议，为客户端（AM和AMRMProxy请求拦截器）提供服务。
 */
public class OpportunisticContainerAllocatorAMService
    extends ApplicationMasterService implements DistributedSchedulingAMProtocol,
    EventHandler<SchedulerEvent> {

  private static final Logger LOG =
      LoggerFactory.getLogger(OpportunisticContainerAllocatorAMService.class);

  /** 节点队列负载监控器，维护各节点负载并选出负载最低节点 */
  private final NodeQueueLoadMonitor nodeMonitor;
  /** 机会容器分配器，负责处理机会容器的分配逻辑 */
  private final OpportunisticContainerAllocator oppContainerAllocator;

  /** 每次分配选择的节点数量，来自配置 */
  private final int numNodes;

  /** 缓存节点列表刷新间隔（毫秒），来自配置 */
  private final long cacheRefreshInterval;
  /** 缓存的最低负载节点列表 */
  private volatile List<RemoteNode> cachedNodes;
  /** 上次缓存更新时间戳 */
  private volatile long lastCacheUpdateTime;

  /**
   * 机会调度AM处理器，处理AM请求中的机会容器分配逻辑，插入到AM处理链中。
   */
  class OpportunisticAMSProcessor implements
      ApplicationMasterServiceProcessor {

    private ApplicationMasterServiceContext context;
    private ApplicationMasterServiceProcessor nextProcessor;

    private YarnScheduler getScheduler() {
      return ((RMContext)context).getScheduler();
    }

    @Override
    public void init(ApplicationMasterServiceContext amsContext,
        ApplicationMasterServiceProcessor next) {
      this.context = amsContext;
      // The AMSProcessingChain guarantees that 'next' is not null.
      this.nextProcessor = next;
    }

    /**
     * 注册应用尝试，初始化机会容器上下文
     */
    @Override
    public void registerApplicationMaster(
        ApplicationAttemptId applicationAttemptId,
        RegisterApplicationMasterRequest request,
        RegisterApplicationMasterResponse response)
        throws IOException, YarnException {
      // 获取当前应用尝试调度信息
      SchedulerApplicationAttempt appAttempt = ((AbstractYarnScheduler)
          getScheduler()).getApplicationAttempt(applicationAttemptId);
      // 若尚未初始化机会容器上下文则创建
      if (appAttempt.getOpportunisticContainerContext() == null) {
        OpportunisticContainerContext opCtx =
            new OpportunisticContainerContext();
        // 设置容器ID生成器，复用应用尝试的容器ID生成逻辑
        opCtx.setContainerIdGenerator(new OpportunisticContainerAllocator
            .ContainerIdGenerator() {
          @Override
          public long generateContainerId() {
            return appAttempt.getAppSchedulingInfo().getNewContainerId();
          }
        });
        // 读取容器令牌过期间隔配置
        int tokenExpiryInterval = getConfig()
            .getInt(YarnConfiguration.RM_CONTAINER_ALLOC_EXPIRY_INTERVAL_MS,
                YarnConfiguration.
                    DEFAULT_RM_CONTAINER_ALLOC_EXPIRY_INTERVAL_MS);
        // 更新分配参数：最小/最大资源能力，令牌过期时间
        opCtx.updateAllocationParams(
            getScheduler().getMinimumResourceCapability(),
            getScheduler().getMaximumResourceCapability(),
            getScheduler().getMinimumResourceCapability(),
            tokenExpiryInterval);
        appAttempt.setOpportunisticContainerContext(opCtx);
      }
      // 传递给下一个处理器继续处理注册逻辑
      nextProcessor.registerApplicationMaster(
          applicationAttemptId, request, response);
    }

    /**
     * 处理allocate请求，分离机会和保障容器，先分配机会容器再处理保障容器
     */
    @Override
    public void allocate(ApplicationAttemptId appAttemptId,
        AllocateRequest request, AllocateResponse response)
        throws YarnException {
      // 将请求按优先级分离为保障容器请求和机会容器请求
      OpportunisticContainerAllocator.PartitionedResourceRequests
          partitionedAsks =
          oppContainerAllocator.partitionAskList(request.getAskList());

      // 获取当前应用尝试调度信息
      SchedulerApplicationAttempt appAttempt =
          ((AbstractYarnScheduler)rmContext.getScheduler())
              .getApplicationAttempt(appAttemptId);

      // 校验应用尝试ID有效性
      if (!appAttempt.getApplicationAttemptId().equals(appAttemptId)){
        LOG.error("Calling allocate on previous or removed or non "
            + "existent application attempt {}", appAttemptId);
        return;
      }

      // 获取机会容器上下文，更新节点列表（最低负载节点）
      OpportunisticContainerContext oppCtx =
          appAttempt.getOpportunisticContainerContext();
      oppCtx.updateNodeList(getLeastLoadedNodes());

      // 为机会容器请求补全节点标签（使用AM所在节点分区）
      if (!partitionedAsks.getOpportunistic().isEmpty()) {
        String appPartition = appAttempt.getAppAMNodePartitionName();

        for (ResourceRequest req : partitionedAsks.getOpportunistic()) {
          if (null == req.getNodeLabelExpression()) {
            req.setNodeLabelExpression(appPartition);
          }
        }
      }

      // 执行机会容器分配
      List<Container> oppContainers =
          oppContainerAllocator.allocateContainers(
              request.getResourceBlacklistRequest(),
              partitionedAsks.getOpportunistic(), appAttemptId, oppCtx,
              ResourceManager.getClusterTimeStamp(), appAttempt.getUser());

      // 分配成功后处理：创建RM容器、更新指标、添加令牌、加入响应
      if (!oppContainers.isEmpty()) {
        OpportunisticSchedulerMetrics schedulerMetrics =
            OpportunisticSchedulerMetrics.getMetrics();
        schedulerMetrics.incrAllocatedOppContainers(oppContainers.size());
        handleNewContainers(oppContainers, false);
        appAttempt.updateNMTokens(oppContainers);
        ApplicationMasterServiceUtils.addToAllocatedContainers(
            response, oppContainers);
      }

      // 将剩余保障容器请求传递给下一个处理器处理
      request.setAskList(partitionedAsks.getGuaranteed());
      nextProcessor.allocate(appAttemptId, request, response);
    }

    @Override
    public void finishApplicationMaster(
        ApplicationAttemptId applicationAttemptId,
        FinishApplicationMasterRequest request,
        FinishApplicationMasterResponse response) {
      // 传递给下一个处理器处理完成逻辑
      nextProcessor.finishApplicationMaster(applicationAttemptId,
          request, response);
    }
  }

  /**
   * 构造机会容器分配AM服务，初始化负载监控和分配器
   * @param rmContext RM上下文
   * @param scheduler YARN调度器
   */
  public OpportunisticContainerAllocatorAMService(RMContext rmContext,
      YarnScheduler scheduler) {
    super(OpportunisticContainerAllocatorAMService.class.getName(),
        rmContext, scheduler);
    // 读取每次心跳最大分配机会容器数量配置
    int maxAllocationsPerAMHeartbeat = rmContext.getYarnConfiguration().getInt(
        YarnConfiguration.OPP_CONTAINER_MAX_ALLOCATIONS_PER_AM_HEARTBEAT,
        YarnConfiguration.
            DEFAULT_OPP_CONTAINER_MAX_ALLOCATIONS_PER_AM_HEARTBEAT);
    // 读取每次分配选择的节点数量配置
    this.numNodes = rmContext.getYarnConfiguration().getInt(
        YarnConfiguration.OPP_CONTAINER_ALLOCATION_NODES_NUMBER_USED,
        YarnConfiguration.DEFAULT_OPP_CONTAINER_ALLOCATION_NODES_NUMBER_USED);
    // 读取节点排序间隔配置，作为缓存刷新间隔
    long nodeSortInterval = rmContext.getYarnConfiguration().getLong(
        YarnConfiguration.NM_CONTAINER_QUEUING_SORTING_NODES_INTERVAL_MS,
        YarnConfiguration.
            DEFAULT_NM_CONTAINER_QUEUING_SORTING_NODES_INTERVAL_MS);
    this.cacheRefreshInterval = nodeSortInterval;
    this.lastCacheUpdateTime = System.currentTimeMillis();
    // 读取负载比较器类型配置
    NodeQueueLoadMonitor.LoadComparator comparator =
        NodeQueueLoadMonitor.LoadComparator.valueOf(
            rmContext.getYarnConfiguration().get(
                YarnConfiguration.NM_CONTAINER_QUEUING_LOAD_COMPARATOR,
                YarnConfiguration.
                    DEFAULT_NM_CONTAINER_QUEUING_LOAD_COMPARATOR));

    // 创建节点队列负载监控器
    NodeQueueLoadMonitor topKSelector =
        new NodeQueueLoadMonitor(nodeSortInterval, comparator, numNodes);

    // 读取阈值计算sigma参数
    float sigma = rmContext.getYarnConfiguration()
        .getFloat(YarnConfiguration.NM_CONTAINER_QUEUING_LIMIT_STDEV,
            YarnConfiguration.DEFAULT_NM_CONTAINER_QUEUING_LIMIT_STDEV);

    int limitMin, limitMax;

    // 根据比较器类型读取不同的阈值上下限配置
    if (comparator == NodeQueueLoadMonitor.LoadComparator.QUEUE_LENGTH ||
        comparator ==
            NodeQueueLoadMonitor.LoadComparator.QUEUE_LENGTH_THEN_RESOURCES) {
      limitMin = rmContext.getYarnConfiguration()
          .getInt(YarnConfiguration.NM_CONTAINER_QUEUING_MIN_QUEUE_LENGTH,
              YarnConfiguration.
                  DEFAULT_NM_CONTAINER_QUEUING_MIN_QUEUE_LENGTH);
      limitMax = rmContext.getYarnConfiguration()
          .getInt(YarnConfiguration.NM_CONTAINER_QUEUING_MAX_QUEUE_LENGTH,
              YarnConfiguration.
                  DEFAULT_NM_CONTAINER_QUEUING_MAX_QUEUE_LENGTH);
    } else {
      limitMin = rmContext.getYarnConfiguration()
          .getInt(
              YarnConfiguration.NM_CONTAINER_QUEUING_MIN_QUEUE_WAIT_TIME_MS,
              YarnConfiguration.
                  DEFAULT_NM_CONTAINER_QUEUING_MIN_QUEUE_WAIT_TIME_MS);
      limitMax = rmContext.getYarnConfiguration()
          .getInt(
              YarnConfiguration.NM_CONTAINER_QUEUING_MAX_QUEUE_WAIT_TIME_MS,
              YarnConfiguration.
                  DEFAULT_NM_CONTAINER_QUEUING_MAX_QUEUE_WAIT_TIME_MS);
    }

    // 初始化阈值计算器
    topKSelector.initThresholdCalculator(sigma, limitMin, limitMax);
    this.nodeMonitor = topKSelector;
    // 创建集中式机会容器分配器
    this.oppContainerAllocator =
        new CentralizedOpportunisticContainerAllocator(
            rmContext.getContainerTokenSecretManager(),
            maxAllocationsPerAMHeartbeat, nodeMonitor);
  }

  @Override
  public Server getServer(YarnRPC rpc, Configuration serverConf,
      InetSocketAddress addr, AMRMTokenSecretManager secretManager) {
    // 如果启用分布式调度，同时暴露分布式调度协议和普通AM协议
    if (YarnConfiguration.isDistSchedulingEnabled(serverConf)) {
      Server server = rpc.getServer(DistributedSchedulingAMProtocol.class, this,
          addr, serverConf, secretManager,
          serverConf.getInt(YarnConfiguration.RM_SCHEDULER_CLIENT_THREAD_COUNT,
              YarnConfiguration.DEFAULT_RM_SCHEDULER_CLIENT_THREAD_COUNT));
      // 为了兼容不支持分布式调度的NM，同时添加普通ApplicationMasterProtocol协议支持
      ((RPC.Server) server).addProtocol(RPC.RpcKind.RPC_PROTOCOL_BUFFER,
          ApplicationMasterProtocolPB.class,
          ApplicationMasterProtocolService.newReflectiveBlockingService(
              new ApplicationMasterProtocolPBServiceImpl(this)));
      return server;
    }
    // 未启用分布式调度，使用父类方法获取服务端
    return super.getServer(rpc, serverConf, addr, secretManager);
  }

  @Override
  protected List<ApplicationMasterServiceProcessor> getProcessorList(
      Configuration conf) {
    // 获取父类处理器列表，添加本服务的机会调度处理器
    List<ApplicationMasterServiceProcessor> retVal =
        super.getProcessorList(conf);
    retVal.add(new OpportunisticAMSProcessor());
    return retVal;
  }

  @Override
  public RegisterDistributedSchedulingAMResponse
      registerApplicationMasterForDistributedScheduling(
      RegisterApplicationMasterRequest request) throws YarnException,
      IOException {
    // 先调用普通注册逻辑
    RegisterApplicationMasterResponse response =
        registerApplicationMaster(request);
    // 构建分布式调度注册响应
    RegisterDistributedSchedulingAMResponse dsResp = recordFactory
        .newRecordInstance(RegisterDistributedSchedulingAMResponse.class);
    dsResp.setRegisterResponse(response);
    dsResp.setMinContainerResource(
        rmContext.getScheduler().getMinimumResourceCapability());
    dsResp.setMaxContainerResource(
        rmContext.getScheduler().getMaximumResourceCapability());
    dsResp.setContainerTokenExpiryInterval(
        getConfig().getInt(
            YarnConfiguration.RM_CONTAINER_ALLOC_EXPIRY_INTERVAL_MS,
            YarnConfiguration.DEFAULT_RM_CONTAINER_ALLOC_EXPIRY_INTERVAL_MS));
    dsResp.setContainerIdStart(
        this.rmContext.getEpoch() << ResourceManager.EPOCH_BIT_SHIFT);

    // 返回用于调度的最低负载节点列表
    dsResp.setNodesForScheduling(getLeastLoadedNodes());
    return dsResp;
  }

  @Override
  public DistributedSchedulingAllocateResponse allocateForDistributedScheduling(
      DistributedSchedulingAllocateRequest request)
      throws YarnException, IOException {
    // 处理分布式调度已经分配好的容器，在RM侧创建对应的RMContainer
    List<Container> dist