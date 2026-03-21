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

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.ams.ApplicationMasterServiceContext;
import org.apache.hadoop.yarn.ams.ApplicationMasterServiceUtils;
import org.apache.hadoop.yarn.ams.ApplicationMasterServiceProcessor;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.CollectorInfo;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerUpdateType;
import org.apache.hadoop.yarn.api.records.EnhancedHeadroom;
import org.apache.hadoop.yarn.api.records.NMToken;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeUpdateType;
import org.apache.hadoop.yarn.api.records.PreemptionContainer;
import org.apache.hadoop.yarn.api.records.PreemptionContract;
import org.apache.hadoop.yarn.api.records.PreemptionMessage;
import org.apache.hadoop.yarn.api.records.PreemptionResourceRequest;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceBlacklistRequest;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.StrictPreemptionContract;
import org.apache.hadoop.yarn.api.records.UpdateContainerError;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.InvalidContainerReleaseException;
import org.apache.hadoop.yarn.exceptions.InvalidResourceBlacklistRequestException;
import org.apache.hadoop.yarn.exceptions.InvalidResourceRequestException;
import org.apache.hadoop.yarn.exceptions.SchedulerInvalidResourceRequestException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.resource.ResourceProfilesManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt
    .RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptRegistrationEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event
    .RMAppAttemptStatusupdateEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event
    .RMAppAttemptUnregistrationEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler
    .AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ContainerUpdates;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler
    .SchedulerNodeReport;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.apache.hadoop.yarn.util.resource.Resources;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.apache.hadoop.yarn.exceptions
        .InvalidResourceRequestException.InvalidResourceType
        .GREATER_THEN_MAX_ALLOCATION;
import static org.apache.hadoop.yarn.exceptions
        .InvalidResourceRequestException.InvalidResourceType.LESS_THAN_ZERO;

/**
 * 默认Application Master服务处理器，必须是AMS处理链的最后一个处理器，
 * 负责处理Application Master注册、资源分配、注销等核心请求的最终处理。
 */
final class DefaultAMSProcessor implements ApplicationMasterServiceProcessor {

  private static final Logger LOG =
      LoggerFactory.getLogger(DefaultAMSProcessor.class);

  private final static List<Container> EMPTY_CONTAINER_LIST =
      new ArrayList<Container>();
  protected static final Allocation EMPTY_ALLOCATION = new Allocation(
      EMPTY_CONTAINER_LIST, Resources.createResource(0), null, null, null);

  private final RecordFactory recordFactory =
      RecordFactoryProvider.getRecordFactory(null);

  private RMContext rmContext;
  private ResourceProfilesManager resourceProfilesManager;
  private boolean timelineServiceV2Enabled;
  private boolean nodelabelsEnabled;
  private Set<String> exclusiveEnforcedPartitions;

  @Override
  public void init(ApplicationMasterServiceContext amsContext,
      ApplicationMasterServiceProcessor nextProcessor) {
    this.rmContext = (RMContext)amsContext;
    this.resourceProfilesManager = rmContext.getResourceProfilesManager();
    this.timelineServiceV2Enabled = YarnConfiguration.
        timelineServiceV2Enabled(rmContext.getYarnConfiguration());
    this.nodelabelsEnabled = YarnConfiguration
        .areNodeLabelsEnabled(rmContext.getYarnConfiguration());
    this.exclusiveEnforcedPartitions = YarnConfiguration
        .getExclusiveEnforcedPartitions(rmContext.getYarnConfiguration());
  }

  @Override
  public void registerApplicationMaster(
      ApplicationAttemptId applicationAttemptId,
      RegisterApplicationMasterRequest request,
      RegisterApplicationMasterResponse response)
      throws IOException, YarnException {

    RMApp app = getRmContext().getRMApps().get(
        applicationAttemptId.getApplicationId());
    LOG.info("AM registration " + applicationAttemptId);
    // 发送AM注册事件到事件分发器处理
    getRmContext().getDispatcher().getEventHandler()
        .handle(
            new RMAppAttemptRegistrationEvent(applicationAttemptId, request
                .getHost(), request.getRpcPort(), request.getTrackingUrl()));
    // 记录AM注册审计日志
    RMAuditLogger.logSuccess(app.getUser(),
        RMAuditLogger.AuditConstants.REGISTER_AM,
        "ApplicationMasterService", app.getApplicationId(),
        applicationAttemptId);
    // 设置队列最大资源能力返回给AM
    response.setMaximumResourceCapability(getScheduler()
        .getMaximumResourceCapability(app.getQueue()));
    // 设置应用ACLs返回给AM
    response.setApplicationACLs(app.getRMAppAttempt(applicationAttemptId)
        .getSubmissionContext().getAMContainerSpec().getApplicationACLs());
    // 设置应用队列名称返回给AM
    response.setQueue(app.getQueue());
    // 安全模式下设置Client到AM令牌主密钥
    if (UserGroupInformation.isSecurityEnabled()) {
      LOG.info("Setting client token master key");
      response.setClientToAMTokenMasterKey(java.nio.ByteBuffer.wrap(
          getRmContext().getClientToAMTokenSecretManager()
          .getMasterKey(applicationAttemptId).getEncoded()));
    }

    // 对于保持容器的AM重启，获取上一次尝试的容器和对应的NM令牌
    if (app.getApplicationSubmissionContext()
        .getKeepContainersAcrossApplicationAttempts()) {
      // 清除密钥管理器中该尝试的节点集合，UAM重启时必须处理，因为复用尝试ID
      rmContext.getNMTokenSecretManager().clearNodeSetForAttempt(applicationAttemptId);
      List<Container> transferredContainers = getScheduler()
          .getTransferredContainers(applicationAttemptId);
      if (!transferredContainers.isEmpty()) {
        response.setContainersFromPreviousAttempts(transferredContainers);

        List<NMToken> nmTokens = new ArrayList<NMToken>();
        // 为每个转移过来的容器生成NM令牌
        for (Container container : transferredContainers) {
          try {
            NMToken token = getRmContext().getNMTokenSecretManager()
                .createAndGetNMToken(app.getUser(), applicationAttemptId,
                    container);
            if (null != token) {
              nmTokens.add(token);
            }
          } catch (IllegalArgumentException e) {
            // DNS解析错误直接抛出，RPC层RMProxy会自动重试
            if (e.getCause() instanceof UnknownHostException) {
              throw (UnknownHostException) e.getCause();
            }
          }
        }
        response.setNMTokensFromPreviousAttempts(nmTokens);
        LOG.info("Application " + app.getApplicationId() + " retrieved "
            + transferredContainers.size() + " containers from previous"
            + " attempts and " + nmTokens.size() + " NM tokens.");
      }
    }

    response.setSchedulerResourceTypes(getScheduler()
        .getSchedulingResourceTypes());
    response.setResourceTypes(ResourceUtils.getResourcesTypeInfo());
    // 启用资源配置文件时返回所有资源配置信息
    if (getRmContext().getYarnConfiguration().getBoolean(
        YarnConfiguration.RM_RESOURCE_PROFILES_ENABLED,
        YarnConfiguration.DEFAULT_RM_RESOURCE_PROFILES_ENABLED)) {
      response.setResourceProfiles(
          resourceProfilesManager.getResourceProfiles());
    }
  }

  @Override
  public void allocate(ApplicationAttemptId appAttemptId,
      AllocateRequest request, AllocateResponse response) throws YarnException {

    // 处理AM进度更新
    handleProgress(appAttemptId, request);

    List<ResourceRequest> ask = request.getAskList();
    List<ContainerId> release = request.getReleaseList();

    ResourceBlacklistRequest blacklistRequest =
        request.getResourceBlacklistRequest();
    List<String> blacklistAdditions =
        (blacklistRequest != null) ?
            blacklistRequest.getBlacklistAdditions() : Collections.emptyList();
    List<String> blacklistRemovals =
        (blacklistRequest != null) ?
            blacklistRequest.getBlacklistRemovals() : Collections.emptyList();
    RMApp app =
        getRmContext().getRMApps().get(appAttemptId.getApplicationId());

    // 为ANY资源请求补上应用默认节点标签表达式
    ApplicationSubmissionContext asc = app.getApplicationSubmissionContext();
    for (ResourceRequest req : ask) {
      if (null == req.getNodeLabelExpression()
          && ResourceRequest.ANY.equals(req.getResourceName())) {
        req.setNodeLabelExpression(asc.getNodeLabelExpression());
      }
      if (ResourceRequest.ANY.equals(req.getResourceName())) {
        // 强制设置分区排他性
        SchedulerUtils.enforcePartitionExclusivity(req,
            exclusiveEnforcedPartitions, asc.getNodeLabelExpression());
      }
    }

    Resource maximumCapacity =
        getScheduler().getMaximumResourceCapability(app.getQueue());

    // 资源请求合法性校验
    try {
      RMServerUtils.normalizeAndValidateRequests(ask,
          maximumCapacity, app.getQueue(),
          getScheduler(), getRmContext(), nodelabelsEnabled);
    } catch (InvalidResourceRequestException e) {
      RMAppAttempt rmAppAttempt = app.getRMAppAttempt(appAttemptId);
      handleInvalidResourceException(e, rmAppAttempt);
    }

    try {
      // 校验黑名单请求合法性
      RMServerUtils.validateBlacklistRequest(blacklistRequest);
    }  catch (InvalidResourceBlacklistRequestException e) {
      LOG.warn("Invalid blacklist request by application " + appAttemptId, e);
      throw e;
    }

    // 不保持容器跨尝试时，校验容器释放请求合法性
    if (!app.getApplicationSubmissionContext()
        .getKeepContainersAcrossApplicationAttempts()) {
      try {
        RMServerUtils.validateContainerReleaseRequest(release, appAttemptId);
      } catch (InvalidContainerReleaseException e) {
        LOG.warn("Invalid container release by application " + appAttemptId,
            e);
        throw e;
      }
    }

    // 拆分容器更新请求，收集更新错误
    List<UpdateContainerError> updateErrors = new ArrayList<>();
    ContainerUpdates containerUpdateRequests =
        RMServerUtils.validateAndSplitUpdateResourceRequests(
            getRmContext(), request, maximumCapacity, updateErrors);

    Allocation allocation;
    RMAppAttemptState state =
        app.getRMAppAttempt(appAttemptId).getAppAttemptState();
    // 应用已进入最终状态，忽略分配请求
    if (state.equals(RMAppAttemptState.FINAL_SAVING) ||
        state.equals(RMAppAttemptState.FINISHING) ||
        app.isAppFinalStateStored()) {
      LOG.warn(appAttemptId + " is in " + state +
          " state, ignore container allocate request.");
      allocation = EMPTY_ALLOCATION;
    } else {
      try {
        // 调用调度器进行资源分配
        allocation = getScheduler().allocate(appAttemptId, ask,
            request.getSchedulingRequests(), release,
            blacklistAdditions, blacklistRemovals, containerUpdateRequests);
      } catch (SchedulerInvalidResourceRequestException e) {
        LOG.warn("Exceptions caught when scheduler handling requests");
        throw new YarnException(e);
      }
    }

    // 日志记录黑名单更新
    if (!blacklistAdditions.isEmpty() || !blacklistRemovals.isEmpty()) {
      LOG.info("blacklist are updated in Scheduler." +
          "blacklistAdditions: " + blacklistAdditions + ", " +
          "blacklistRemovals: " + blacklistRemovals);
    }
    RMAppAttempt appAttempt = app.getRMAppAttempt(appAttemptId);

    // 设置NM令牌到响应
    if (allocation.getNMTokens() != null &&
        !allocation.getNMTokens().isEmpty()) {
      response.setNMTokens(allocation.getNMTokens());
    }

    // 将容器更新错误添加到响应
    ApplicationMasterServiceUtils.addToUpdateContainerErrors(
        response, updateErrors);

    // 处理节点状态更新，添加更新节点报告到响应
    handleNodeUpdates(app, response);

    // 添加新分配容器到响应
    ApplicationMasterServiceUtils.addToAllocatedContainers(
        response, allocation.getContainers());

    // 添加已完成容器状态到响应
    response.setCompletedContainersStatuses(appAttempt
        .pullJustFinishedContainers());
    // 设置可用资源信息
    response.setAvailableResources(allocation.getResourceLimit());

    // 计算并设置增强容量信息（待处理容器数+总vcore数）
    QueueMetrics queueMetrics =
        this.rmContext.getScheduler().getRootQueueMetrics();
    if (queueMetrics != null) {
      int totalVirtualCores =
          queueMetrics.getAllocatedVirtualCores() + queueMetrics
              .getAvailableVirtualCores();
      int pendingContainers = queueMetrics.getPendingContainers();
      response.setEnhancedHeadroom(
          EnhancedHeadroom.newInstance(pendingContainers, totalVirtualCores));
    }

    // 添加容器更新结果和错误到响应
    addToContainerUpdates(response, allocation,
        ((AbstractYarnScheduler)getScheduler())
            .getApplicationAttempt(appAttemptId).pullUpdateContainerErrors());

    // 获取队列默认节点标签，用于计算对应标签下可用节点数
    String label="";
    try {
      label = rmContext.getScheduler()
          .getQueueInfo(app.getQueue(), false, false)
          .getDefaultNodeLabelExpression();
    } catch (Exception e){
      // 动态队列场景下队列可能还不存在，忽略异常
    }

    // 设置集群节点数，带标签时返回对应标签下的活跃节点数
    if (label == null || label.equals("")) {
      response.setNumClusterNodes(getScheduler().getNumClusterNodes());
    } else {
      response.setNumClusterNodes(rmContext.getNodeLabelManager().getActiveNMCountPerLabel(label));
    }

    // 开启Timeline Service V2时添加采集器信息到响应
    if (timelineServiceV2Enabled) {
      CollectorInfo collectorInfo = app.getCollectorInfo();
      if (collectorInfo != null) {
        response.setCollectorInfo(collectorInfo);
      }
    }

    // 生成抢占消息添加到响应
    response.setPreemptionMessage(generatePreemptionMessage(allocation));

    // 设置应用优先级到响应
    response.setApplicationPriority(app
        .getApplicationPriority());

    // 设置上一次尝试保留容器到响应
    response.setContainersFromPreviousAttempts(
        allocation.getPreviousAttemptContainers());

    // 设置被拒绝的调度请求到响应
    response.setRejectedSchedulingRequests(allocation.getRejectedRequest());

  }

  /**
   * 处理非法