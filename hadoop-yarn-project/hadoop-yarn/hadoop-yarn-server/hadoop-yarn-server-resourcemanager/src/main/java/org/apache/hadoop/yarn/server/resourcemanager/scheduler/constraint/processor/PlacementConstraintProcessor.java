// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.processor;

import org.apache.hadoop.util.Lists;
import org.apache.hadoop.yarn.ams.ApplicationMasterServiceContext;
import org.apache.hadoop.yarn.ams.ApplicationMasterServiceProcessor;
import org.apache.hadoop.yarn.ams.ApplicationMasterServiceUtils;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.RejectedSchedulingRequest;
import org.apache.hadoop.yarn.api.records.RejectionReason;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceSizing;
import org.apache.hadoop.yarn.api.records.SchedulingRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.resourcemanager.RMContextImpl;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.algorithm.DefaultPlacementAlgorithm;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.api.ConstraintPlacementAlgorithm;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.api.PlacedSchedulingRequest;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.api.SchedulingRequestWithPlacementAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.api.SchedulingResponse;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

/**
 * YARN放置约束处理处理器，实现ApplicationMasterServiceProcessor接口，
 * 负责处理携带放置约束的调度请求，根据约束将容器分配到符合要求的节点上。
 * 主要处理流程：
 * 1. 完成初始化工作，加载配置的放置算法
 * 2. 拦截来自ApplicationMaster的调度请求，提取放置约束
 * 3. 将请求分发到放置规划器计算符合约束的节点
 * 4. 对分配失败的请求进行重试，超过重试次数后标记为拒绝
 */
public class PlacementConstraintProcessor extends AbstractPlacementProcessor {

  /**
   * SchedulingResponse的包装类，添加放置尝试次数和最后尝试节点信息
   */
  static final class Response extends SchedulingResponse {

    private final int placementAttempt;
    private final SchedulerNode attemptedNode;

    private Response(boolean isSuccess, ApplicationId applicationId,
        SchedulingRequest schedulingRequest, int placementAttempt,
        SchedulerNode attemptedNode) {
      super(isSuccess, applicationId, schedulingRequest);
      this.placementAttempt = placementAttempt;
      this.attemptedNode = attemptedNode;
    }
  }

  private static final Logger LOG =
      LoggerFactory.getLogger(PlacementConstraintProcessor.class);

  // 调度分配线程池
  private ExecutorService schedulingThreadPool;
  // 最大重试次数
  private int retryAttempts;
  // 待重试请求按应用分组存储
  private Map<ApplicationId, List<BatchedRequests>> requestsToRetry =
      new ConcurrentHashMap<>();
  // 超过重试次数需拒绝的请求按应用分组存储
  private Map<ApplicationId, List<SchedulingRequest>> requestsToReject =
      new ConcurrentHashMap<>();

  // 批量请求迭代器类型
  private BatchedRequests.IteratorType iteratorType;
  // 放置请求分发器
  private PlacementDispatcher placementDispatcher;


  @Override
  public void init(ApplicationMasterServiceContext amsContext,
      ApplicationMasterServiceProcessor nextProcessor) {
    LOG.info("Initializing Constraint Placement Processor:");
    super.init(amsContext, nextProcessor);

    // 仅使用配置中的第一个放置算法实现类
    List<ConstraintPlacementAlgorithm> instances =
        ((RMContextImpl) amsContext).getYarnConfiguration().getInstances(
            YarnConfiguration.RM_PLACEMENT_CONSTRAINTS_ALGORITHM_CLASS,
            ConstraintPlacementAlgorithm.class);
    ConstraintPlacementAlgorithm algorithm = null;
    if (instances != null && !instances.isEmpty()) {
      algorithm = instances.get(0);
    } else {
      // 未配置时使用默认放置算法
      algorithm = new DefaultPlacementAlgorithm();
    }
    LOG.info("Placement Algorithm [{}]", algorithm.getClass().getName());

    // 从配置读取迭代器类型
    String iteratorName = ((RMContextImpl) amsContext).getYarnConfiguration()
        .get(YarnConfiguration.RM_PLACEMENT_CONSTRAINTS_ALGORITHM_ITERATOR,
            BatchedRequests.IteratorType.SERIAL.name());
    LOG.info("Placement Algorithm Iterator[{}]", iteratorName);
    try {
      iteratorType = BatchedRequests.IteratorType.valueOf(iteratorName);
    } catch (IllegalArgumentException e) {
      throw new YarnRuntimeException(
          "Could not instantiate Placement Algorithm Iterator: ", e);
    }

    // 从配置读取放置算法线程池大小
    int algoPSize = ((RMContextImpl) amsContext).getYarnConfiguration().getInt(
        YarnConfiguration.RM_PLACEMENT_CONSTRAINTS_ALGORITHM_POOL_SIZE,
        YarnConfiguration.DEFAULT_RM_PLACEMENT_CONSTRAINTS_ALGORITHM_POOL_SIZE);
    this.placementDispatcher = new PlacementDispatcher();
    this.placementDispatcher.init(
        ((RMContextImpl)amsContext), algorithm, algoPSize);
    LOG.info("Planning Algorithm pool size [{}]", algoPSize);

    // 从配置读取调度分配线程池大小
    int schedPSize = ((RMContextImpl) amsContext).getYarnConfiguration().getInt(
        YarnConfiguration.RM_PLACEMENT_CONSTRAINTS_SCHEDULER_POOL_SIZE,
        YarnConfiguration.DEFAULT_RM_PLACEMENT_CONSTRAINTS_SCHEDULER_POOL_SIZE);
    this.schedulingThreadPool = Executors.newFixedThreadPool(schedPSize);
    LOG.info("Scheduler pool size [{}]", schedPSize);

    // 从配置读取最大重试次数
    this.retryAttempts =
        ((RMContextImpl) amsContext).getYarnConfiguration().getInt(
            YarnConfiguration.RM_PLACEMENT_CONSTRAINTS_RETRY_ATTEMPTS,
            YarnConfiguration.DEFAULT_RM_PLACEMENT_CONSTRAINTS_RETRY_ATTEMPTS);
    LOG.info("Num retry attempts [{}]", this.retryAttempts);
  }

  @Override
  public void allocate(ApplicationAttemptId appAttemptId,
      AllocateRequest request, AllocateResponse response) throws YarnException {
    // 拷贝调度请求，后续会清空原请求避免重复处理
    List<SchedulingRequest> schedulingRequests =
        new ArrayList<>(request.getSchedulingRequests());
    // 将新收到的请求分发到放置器计算节点
    dispatchRequestsForPlacement(appAttemptId, schedulingRequests);
    // 重新分发之前分配失败的待重试请求
    reDispatchRetryableRequests(appAttemptId);
    // 对已经完成放置计算的请求，提交到调度器尝试分配
    schedulePlacedRequests(appAttemptId);

    // 清空原请求中的调度请求，避免被后续处理器重复处理
    request.setSchedulingRequests(Collections.emptyList());

    // 调用下一个处理器继续处理剩余逻辑
    nextAMSProcessor.allocate(appAttemptId, request, response);

    // 处理拒绝请求，将其添加到分配响应返回给AM
    handleRejectedRequests(appAttemptId, response);
  }

  private void dispatchRequestsForPlacement(ApplicationAttemptId appAttemptId,
      List<SchedulingRequest> schedulingRequests) {
    if (schedulingRequests != null && !schedulingRequests.isEmpty()) {
      SchedulerApplicationAttempt appAttempt =
          scheduler.getApplicationAttempt(appAttemptId);
      String queueName = null;
      if(appAttempt != null) {
        queueName = appAttempt.getQueueName();
      }
      // 获取队列最大资源能力
      Resource maxAllocation =
          scheduler.getMaximumResourceCapability(queueName);
      // 分发前对请求资源做规格归一化
      schedulingRequests.forEach(req -> {
        Resource reqResource = req.getResourceSizing().getResources();
        req.getResourceSizing().setResources(
            this.scheduler.getNormalizedResource(reqResource, maxAllocation));
      });
      // 封装为批量请求，分发到放置 dispatcher
      this.placementDispatcher.dispatch(new BatchedRequests(iteratorType,
          appAttemptId.getApplicationId(), schedulingRequests, 1));
    }
  }

  private void reDispatchRetryableRequests(ApplicationAttemptId appAttId) {
    List<BatchedRequests> reqsToRetry =
        this.requestsToRetry.get(appAttId.getApplicationId());
    if (reqsToRetry != null && !reqsToRetry.isEmpty()) {
      synchronized (reqsToRetry) {
        // 重新分发所有待重试请求
        for (BatchedRequests bReq: reqsToRetry) {
          this.placementDispatcher.dispatch(bReq);
        }
        // 清空重试列表，避免重复分发
        reqsToRetry.clear();
      }
    }
  }

  private void schedulePlacedRequests(ApplicationAttemptId appAttemptId) {
    ApplicationId applicationId = appAttemptId.getApplicationId();
    // 从放置dispatcher拉取已经完成放置计算的请求
    List<PlacedSchedulingRequest> placedSchedulingRequests =
        this.placementDispatcher.pullPlacedRequests(applicationId);
    // 遍历每个已放置请求，在计算出的节点上尝试分配容器
    for (PlacedSchedulingRequest placedReq : placedSchedulingRequests) {
      SchedulingRequest sReq = placedReq.getSchedulingRequest();
      for (SchedulerNode node : placedReq.getNodes()) {
        // 克隆调度请求，避免并发修改问题
        final SchedulingRequest sReqClone =
            SchedulingRequest.newInstance(sReq.getAllocationRequestId(),
                sReq.getPriority(), sReq.getExecutionType(),
                sReq.getAllocationTags(),
                ResourceSizing.newInstance(
                    sReq.getResourceSizing().getResources()),
                sReq.getPlacementConstraint());
        SchedulerApplicationAttempt applicationAttempt =
            this.scheduler.getApplicationAttempt(appAttemptId);
        // 提交分配任务到线程池异步处理
        Runnable task = () -> {
          boolean success =
              scheduler.attemptAllocationOnNode(
                  applicationAttempt, sReqClone, node);
          if (!success) {
            LOG.warn("Unsuccessful allocation attempt [{}] for [{}]",
                placedReq.getPlacementAttempt(), sReqClone);
          }
          // 处理分配结果，记录成功/失败状态
          handleSchedulingResponse(
              new Response(success, applicationId, sReqClone,
              placedReq.getPlacementAttempt(), node));
        };
        this.schedulingThreadPool.submit(task);
      }
    }
  }

  private void handleRejectedRequests(ApplicationAttemptId appAttemptId,
      AllocateResponse response) {
    // 拉取放置阶段被算法拒绝的请求
    List<SchedulingRequestWithPlacementAttempt> rejectedAlgoRequests =
        this.placementDispatcher.pullRejectedRequests(
            appAttemptId.getApplicationId());
    if (rejectedAlgoRequests != null && !rejectedAlgoRequests.isEmpty()) {
      LOG.warn("Following requests of [{}] were rejected by" +
              " the PlacementAlgorithmOutput Algorithm: {}",
          appAttemptId.getApplicationId(), rejectedAlgoRequests);
      // 未超过重试次数的请求，交给结果处理继续重试
      rejectedAlgoRequests.stream()
          .filter(req -> req.getPlacementAttempt() < retryAttempts)
          .forEach(req -> handleSchedulingResponse(
              new Response(false, appAttemptId.getApplicationId(),
                  req.getSchedulingRequest(), req.getPlacementAttempt(),
                  null)));
      // 超过重试次数的请求，直接添加到拒绝列表返回给AM
      ApplicationMasterServiceUtils.addToRejectedSchedulingRequests(response,
          rejectedAlgoRequests.stream()
              .filter(req -> req.getPlacementAttempt() >= retryAttempts)
              .map(sr -> RejectedSchedulingRequest.newInstance(
                  RejectionReason.COULD_NOT_PLACE_ON_NODE,
                  sr.getSchedulingRequest()))
              .collect(Collectors.toList()));
    }
    // 处理放置成功但分配失败且耗尽重试次数的请求
    List<SchedulingRequest> rejectedRequests =
        this.requestsToReject.get(appAttemptId.getApplicationId());
    if (rejectedRequests != null && !rejectedRequests.isEmpty()) {
      synchronized (rejectedRequests) {
        LOG.warn("Following requests of [{}] exhausted all retry attempts " +
                "trying to schedule on placed node: {}",
            appAttemptId.getApplicationId(), rejectedRequests);
        // 添加到拒绝列表返回给AM
        ApplicationMasterServiceUtils.addToRejectedSchedulingRequests(response,
            rejectedRequests.stream()
                .map(sr -> RejectedSchedulingRequest.newInstance(
                    RejectionReason.COULD_NOT_SCHEDULE_ON_NODE, sr))
                .collect(Collectors.toList()));
        rejectedRequests.clear();
      }
    }
  }

  @Override
  public void finishApplicationMaster(ApplicationAttemptId appAttemptId,
      FinishApplicationMasterRequest request,
      FinishApplicationMasterResponse response) {
    // 清理应用在放置dispatcher中的状态
    placementDispatcher.clearApplicationState(appAttemptId.getApplicationId());
    // 清理本地存储的拒绝和重试请求
    requestsToReject.remove(appAttemptId.getApplicationId());
    requestsToRetry.remove(appAttemptId.getApplicationId());
    super.finishApplicationMaster(appAttemptId, request, response);
  }

  private void handleSchedulingResponse(SchedulingResponse schedulerResponse) {
    int placementAttempt = ((Response)schedulerResponse).placementAttempt;
    // 分配失败且未超过最大重试次数，加入重试列表等待下次重试
    if (!schedulerResponse.isSuccess() && placementAttempt < retryAttempts) {
      List<BatchedRequests> reqsToRetry =
          requestsToRetry.computeIfAbsent(
              schedulerResponse.getApplicationId(),
              k -> new ArrayList<>());
      synchronized (reqsToRetry) {
        addToRetryList(schedulerResponse, placementAttempt, reqsToRetry);
      }
      LOG.warn("Going to retry request for application [{}] after [{}]" +
              " attempts: [{}]", schedulerResponse.getApplicationId(),
          placementAttempt, schedulerResponse.getSchedulingRequest());
    } else {
      // 分配失败且已耗尽重试次数，加入拒绝列表
      if (!schedulerResponse.isSuccess()) {
        LOG.warn("Not retrying request for application [{}] after [{}]" +
                " attempts: [{}]", schedulerResponse.getApplicationId(),
            placementAttempt, schedulerResponse.getSchedulingRequest());
        List<SchedulingRequest> reqsToReject =
            requestsToReject.computeIfAbsent(
                schedulerResponse.getApplicationId(),
                k -> new ArrayList<>());
        synchronized (reqsToReject) {
          reqsToReject.add(schedulerResponse.getSchedulingRequest());
        }
      }
    }
  }

  private void addToRetryList(SchedulingResponse schedulerResponse,
      int placementAttempt, List<BatchedRequests> reqsToRetry) {
    boolean isAdded = false;
    // 查找对应重试次数的批量请求，添加到已有批次
    for (BatchedRequests br : reqsToRetry) {
      if (br.getPlacementAttempt() == placementAttempt + 1) {
        br.addToBatch(schedulerResponse.getSchedulingRequest());
        // 将本次尝试失败的节点加入黑名单，避免下次重试再次选择
        br.addToBlacklist(
            schedulerResponse.getSchedulingRequest().getAllocationTags(),
            ((Response) schedulerResponse).attemptedNode);
        isAdded = true;
        break;
      }
    }
    // 不存在对应批次则创建新批次
    if (!isAdded) {
      BatchedRequests br = new BatchedRequests(iteratorType,
          schedulerResponse.getApplicationId(),
          Lists.newArrayList(schedulerResponse.getSchedulingRequest()),
          placementAttempt + 1);
      reqsToRetry.add(br);
      br.addToBlacklist(
          schedulerResponse.getSchedulingRequest().getAllocationTags(),
          ((Response) schedulerResponse).attemptedNode);
    }
  }
}