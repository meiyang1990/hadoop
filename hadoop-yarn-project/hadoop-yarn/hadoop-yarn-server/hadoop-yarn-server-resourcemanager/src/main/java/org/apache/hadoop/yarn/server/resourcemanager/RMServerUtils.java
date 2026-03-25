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

package org.apache.hadoop.yarn.server.resourcemanager;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.util.Sets;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.ApplicationTimeoutType;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerUpdateType;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceBlacklistRequest;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.UpdateContainerError;
import org.apache.hadoop.yarn.api.records.UpdateContainerRequest;
import org.apache.hadoop.yarn.api.records.YarnApplicationAttemptState;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.InvalidContainerReleaseException;
import org.apache.hadoop.yarn.exceptions
    .InvalidResourceBlacklistRequestException;
import org.apache.hadoop.yarn.exceptions.InvalidResourceRequestException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.security.YarnAuthorizationProvider;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt
    .RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ContainerUpdates;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler
    .ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler
    .SchedContainerChangeRequest;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.hadoop.yarn.util.Times;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * YARN ResourceManager 服务端工具类，为 REST 和 RPC API 提供公用工具方法
 */
public class RMServerUtils {

  private static final Logger LOG_HANDLE =
      LoggerFactory.getLogger(RMServerUtils.class);

  public static final String UPDATE_OUTSTANDING_ERROR =
      "UPDATE_OUTSTANDING_ERROR";
  private static final String INCORRECT_CONTAINER_VERSION_ERROR =
      "INCORRECT_CONTAINER_VERSION_ERROR";
  private static final String INVALID_CONTAINER_ID =
      "INVALID_CONTAINER_ID";
  public static final String RESOURCE_OUTSIDE_ALLOWED_RANGE =
      "RESOURCE_OUTSIDE_ALLOWED_RANGE";

  protected static final RecordFactory RECORD_FACTORY =
      RecordFactoryProvider.getRecordFactory(null);

  private static Clock clock = SystemClock.getInstance();

  /**
   * 根据指定节点状态集合查询符合条件的RM节点
   * @param context RM上下文
   * @param acceptedStates 接受的节点状态集合
   * @return 符合条件的RM节点列表
   */
  public static List<RMNode> queryRMNodes(RMContext context,
      EnumSet<NodeState> acceptedStates) {
    // 检查查询请求是否包含活跃和非活跃状态节点
    ArrayList<RMNode> results = new ArrayList<RMNode>();
    boolean hasActive = false;
    boolean hasInactive = false;
    for (NodeState nodeState : acceptedStates) {
      if (!hasInactive && nodeState.isInactiveState()) {
        hasInactive = true;
      }
      if (!hasActive && nodeState.isActiveState()) {
        hasActive = true;
      }
      if (hasActive && hasInactive) {
        break;
      }
    }
    // 查询活跃节点列表，过滤符合状态的节点
    if (hasActive) {
      for (RMNode rmNode : context.getRMNodes().values()) {
        if (acceptedStates.contains(rmNode.getState())) {
          results.add(rmNode);
        }
      }
    }

    // 查询非活跃节点列表，过滤符合状态的节点
    if (hasInactive) {
      for (RMNode rmNode : context.getInactiveRMNodes().values()) {
        if ((rmNode != null) && acceptedStates.contains(rmNode.getState())) {
          results.add(rmNode);
        }
      }
    }
    return results;
  }

  /**
   * 验证并拆分容器更新请求，按更新类型分类
   * - 检查同一个请求中是否存在对同一个容器的多次更新
   * - 验证目标资源是否在最大最小分配范围内
   * @param rmContext RM上下文
   * @param request 分配请求
   * @param maximumAllocation 最大分配资源
   * @param updateErrors 容器更新错误列表
   * @return 按类型拆分后的容器更新请求
   */
  public static ContainerUpdates
      validateAndSplitUpdateResourceRequests(RMContext rmContext,
      AllocateRequest request, Resource maximumAllocation,
      List<UpdateContainerError> updateErrors) {
    ContainerUpdates updateRequests =
        new ContainerUpdates();
    Set<ContainerId> outstandingUpdate = new HashSet<>();
    // 遍历所有更新请求逐个验证
    for (UpdateContainerRequest updateReq : request.getUpdateRequests()) {
      // 获取容器信息并验证容器ID和版本
      RMContainer rmContainer = rmContext.getScheduler().getRMContainer(
          updateReq.getContainerId());
      String msg = validateContainerIdAndVersion(outstandingUpdate,
          updateReq, rmContainer);
      ContainerUpdateType updateType = updateReq.getContainerUpdateType();
      if (msg == null) {
        // 处理资源增减类型的更新
        if ((updateType != ContainerUpdateType.PROMOTE_EXECUTION_TYPE) &&
            (updateType !=ContainerUpdateType.DEMOTE_EXECUTION_TYPE)) {
          if (validateIncreaseDecreaseRequest(
              rmContext, updateReq, maximumAllocation)) {
            if (ContainerUpdateType.INCREASE_RESOURCE == updateType) {
              updateRequests.getIncreaseRequests().add(updateReq);
            } else {
              updateRequests.getDecreaseRequests().add(updateReq);
            }
            outstandingUpdate.add(updateReq.getContainerId());
          } else {
            msg = RESOURCE_OUTSIDE_ALLOWED_RANGE;
          }
        } else {
          // 处理执行类型升降级（机会容器<->保障容器）
          ExecutionType original = rmContainer.getExecutionType();
          ExecutionType target = updateReq.getExecutionType();
          if (target != original) {
            if (target == ExecutionType.GUARANTEED &&
                original == ExecutionType.OPPORTUNISTIC) {
              updateRequests.getPromotionRequests().add(updateReq);
              outstandingUpdate.add(updateReq.getContainerId());
            } else if (target == ExecutionType.OPPORTUNISTIC &&
                original == ExecutionType.GUARANTEED) {
              updateRequests.getDemotionRequests().add(updateReq);
              outstandingUpdate.add(updateReq.getContainerId());
            }
          }
        }
      }
      // 如果有错误，添加到错误列表
      checkAndcreateUpdateError(updateErrors, updateReq, rmContainer, msg);
    }
    return updateRequests;
  }

  /**
   * 创建并添加容器更新错误到错误列表
   * @param errors 错误列表
   * @param updateReq 更新请求
   * @param rmContainer 目标容器
   * @param msg 错误原因
   */
  private static void checkAndcreateUpdateError(
      List<UpdateContainerError> errors, UpdateContainerRequest updateReq,
      RMContainer rmContainer, String msg) {
    if (msg != null) {
      UpdateContainerError updateError = RECORD_FACTORY
          .newRecordInstance(UpdateContainerError.class);
      updateError.setReason(msg);
      updateError.setUpdateContainerRequest(updateReq);
      if (rmContainer != null) {
        updateError.setCurrentContainerVersion(
            rmContainer.getContainer().getVersion());
      } else {
        updateError.setCurrentContainerVersion(-1);
      }
      errors.add(updateError);
    }
  }

  /**
   * 验证容器ID和版本是否合法
   * @param outstandingUpdate 当前请求中已处理的待更新容器集合
   * @param updateReq 更新请求
   * @param rmContainer 目标容器
   * @return 错误信息，验证通过返回null
   */
  private static String validateContainerIdAndVersion(
      Set<ContainerId> outstandingUpdate, UpdateContainerRequest updateReq,
      RMContainer rmContainer) {
    String msg = null;
    // 容器不存在，ID无效
    if (rmContainer == null) {
      msg = INVALID_CONTAINER_ID;
    }
    // 请求版本与当前容器版本不匹配
    if (msg == null && updateReq.getContainerVersion() !=
        rmContainer.getContainer().getVersion()) {
      msg = INCORRECT_CONTAINER_VERSION_ERROR;
    }
    // 同一个请求中对同一个容器发起多次更新
    if (msg == null &&
        outstandingUpdate.contains(updateReq.getContainerId())) {
      msg = UPDATE_OUTSTANDING_ERROR;
    }
    return msg;
  }

  /**
   * 归一化并验证资源请求列表，确保请求资源合法
   * @param ask 资源请求列表
   * @param maximumAllocation 最大允许分配资源
   * @param queueName 队列名称
   * @param scheduler YARN调度器
   * @param rmContext RM上下文
   * @param nodeLabelsEnabled 是否启用节点标签功能
   * @throws InvalidResourceRequestException 资源请求无效时抛出
   */
  public static void normalizeAndValidateRequests(List<ResourceRequest> ask,
      Resource maximumAllocation, String queueName, YarnScheduler scheduler,
      RMContext rmContext, boolean nodeLabelsEnabled)
          throws InvalidResourceRequestException {
    // 获取队列信息，动态队列可能不存在，忽略异常
    QueueInfo queueInfo = null;
    try {
      queueInfo = scheduler.getQueueInfo(queueName, false, false);
    } catch (IOException e) {
      //Queue may not exist since it could be auto-created in case of
      // dynamic queues
    }

    // 逐个归一化验证资源请求
    for (ResourceRequest resReq : ask) {
      SchedulerUtils.normalizeAndValidateRequest(resReq, maximumAllocation,
          queueName, rmContext, queueInfo, nodeLabelsEnabled);
    }
  }

  /**
   * 验证容器资源变更请求合法性
   * @param request 容器变更请求
   * @param increase true为增加资源，false为减少资源
   * @throws InvalidResourceRequestException 请求无效时抛出
   */
  public static void checkSchedContainerChangeRequest(
      SchedContainerChangeRequest request, boolean increase)
      throws InvalidResourceRequestException {
    RMContext rmContext = request.getRmContext();
    ContainerId containerId = request.getContainerId();
    RMContainer rmContainer = request.getRMContainer();
    Resource targetResource = request.getTargetCapacity();

    // 获取容器原始资源
    Resource originalResource = rmContainer.getAllocatedResource();

    // 所有资源维度必须满足增减方向要求，不允许部分增部分减
    if (increase) {
      if (originalResource.getMemorySize() > targetResource.getMemorySize()
          || originalResource.getVirtualCores() > targetResource
          .getVirtualCores()) {
        String msg =
            "Trying to increase a container, but target resource has some"
                + " resource < original resource, target=" + targetResource
                + " original=" + originalResource + " containerId="
                + containerId;
        throw new InvalidResourceRequestException(msg);
      }
    } else {
      if (originalResource.getMemorySize() < targetResource.getMemorySize()
          || originalResource.getVirtualCores() < targetResource
          .getVirtualCores()) {
        String msg =
            "Trying to decrease a container, but target resource has "
                + "some resource > original resource, target=" + targetResource
                + " original=" + originalResource + " containerId="
                + containerId;
        throw new InvalidResourceRequestException(msg);
      }
    }

    // 验证目标资源不超过节点总资源
    ResourceScheduler scheduler = rmContext.getScheduler();
    RMNode rmNode = request.getSchedulerNode().getRMNode();
    if (!Resources.fitsIn(scheduler.getResourceCalculator(), targetResource,
        rmNode.getTotalCapability())) {
      String msg = "Target resource=" + targetResource + " of containerId="
          + containerId + " is more than node's total resource="
          + rmNode.getTotalCapability();
      throw new InvalidResourceRequestException(msg);
    }
  }

  /**
   * 验证节点黑名单请求合法性
   * @param blacklistRequest 黑名单请求
   * @throws InvalidResourceBlacklistRequestException 请求无效时抛出
   */
  public static void validateBlacklistRequest(
      ResourceBlacklistRequest blacklistRequest)
      throws InvalidResourceBlacklistRequestException {
    if (blacklistRequest != null) {
      List<String> plus = blacklistRequest.getBlacklistAdditions();
      // 不允许将ANY添加到黑名单，会导致所有节点都不可用
      if (plus != null && plus.contains(ResourceRequest.ANY)) {
        throw new InvalidResourceBlacklistRequestException(
            "Cannot add " + ResourceRequest.ANY + " to the blacklist!");
      }
    }
  }

  /**
   * 验证并归一化容器资源增减请求
   * @param rmContext RM上下文
   * @param request 更新请求
   * @param maximumAllocation 最大允许分配资源
   * @return 验证通过返回true，否则返回false
   */
  private static boolean validateIncreaseDecreaseRequest(RMContext rmContext,
      UpdateContainerRequest request, Resource maximumAllocation) {
    // 检查内存不超出范围
    if (request.getCapability().getMemorySize() < 0
        || request.getCapability().getMemorySize() > maximumAllocation
        .getMemorySize()) {
      return false;
    }
    // 检查vcore不超出范围
    if (request.getCapability().getVirtualCores() < 0
        || request.getCapability().getVirtualCores() > maximumAllocation
        .getVirtualCores()) {
      return false;
    }
    // 归一化资源到调度器要求的粒度
    ResourceScheduler scheduler = rmContext.getScheduler();
    request.setCapability(scheduler
        .getNormalizedResource(request.getCapability(), maximumAllocation));
    return true;
  }

  /**
   * 验证容器释放请求，确保释放的容器都属于当前应用尝试
   * @param containerReleaseList 待释放容器ID列表
   * @param appAttemptId 当前应用尝试ID
   * @throws InvalidContainerReleaseException 存在不属于当前尝试的容器时抛出
   */
  public static void
      validateContainerReleaseRequest(List<ContainerId> containerReleaseList,
      ApplicationAttemptId appAttemptId)
      throws InvalidContainerReleaseException {
    for (ContainerId cId : containerReleaseList) {
      if (!appAttemptId.equals(cId.getApplicationAttemptId())) {
        throw new InvalidContainerReleaseException(
            "Cannot release container : "
                + cId.toString()
                + " not belonging to this application attempt : "
                + appAttemptId);
      }
    }
  }

  /**
   * 验证当前用户是否拥有管理员访问权限，默认使用AdminService作为模块名
   * @param authorizer 授权器
   * @param method 调用方法名
   * @param LOG 日志对象
   * @return 当前