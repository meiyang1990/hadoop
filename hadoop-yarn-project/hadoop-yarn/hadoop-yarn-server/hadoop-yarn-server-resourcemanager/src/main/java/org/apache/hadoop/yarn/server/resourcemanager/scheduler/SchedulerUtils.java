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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.util.Lists;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.exceptions.InvalidLabelResourceRequestException;
import org.apache.hadoop.yarn.exceptions.InvalidResourceRequestException;
import org.apache.hadoop.yarn.exceptions.InvalidResourceRequestException
        .InvalidResourceType;
import org.apache.hadoop.yarn.exceptions.SchedulerInvalidResourceRequestException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.security.AccessType;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.SchedulingMode;
import org.apache.hadoop.yarn.server.scheduler.SchedulerRequestKey;
import org.apache.hadoop.yarn.util.UnitsConversionUtil;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.apache.hadoop.yarn.util.resource.Resources;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.collect.Maps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.yarn.exceptions
        .InvalidResourceRequestException
        .GREATER_THAN_MAX_RESOURCE_MESSAGE_TEMPLATE;
import static org.apache.hadoop.yarn.exceptions
        .InvalidResourceRequestException
        .LESS_THAN_ZERO_RESOURCE_MESSAGE_TEMPLATE;
import static org.apache.hadoop.yarn.exceptions
        .InvalidResourceRequestException.UNKNOWN_REASON_MESSAGE_TEMPLATE;

/**
 * YARN资源调度器通用工具类，提供调度器共享的工具方法。
 */
@Private
@Unstable
public class SchedulerUtils {

  /**
   * 存储资源请求与不合法资源信息的结果封装类，用于队列最大资源校验场景。
   */
  public static class MaxResourceValidationResult {
    private ResourceRequest resourceRequest;
    private List<ResourceInformation> invalidResources;

    MaxResourceValidationResult(ResourceRequest resourceRequest,
        List<ResourceInformation> invalidResources) {
      this.resourceRequest = resourceRequest;
      this.invalidResources = invalidResources;
    }

    public boolean isValid() {
      return invalidResources.isEmpty();
    }

    @Override
    public String toString() {
      return "MaxResourceValidationResult{" + "resourceRequest="
          + resourceRequest + ", invalidResources=" + invalidResources + '}';
    }
  }

  private static final Logger LOG =
      LoggerFactory.getLogger(SchedulerUtils.class);

  private static final RecordFactory recordFactory =
      RecordFactoryProvider.getRecordFactory(null);

  public static final String RELEASED_CONTAINER =
      "Container released by application";

  public static final String UPDATED_CONTAINER =
      "Temporary container killed by application for ExeutionType update";

  public static final String LOST_CONTAINER =
      "Container released on a *lost* node";

  public static final String PREEMPTED_CONTAINER =
      "Container preempted by scheduler";

  public static final String COMPLETED_APPLICATION =
      "Container of a completed application";

  public static final String EXPIRED_CONTAINER =
      "Container expired since it was unused";

  public static final String UNRESERVED_CONTAINER =
      "Container reservation no longer required.";

  /**
   * 创建异常场景下的容器状态对象。
   *
   * @param containerId 异常容器ID
   * @param diagnostics 诊断信息
   * @return 异常容器的状态对象
   */
  public static ContainerStatus createAbnormalContainerStatus(
      ContainerId containerId, String diagnostics) {
    return createAbnormalContainerStatus(containerId,
        ContainerExitStatus.ABORTED, diagnostics);
  }


  /**
   * 创建被杀死容器的状态对象。
   * @param containerId 被杀死容器ID
   * @param diagnostics 诊断信息
   * @return 被杀死容器的状态对象
   */
  public static ContainerStatus createKilledContainerStatus(
      ContainerId containerId, String diagnostics) {
    return createAbnormalContainerStatus(containerId,
        ContainerExitStatus.KILLED_BY_RESOURCEMANAGER, diagnostics);
  }

  /**
   * 创建被抢占容器的状态对象。
   *
   * @param containerId 被抢占容器ID
   * @param diagnostics 诊断信息
   * @return 被抢占容器的状态对象
   */
  public static ContainerStatus createPreemptedContainerStatus(
      ContainerId containerId, String diagnostics) {
    return createAbnormalContainerStatus(containerId,
        ContainerExitStatus.PREEMPTED, diagnostics);
  }

  /**
   * 创建异常容器状态对象的通用私有方法。
   *
   * @param containerId 异常容器ID
   * @param exitStatus 退出状态码
   * @param diagnostics 诊断信息
   * @return 异常容器状态对象
   */
  private static ContainerStatus createAbnormalContainerStatus(
      ContainerId containerId, int exitStatus, String diagnostics) {
    // 创建容器状态实例
    ContainerStatus containerStatus =
        recordFactory.newRecordInstance(ContainerStatus.class);
    containerStatus.setContainerId(containerId);
    containerStatus.setDiagnostics(diagnostics);
    containerStatus.setExitStatus(exitStatus);
    containerStatus.setState(ContainerState.COMPLETE);
    return containerStatus;
  }

  /**
   * 归一化资源请求，确保请求资源是最小资源的倍数且不为零。
   *
   * @param ask 资源请求对象
   * @param resourceCalculator 资源计算器
   * @param minimumResource 最小资源单元
   * @param maximumResource 最大允许资源
   */
  @VisibleForTesting
  public static void normalizeRequest(
    ResourceRequest ask,
    ResourceCalculator resourceCalculator,
    Resource minimumResource,
    Resource maximumResource) {
    ask.setCapability(
        getNormalizedResource(ask.getCapability(), resourceCalculator,
            minimumResource, maximumResource, minimumResource));
  }

  /**
   * 归一化资源，确保请求资源是增量资源的倍数且不超过最大限制。
   *
   * @param ask 原始请求资源
   * @param resourceCalculator 资源计算器
   * @param minimumResource 最小资源单元
   * @param maximumResource 最大允许资源
   * @param incrementResource 增量资源步长
   * @return 归一化后的资源对象
   */
  public static Resource getNormalizedResource(
      Resource ask,
      ResourceCalculator resourceCalculator,
      Resource minimumResource,
      Resource maximumResource,
      Resource incrementResource) {
    Resource normalized = Resources.normalize(
        resourceCalculator, ask, minimumResource,
        maximumResource, incrementResource);
    return normalized;
  }

  /**
   * 归一化资源请求中的节点标签表达式，使用队列默认标签填充空请求。
   * @param resReq 资源请求对象
   * @param queueInfo 队列信息
   */
  private static void normalizeNodeLabelExpressionInRequest(
      ResourceRequest resReq, QueueInfo queueInfo) {

    String labelExp = resReq.getNodeLabelExpression();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Requested Node Label Expression : " + labelExp);
      LOG.debug("Queue Info : " + queueInfo);
    }

    // 如果请求没有指定标签，且请求任意节点，使用队列默认标签表达式
    if (labelExp == null && queueInfo != null && ResourceRequest.ANY
        .equals(resReq.getResourceName())) {
      LOG.debug("Setting default node label expression : {}", queueInfo
          .getDefaultNodeLabelExpression());
      labelExp = queueInfo.getDefaultNodeLabelExpression();
    }

    // 如果仍未指定标签且队列已预配置，设置为无标签
    if (labelExp == null && queueInfo != null) {
      labelExp = RMNodeLabelsManager.NO_LABEL;
    }

    if (labelExp != null) {
      resReq.setNodeLabelExpression(labelExp);
    }
  }

  /**
   * 归一化并验证资源请求，检查节点标签和资源合法性。
   * @param resReq 资源请求对象
   * @param maximumAllocation 集群最大允许分配资源
   * @param queueName 队列名称
   * @param isRecovery 是否是恢复模式（恢复模式跳过验证）
   * @param rmContext RM上下文对象
   * @param queueInfo 队列信息
   * @param nodeLabelsEnabled 集群是否启用节点标签
   * @throws InvalidResourceRequestException 资源请求不合法时抛出
   */
  public static void normalizeAndValidateRequest(ResourceRequest resReq,
      Resource maximumAllocation, String queueName, boolean isRecovery,
      RMContext rmContext, QueueInfo queueInfo, boolean nodeLabelsEnabled)
          throws InvalidResourceRequestException {
    Configuration conf = rmContext.getYarnConfiguration();
    // 节点标签未启用时，如果请求包含标签抛出异常（恢复模式除外）
    if (null != conf && !nodeLabelsEnabled) {
      String labelExp = resReq.getNodeLabelExpression();
      if (!(RMNodeLabelsManager.NO_LABEL.equals(labelExp)
          || null == labelExp)) {
        String message = "NodeLabel is not enabled in cluster, but resource"
            + " request contains a label expression.";
        LOG.warn(message);
        if (!isRecovery) {
          throw new InvalidLabelResourceRequestException(
              "Invalid resource request, node label not enabled "
                  + "but request contains label expression");
        }
      }
    }
    // 如果未传入队列信息，尝试从调度器获取
    if (null == queueInfo) {
      try {
        queueInfo = rmContext.getScheduler().getQueueInfo(queueName, false,
            false);
      } catch (IOException e) {
        // 动态队列场景下，自动创建前获取会失败，忽略异常
      }
    }
    // 归一化节点标签表达式
    SchedulerUtils.normalizeNodeLabelExpressionInRequest(resReq, queueInfo);

    // 非恢复模式下验证资源请求合法性
    if (!isRecovery) {
      validateResourceRequest(resReq, maximumAllocation, queueInfo, rmContext);
    }
  }

  /**
   * 归一化并验证资源请求（默认非恢复模式）。
   * @param resReq 资源请求对象
   * @param maximumAllocation 集群最大允许分配资源
   * @param queueName 队列名称
   * @param rmContext RM上下文对象
   * @param queueInfo 队列信息
   * @param nodeLabelsEnabled 集群是否启用节点标签
   * @throws InvalidResourceRequestException 资源请求不合法时抛出
   */
  public static void normalizeAndValidateRequest(ResourceRequest resReq,
      Resource maximumAllocation, String queueName, RMContext rmContext,
      QueueInfo queueInfo, boolean nodeLabelsEnabled)
          throws InvalidResourceRequestException {
    normalizeAndValidateRequest(resReq, maximumAllocation, queueName, false,
        rmContext, queueInfo, nodeLabelsEnabled);
  }

  /**
   * 强制分区独占性约束，确保请求标签与应用标签一致。
   * 对于强制独占分区：
   * 1) 请求指定分区但应用标签不是该分区，覆盖为应用标签
   * 2) 应用标签是强制分区，强制请求使用应用标签
   * @param resReq 资源请求
   * @param enforcedPartitions 强制独占分区列表
   * @param appLabel 应用的节点标签表达式
   */
  public static void enforcePartitionExclusivity(ResourceRequest resReq,
      Set<String> enforcedPartitions, String appLabel) {
    if (enforcedPartitions == null || enforcedPartitions.isEmpty()) {
      return;
    }
    if (!enforcedPartitions.contains(appLabel)
        && enforcedPartitions.contains(resReq.getNodeLabelExpression())) {
      resReq.setNodeLabelExpression(appLabel);
    }
    if (enforcedPartitions.contains(appLabel)) {
      resReq.setNodeLabelExpression(appLabel);
    }
  }

  /**
   * 验证资源请求合法性，检查资源范围和节点标签权限。
   * @param resReq 资源请求
   * @param maximumAllocation 最大允许分配资源
   * @param queueInfo 队列信息
   * @param rmContext RM上下文
   * @throws InvalidResourceRequestException 资源请求不合法时抛出
   */
  private static void validateResourceRequest(ResourceRequest resReq,
      Resource maximumAllocation, QueueInfo queueInfo, RMContext rmContext)
      throws InvalidResourceRequestException {
    final Resource requestedResource = resReq.getCapability();
    // 检查请求资源不超过最大允许分配、不小于零
    checkResourceRequestAgainstAvailableResource(requestedResource,
        maximumAllocation);

    String labelExp = resReq.getNodeLabelExpression();
    // 不允许在非任意节点请求中指定节点标签
    if (!ResourceRequest.ANY.equals(resReq.getResourceName())
        && labelExp != null && !labelExp.trim().isEmpty()) {
      throw new InvalidLabelResourceRequestException(
          "Invalid resource request, queue=" + queueInfo.getQueueName()
              + " specified node label expression in a "
              + "resource request has resource name = "
              + resReq.getResourceName());
    }

    // 目前不支持多个标签的与表达式
    if (labelExp != null && labelExp.contains("&&")) {
      throw new InvalidLabelResourceRequestException(
          "Invalid resource request, queue=" + queueInfo.getQueueName()
              + " specified more than one node label "
              + "in a node label expression, node label expression = "
              + labelExp);
    }

    // 检查队列是否有权限访问请求的标签
    if (labelExp != null && !labelExp.trim().isEmpty() && queueInfo != null) {
      if (!checkQueueLabelExpression(queueInfo.getAccessibleNodeLabels(),
          labelExp, rmContext)) {
        throw new InvalidLabelResourceRequestException(
            "Invalid resource request" + ", queue=" + queueInfo.getQueueName()
                + " doesn't have permission to access all labels "
                + "in resource request. labelExpression of resource request="
                + labelExp + ". Queue labels="
                + (queueInfo.getAccessibleNodeLabels() == null ? ""
                    : StringUtils.join(
                        queueInfo.getAccessibleNodeLabels().iterator(), ',')));
      } else {
        // 检查集群确实存在请求的标签
        checkQueueLabelInLabelManager(labelExp, rmContext);
      }
    }
  }

  /**
   * 提取资源对象中值为零的资源类型。
   * @param resource 输入资源对象
   * @return 值为零的资源信息映射
   */
  private static Map<String, ResourceInformation> getZeroResources(
      Resource resource) {
    Map<String, ResourceInformation> resourceInformations = Maps.newHashMap();
    int maxLength = ResourceUtils.getNumberOfCountableResourceTypes();

    // 遍历所有可计数资源类型，收集值为零的资源
    for (int i = 0; i < maxLength; i++) {
      ResourceInformation resourceInformation =
          resource.getResourceInformation(i);
      if (resourceInformation.getValue() == 0L) {
        resourceInformations.put(resourceInformation.getName(),
            resourceInformation);
      }
    }
    return resourceInformations;
  }

  @Private
  @VisibleForTesting
  /**
   * 检查资源请求是否在可用资源范围内，不小于零且不超过最大分配。
   * @param reqResource 请求资源
   * @param availableResource 可用最大资源
   * @throws InvalidResourceRequestException 资源不合法时抛出
   */
  static void checkResourceRequestAgainstAvailableResource(Resource reqResource,
      Resource availableResource) throws InvalidResourceRequestException {
    // 遍历所有可计数资源类型逐一检查
    for (int i =