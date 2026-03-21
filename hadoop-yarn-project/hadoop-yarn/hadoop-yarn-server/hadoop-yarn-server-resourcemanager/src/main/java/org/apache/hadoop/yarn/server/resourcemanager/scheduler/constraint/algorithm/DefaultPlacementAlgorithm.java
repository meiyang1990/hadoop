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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.algorithm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceSizing;
import org.apache.hadoop.yarn.api.records.SchedulingRequest;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.InvalidAllocationTagsQueryException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.PlacementConstraintManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.PlacementConstraintsUtil;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.api.ConstraintPlacementAlgorithm;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.api.ConstraintPlacementAlgorithmInput;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.api.ConstraintPlacementAlgorithmOutput;
 collector;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.api.ConstraintPlacementAlgorithmOutputCollector;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.api.PlacedSchedulingRequest;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.api.SchedulingRequestWithPlacementAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.processor.BatchedRequests;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.constraint.processor.NodeCandidateSelector;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * YARN 约束 placement 默认放置算法实现类
 * 支持调度请求级别的多种迭代策略，包括串行、流行标签等策略
 * 核心功能是根据放置约束将容器分配到符合要求的节点上
 */
public class DefaultPlacementAlgorithm implements ConstraintPlacementAlgorithm {

  private static final Logger LOG =
      LoggerFactory.getLogger(DefaultPlacementAlgorithm.class);

  // 单个调度请求的最大重试放置重试次数
  private static final int RE_ATTEMPT_COUNT = 2;

  private LocalAllocationTagsManager tagsManager;
  private PlacementConstraintManager constraintManager;
  private NodeCandidateSelector nodeSelector;
  private ResourceCalculator resourceCalculator;

  @Override
  public void init(RMContext rmContext) {
    // 初始化本地分配标签管理器，封装全局标签管理器
    this.tagsManager = new LocalAllocationTagsManager(
        rmContext.getAllocationTagsManager());
    this.constraintManager = rmContext.getPlacementConstraintManager();
    this.resourceCalculator = rmContext.getScheduler().getResourceCalculator();
    // 初始化节点选择器，从调度器获取符合过滤条件的节点
    this.nodeSelector =
        filter -> ((AbstractYarnScheduler) (rmContext).getScheduler())
            .getNodes(filter);
  }

  /**
   * 尝试在指定节点上放置当前调度请求，检查资源和约束是否满足
   * @param appId 应用ID
   * @param availableResources 节点可用资源
   * @param schedulingRequest 待放置调度请求
   * @param schedulerNode 目标节点
   * @param ignoreResourceCheck 是否忽略资源检查
   * @return 是否可以放置
   * @throws InvalidAllocationTagsQueryException 标签查询异常
   */
  boolean attemptPlacementOnNode(ApplicationId appId,
      Resource availableResources, SchedulingRequest schedulingRequest,
      SchedulerNode schedulerNode, boolean ignoreResourceCheck)
      throws InvalidAllocationTagsQueryException {
    boolean fitsInNode = ignoreResourceCheck ||
        Resources.fitsIn(resourceCalculator,
            schedulingRequest.getResourceSizing().getResources(),
            availableResources);
    boolean constraintsSatisfied =
        PlacementConstraintsUtil.canSatisfyConstraints(appId,
        schedulingRequest, schedulerNode, constraintManager, tagsManager);
    return fitsInNode && constraintsSatisfied;
  }


  @Override
  public void place(ConstraintPlacementAlgorithmInput input,
      ConstraintPlacementAlgorithmOutputCollector collector) {
    // 获取批量放置请求输入
    BatchedRequests requests = (BatchedRequests) input;
    int placementAttempt = requests.getPlacementAttempt();
    // 初始化放置结果输出对象
    ConstraintPlacementAlgorithmOutput resp =
        new ConstraintPlacementAlgorithmOutput(requests.getApplicationId());
    // 获取集群所有节点列表
    List<SchedulerNode> allNodes = nodeSelector.selectNodes(null);

    List<SchedulingRequest> rejectedRequests = new ArrayList<>();
    // 缓存各节点可用资源
    Map<NodeId, Resource> availResources = new HashMap<>();
    int rePlacementCount = RE_ATTEMPT_COUNT;
    // 最多重试 RE_ATTEMPT_COUNT 次放置
    while (rePlacementCount > 0) {
      // 执行批量放置
      doPlacement(requests, resp, allNodes, rejectedRequests, availResources);
      // 验证放置结果，修正冲突
      validatePlacement(requests.getApplicationId(), resp,
          rejectedRequests, availResources);
      // 如果没有失败请求或者已达到重试次数退出循环
      if (rejectedRequests.size() == 0 || rePlacementCount == 1) {
        break;
      }
      // 重新构造批量请求，准备下一轮重试放置失败的请求
      requests = new BatchedRequests(requests.getIteratorType(),
          requests.getApplicationId(), rejectedRequests,
          requests.getPlacementAttempt());
      rejectedRequests = new ArrayList<>();
      rePlacementCount--;
    }

    // 将所有仍失败的请求添加到输出结果
    resp.getRejectedRequests().addAll(
        rejectedRequests.stream().map(
            x -> new SchedulingRequestWithPlacementAttempt(
                placementAttempt, x)).collect(Collectors.toList()));
    // 收集最终结果输出
    collector.collect(resp);
    // 清理本次放置周期的临时容器标签
    this.tagsManager.cleanTempContainers(requests.getApplicationId());
  }

  /**
   * 执行批量调度请求的放置逻辑
   * @param requests 批量待放置请求
   * @param resp 放置结果输出对象
   * @param allNodes 所有可用节点列表
   * @param rejectedRequests 存放放置失败的请求列表
   * @param availableResources 节点可用资源缓存
   */
  private void doPlacement(BatchedRequests requests,
      ConstraintPlacementAlgorithmOutput resp,
      List<SchedulerNode> allNodes,
      List<SchedulingRequest> rejectedRequests,
      Map<NodeId, Resource> availableResources) {
    Iterator<SchedulingRequest> requestIterator = requests.iterator();
    Iterator<SchedulerNode> nIter = allNodes.iterator();
    SchedulerNode lastSatisfiedNode = null;
    // 遍历所有待放置调度请求
    while (requestIterator.hasNext()) {
      if (allNodes.isEmpty()) {
        LOG.warn("No nodes available for placement at the moment !!");
        break;
      }
      SchedulingRequest schedulingRequest = requestIterator.next();
      // 构造已放置请求对象
      PlacedSchedulingRequest placedReq =
          new PlacedSchedulingRequest(schedulingRequest);
      placedReq.setPlacementAttempt(requests.getPlacementAttempt());
      resp.getPlacedRequests().add(placedReq);
      // 构造循环迭代器，从上次满足的节点继续遍历
      CircularIterator<SchedulerNode> nodeIter =
          new CircularIterator(lastSatisfiedNode, nIter, allNodes);
      // 当前请求需要分配的容器数量
      int numAllocs =
          schedulingRequest.getResourceSizing().getNumAllocations();
      // 遍历候选节点，直到分配完所有容器
      while (nodeIter.hasNext() && numAllocs > 0) {
        SchedulerNode node = nodeIter.next();
        try {
          // 获取当前请求第一个分配标签（用于黑名单过滤
          String tag = schedulingRequest.getAllocationTags() == null ? "" :
              schedulingRequest.getAllocationTags().iterator().next();
          // 从缓存获取或初始化节点可用资源
          Resource unallocatedResource =
              availableResources.computeIfAbsent(node.getNodeID(),
                  x -> Resource.newInstance(node.getUnallocatedResource()));
          // 检查节点不在黑名单，且满足放置约束
          if (!requests.getBlacklist(tag).contains(node.getNodeID()) &&
              attemptPlacementOnNode(
                  requests.getApplicationId(), unallocatedResource,
                  schedulingRequest, node, false)) {
            // 减少需要分配数量减一
            schedulingRequest.getResourceSizing()
                .setNumAllocations(--numAllocs);
            // 更新节点可用资源
            Resources.subtractFrom(unallocatedResource,
                schedulingRequest.getResourceSizing().getResources());
            // 将节点添加到已放置请求节点列表
            placedReq.getNodes().add(node);
            // 更新剩余需要分配数量
            numAllocs =
                schedulingRequest.getResourceSizing().getNumAllocations();
            // 为节点添加本次放置的临时标签，用于后续约束检查
            this.tagsManager.addTempTags(node.getNodeID(),
                requests.getApplicationId(),
                schedulingRequest.getAllocationTags());
            lastSatisfiedNode = node;
          }
        } catch (InvalidAllocationTagsQueryException e) {
          LOG.warn("Got exception from TagManager !", e);
        }
      }
    }
    // 将仍有未分配容器的请求添加到拒绝列表
    requests.getSchedulingRequests().stream()
        .filter(sReq -> sReq.getResourceSizing().getNumAllocations() > 0)
        .forEach(rejReq -> rejectedRequests.add(cloneReq(rejReq)));
  }

  /**
   * 批量放置完成后验证所有放置结果，解决请求放置顺序导致的约束冲突
   * 验证逻辑：移除当前请求的临时标签，重新检查约束是否仍然满足，不满足则回滚放置
   * 解决不同请求放置顺序引发的约束冲突问题，例如文中示例中的反亲和性冲突
   * @param applicationId 应用ID
   * @param resp 放置结果对象
   * @param rejectedRequests 存放验证失败需要重新放置的请求列表
   * @param availableResources 节点可用资源缓存
   */
  private void validatePlacement(ApplicationId applicationId,
      ConstraintPlacementAlgorithmOutput resp,
      List<SchedulingRequest> rejectedRequests,
      Map<NodeId, Resource> availableResources) {
    Iterator<PlacedSchedulingRequest> pReqIter =
        resp.getPlacedRequests().iterator();
    // 遍历所有已放置请求
    while (pReqIter.hasNext()) {
      PlacedSchedulingRequest pReq = pReqIter.next();
      Iterator<SchedulerNode> nodeIter = pReq.getNodes().iterator();
      // 统计当前请求验证失败需要重新放置的容器数量
      int num = 0;
      // 遍历该请求分配到的所有节点
      while (nodeIter.hasNext()) {
        SchedulerNode node = nodeIter.next();
        try {
          // 临时移除当前放置添加的临时标签
          this.tagsManager.removeTempTags(node.getNodeID(),
              applicationId, pReq.getSchedulingRequest().getAllocationTags());
          Resource availOnNode = availableResources.get(node.getNodeID());
          // 重新检查约束是否满足
          if (!attemptPlacementOnNode(applicationId, availOnNode,
              pReq.getSchedulingRequest(), node, true)) {
            // 验证不通过，移除该节点分配
            nodeIter.remove();
            num++;
            // 恢复节点可用资源
            Resources.addTo(availOnNode,
                pReq.getSchedulingRequest().getResourceSizing().getResources());
          } else {
            // 验证通过，把标签加回节点
            this.tagsManager.addTempTags(node.getNodeID(),
                applicationId, pReq.getSchedulingRequest().getAllocationTags());
          }
        } catch (InvalidAllocationTagsQueryException e) {
          LOG.warn("Got exception from TagManager !", e);
        }
      }
      // 如果有验证失败的容器，添加到重新放置列表
      if (num > 0) {
        SchedulingRequest sReq = cloneReq(pReq.getSchedulingRequest());
        sReq.getResourceSizing().setNumAllocations(num);
        rejectedRequests.add(sReq);
      }
      // 如果该请求所有容器都验证失败，从已放置列表移除整个请求
      if (pReq.getNodes().isEmpty()) {
        pReqIter.remove();
      }
    }
  }

  /**
   * 克隆调度请求对象，用于重试放置
   * @param sReq 原调度请求
   * @return 新克隆的请求对象
   */
  private static SchedulingRequest cloneReq(SchedulingRequest sReq) {
    return SchedulingRequest.newInstance(
        sReq.getAllocationRequestId(), sReq.getPriority(),
        sReq.getExecutionType(), sReq.getAllocationTags(),
        ResourceSizing.newInstance(
            sReq.getResourceSizing().getNumAllocations(),
            sReq.getResourceSizing().getResources()),
        sReq.getPlacementConstraint());
  }

}