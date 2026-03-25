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

package org.apache.hadoop.yarn.server.scheduler;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RemoteNode;
import org.apache.hadoop.yarn.server.metrics.OpportunisticSchedulerMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.Objects;

import static org.apache.hadoop.yarn.server.scheduler.OpportunisticContainerAllocator.Allocation;
import static org.apache.hadoop.yarn.server.scheduler.OpportunisticContainerAllocator.AllocationParams;
import static org.apache.hadoop.yarn.server.scheduler.OpportunisticContainerAllocator.ContainerIdGenerator;
import static org.apache.hadoop.yarn.server.scheduler.OpportunisticContainerAllocator.EnrichedResourceRequest;

/**
 * 文件说明：YARN 机会容器调度上下文，封装机会调度器为单个应用分配容器所需的所有应用级上下文信息
 * 核心职责：维护应用级别机会容器请求、节点信息、分配参数等上下文数据，支持待处理请求匹配和分配流程
 */
public class OpportunisticContainerContext {

  private static final Logger LOG = LoggerFactory
      .getLogger(OpportunisticContainerContext.class);

  // 应用分配参数，包含最小/最大/增量资源和容器令牌过期设置
  private AllocationParams appParams =
      new AllocationParams();
  // 容器ID生成器，用于为新分配的机会容器生成唯一ID
  private ContainerIdGenerator containerIdGenerator =
      new ContainerIdGenerator();

  // 集群可用节点列表，支持集中式 placement 优化
  private volatile List<RemoteNode> nodeList = new LinkedList<>();
  // 节点主机名到节点信息的映射缓存
  private final LinkedHashMap<String, RemoteNode> nodeMap =
      new LinkedHashMap<>();

  // 节点黑名单，不分配容器到黑名单中的节点
  private final Set<String> blacklist = new HashSet<>();

  // 待处理的机会容器请求映射，索引顺序为：优先级 -> 资源位置 -> 资源容量
  // 用于将分配得到的容器匹配到对应的未满足资源请求
  private final TreeMap
      <SchedulerRequestKey, Map<Resource, EnrichedResourceRequest>>
      outstandingOpReqs = new TreeMap<>();

  public AllocationParams getAppParams() {
    return appParams;
  }

  public ContainerIdGenerator getContainerIdGenerator() {
    return containerIdGenerator;
  }

  public void setContainerIdGenerator(
      ContainerIdGenerator containerIdGenerator) {
    this.containerIdGenerator = containerIdGenerator;
  }

  public Map<String, RemoteNode> getNodeMap() {
    return Collections.unmodifiableMap(nodeMap);
  }

  /**
   * 更新集群节点列表，重新构建节点主机名映射缓存
   * @param newNodeList 最新的集群可用节点列表
   */
  public synchronized void updateNodeList(List<RemoteNode> newNodeList) {
    // 仅当节点列表发生变更时才更新缓存，优化性能
    if (newNodeList != nodeList) {
      nodeList = newNodeList;
      nodeMap.clear();
      for (RemoteNode n : nodeList) {
        nodeMap.put(n.getNodeId().getHost(), n);
      }
    }
  }

  /**
   * 更新应用分配参数，设置资源范围和容器令牌过期时间
   * @param minResource 最小分配资源
   * @param maxResource 最大分配资源
   * @param incrResource 增量分配步长
   * @param containerTokenExpiryInterval 容器令牌过期间隔
   */
  public void updateAllocationParams(Resource minResource, Resource maxResource,
      Resource incrResource, int containerTokenExpiryInterval) {
    appParams.setMinResource(minResource);
    appParams.setMaxResource(maxResource);
    appParams.setIncrementResource(incrResource);
    appParams.setContainerTokenExpiryInterval(containerTokenExpiryInterval);
  }

  public Set<String> getBlacklist() {
    return blacklist;
  }

  public TreeMap<SchedulerRequestKey, Map<Resource, EnrichedResourceRequest>>
      getOutstandingOpReqs() {
    return outstandingOpReqs;
  }

  /**
   * 将新收到的机会容器资源请求添加到待处理请求映射中
   * 按优先级、资源名称、资源容量建立索引，支持后续分配匹配
   * @param resourceAsks 待添加的资源请求列表
   */
  public void addToOutstandingReqs(List<ResourceRequest> resourceAsks) {
    for (ResourceRequest request : resourceAsks) {
      // 从请求创建调度key，包含优先级和分配请求ID
      SchedulerRequestKey schedulerKey = SchedulerRequestKey.create(request);

      // 从待处理请求获取对应优先级分组
      Map<Resource, EnrichedResourceRequest> reqMap =
          getOutstandingOpReqs().get(schedulerKey);

      // 请求容器数为0，表示取消对应请求
      if (request.getNumContainers() == 0) {
        if (Objects.nonNull(reqMap) &&
                ResourceRequest.isAnyLocation(request.getResourceName())) {
          // 移除对应容量的请求
          reqMap.remove(request.getCapability());
          // 如果分组为空，移除整个key
          if (reqMap.isEmpty()) {
            outstandingOpReqs.remove(schedulerKey);
          }
          continue;
        } else if (Objects.isNull(reqMap) || Objects.isNull(
                reqMap.get(request.getCapability()))) {
          continue;
        }
      }

      // 初始化分组
      if (reqMap == null) {
        reqMap = new HashMap<>();
        getOutstandingOpReqs().put(schedulerKey, reqMap);
      }

      // 获取或创建对应资源容量的增强请求
      EnrichedResourceRequest eReq = reqMap.get(request.getCapability());
      if (eReq == null) {
        eReq = new EnrichedResourceRequest(request);
        reqMap.put(request.getCapability(), eReq);
      }
      // ANY位置请求仅更新容器总数
      if (ResourceRequest.isAnyLocation(request.getResourceName())) {
        eReq.getRequest().setResourceName(ResourceRequest.ANY);
        eReq.getRequest().setNumContainers(request.getNumContainers());
      } else {
        // 特定位置请求添加位置计数
        eReq.addLocation(request.getResourceName(), request.getNumContainers());
      }
      // 打印ANY请求日志，便于调试
      if (ResourceRequest.isAnyLocation(request.getResourceName())) {
        LOG.info("# of outstandingOpReqs in ANY (at "
            + "priority = " + schedulerKey.getPriority()
            + ", allocationReqId = " + schedulerKey.getAllocationRequestId()
            + ", with capability = " + request.getCapability() + " ) : "
            + ", with location = " + request.getResourceName() + " ) : "
            + ", numContainers = " + eReq.getRequest().getNumContainers());
      }
    }
  }

  /**
   * 将已分配的容器匹配到对应待处理请求，扣减待分配容器计数
   * 并记录分配延迟指标
   * @param capability 分配容器的资源容量
   * @param allocations 已分配容器列表
   */
  public void matchAllocationToOutstandingRequest(Resource capability,
      List<Allocation> allocations) {
    for (OpportunisticContainerAllocator.Allocation allocation : allocations) {
      // 从容器提取调度key
      SchedulerRequestKey schedulerKey =
          SchedulerRequestKey.extractFrom(allocation.getContainer());
      // 获取对应优先级的待处理请求分组
      Map<Resource, EnrichedResourceRequest> asks =
          outstandingOpReqs.get(schedulerKey);

      if (asks == null) {
        continue;
      }

      // 获取对应容量的待处理请求
      EnrichedResourceRequest err = asks.get(capability);
      if (err != null) {
        // 扣减待分配容器计数
        int numContainers = err.getRequest().getNumContainers();
        numContainers--;
        err.getRequest().setNumContainers(numContainers);
        // 计数为0则从待处理列表移除
        if (numContainers == 0) {
          asks.remove(capability);
          if (asks.size() == 0) {
            outstandingOpReqs.remove(schedulerKey);
          }
        } else {
          // 非ANY位置分配，移除对应位置的计数
          if (!ResourceRequest.isAnyLocation(allocation.getResourceName())) {
            err.removeLocation(allocation.getResourceName());
          }
        }
        // 记录机会容器分配延迟指标
        getOppSchedulerMetrics().addAllocateOLatencyEntry(
            Time.monotonicNow() - err.getTimestamp());
      }
    }
  }

  @VisibleForTesting
  public OpportunisticSchedulerMetrics getOppSchedulerMetrics() {
    return OpportunisticSchedulerMetrics.getMetrics();
  }
}