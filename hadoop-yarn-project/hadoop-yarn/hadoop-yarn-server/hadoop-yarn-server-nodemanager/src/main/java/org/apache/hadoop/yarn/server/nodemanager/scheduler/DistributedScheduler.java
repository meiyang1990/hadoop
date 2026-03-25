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

package org.apache.hadoop.yarn.server.nodemanager.scheduler;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.api.protocolrecords.DistributedSchedulingAllocateRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.DistributedSchedulingAllocateResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterDistributedSchedulingAMResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.NMToken;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.api.protocolrecords.RemoteNode;
import org.apache.hadoop.yarn.server.nodemanager.amrmproxy.AMRMProxyApplicationContext;
import org.apache.hadoop.yarn.server.nodemanager.amrmproxy.AbstractRequestInterceptor;
import org.apache.hadoop.yarn.server.nodemanager.security.NMTokenSecretManagerInNM;

import org.apache.hadoop.yarn.server.scheduler.OpportunisticContainerAllocator;
import org.apache.hadoop.yarn.server.scheduler.OpportunisticContainerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * <p>分布式调度器运行在NodeManager上，作为AMRMProxy的请求拦截器实现。
 * 核心职责如下：</p>
 * <ul>
 *   <li>拦截ApplicationMasterProtocol调用，从RM侧ClusterMonitor响应中提取调度指令，辅助本节点做出分布式调度决策</li>
 *   <li>调用OpportunisticContainerAllocator为待处理的机会型容器请求分配资源</li>
 * </ul>
 */
public final class DistributedScheduler extends AbstractRequestInterceptor {

  private static final Logger LOG = LoggerFactory
      .getLogger(DistributedScheduler.class);

  private final static RecordFactory RECORD_FACTORY =
      RecordFactoryProvider.getRecordFactory(null);

  private OpportunisticContainerContext oppContainerContext =
      new OpportunisticContainerContext();

  // NodeId到NMToken的映射，从RM响应填充，或按需在本地生成
  private Map<NodeId, NMToken> nodeTokens = new HashMap<>();
  private ApplicationAttemptId applicationAttemptId;
  private OpportunisticContainerAllocator containerAllocator;
  private NMTokenSecretManagerInNM nmSecretManager;
  private String appSubmitter;
  private long rmIdentifier;

  /**
   * 初始化分布式调度拦截器，从应用上下文提取所需参数。
   * @param applicationContext AMRMProxy应用上下文
   */
  public void init(AMRMProxyApplicationContext applicationContext) {
    super.init(applicationContext);
    initLocal(applicationContext.getNMContext().getNodeStatusUpdater()
        .getRMIdentifier(),
        applicationContext.getApplicationAttemptId(),
        applicationContext.getNMContext().getContainerAllocator(),
        applicationContext.getNMContext().getNMTokenSecretManager(),
        applicationContext.getUser());
  }

  @VisibleForTesting
  void initLocal(long rmId, ApplicationAttemptId appAttemptId,
      OpportunisticContainerAllocator oppContainerAllocator,
      NMTokenSecretManagerInNM nmSecretManager, String appSubmitter) {
    this.rmIdentifier = rmId;
    this.applicationAttemptId = appAttemptId;
    this.containerAllocator = oppContainerAllocator;
    this.nmSecretManager = nmSecretManager;
    this.appSubmitter = appSubmitter;

    // 覆盖容器ID生成器，递减生成容器ID
    this.oppContainerContext.setContainerIdGenerator(
        new OpportunisticContainerAllocator.ContainerIdGenerator() {
          @Override
          public long generateContainerId() {
            return this.containerIdCounter.decrementAndGet();
          }
        });
  }

  /**
   * 路由应用注册请求到分布式调度注册方法，提取去除分布式调度信息后的标准响应返回。
   *
   * @param request 注册请求
   * @return 标准注册响应
   * @throws YarnException Yarn异常
   * @throws IO异常
   */
  @Override
  public RegisterApplicationMasterResponse registerApplicationMaster
      (RegisterApplicationMasterRequest request) throws YarnException,
      IOException {
    return registerApplicationMasterForDistributedScheduling(request)
        .getRegisterResponse();
  }

  /**
   * 路由资源分配请求到分布式调度分配方法，提取去除分布式调度信息后的标准响应返回。
   *
   * @param request 分配请求
   * @return 标准分配响应
   * @throws YarnException Yarn异常
   * @throws IOException IO异常
   */
  @Override
  public AllocateResponse allocate(AllocateRequest request) throws
      YarnException, IOException {
    // 包装为分布式调度专用请求
    DistributedSchedulingAllocateRequest distRequest = RECORD_FACTORY
        .newRecordInstance(DistributedSchedulingAllocateRequest.class);
    distRequest.setAllocateRequest(request);
    return allocateForDistributedScheduling(distRequest).getAllocateResponse();
  }

  @Override
  public FinishApplicationMasterResponse finishApplicationMaster
      (FinishApplicationMasterRequest request) throws YarnException,
      IOException {
    // 直接透传完成请求给下一个拦截器
    return getNextInterceptor().finishApplicationMaster(request);
  }

  /**
   * 将本节点分配的机会型容器加入分配响应，对缺失NMToken的分配容器本地生成令牌。
   * @param response 原始分配响应
   * @param nmTokens RM返回的NM令牌列表
   * @param allocatedContainers 本节点分配的容器列表
   */
  private void updateAllocateResponse(AllocateResponse response,
      List<NMToken> nmTokens, List<Container> allocatedContainers) {
    List<NMToken> newTokens = new ArrayList<>();
    if (allocatedContainers.size() > 0) {
      // 将本节点分配的容器加入响应
      response.getAllocatedContainers().addAll(allocatedContainers);
      // 为缺失NMToken的容器生成本地令牌
      for (Container alloc : allocatedContainers) {
        if (!nodeTokens.containsKey(alloc.getNodeId())) {
          newTokens.add(nmSecretManager.generateNMToken(appSubmitter, alloc));
        }
      }
      // 合并RM返回的令牌和本地生成的令牌
      List<NMToken> retTokens = new ArrayList<>(nmTokens);
      retTokens.addAll(newTokens);
      response.setNMTokens(retTokens);
    }
  }

  /**
   * 从RM注册响应中更新调度参数。
   * @param registerResponse RM分布式调度注册响应
   */
  private void updateParameters(
      RegisterDistributedSchedulingAMResponse registerResponse) {
    // 如果增量资源为空，使用最小容器资源作为增量
    Resource incrementResource = registerResponse.getIncrContainerResource();
    if (incrementResource == null) {
      incrementResource = registerResponse.getMinContainerResource();
    }
    // 更新资源分配参数
    oppContainerContext.updateAllocationParams(
        registerResponse.getMinContainerResource(),
        registerResponse.getMaxContainerResource(),
        incrementResource,
        registerResponse.getContainerTokenExpiryInterval());

    // 重置容器ID计数器起始值
    oppContainerContext.getContainerIdGenerator()
        .resetContainerIdCounter(registerResponse.getContainerIdStart());
    // 更新可调度节点列表
    setNodeList(registerResponse.getNodesForScheduling());
  }

  private void setNodeList(List<RemoteNode> nodeList) {
    oppContainerContext.updateNodeList(nodeList);
  }

  @Override
  public RegisterDistributedSchedulingAMResponse
      registerApplicationMasterForDistributedScheduling(
          RegisterApplicationMasterRequest request)
      throws YarnException, IOException {
    LOG.info("Forwarding registration request to the" +
        "Distributed Scheduler Service on YARN RM");
    // 转发请求到下一个拦截器处理
    RegisterDistributedSchedulingAMResponse dsResp = getNextInterceptor()
        .registerApplicationMasterForDistributedScheduling(request);
    // 从响应提取参数更新本地调度上下文
    updateParameters(dsResp);
    return dsResp;
  }

  @Override
  public DistributedSchedulingAllocateResponse allocateForDistributedScheduling(
      DistributedSchedulingAllocateRequest request)
      throws YarnException, IOException {

    // 将容器请求按类型分区：保证型和机会型
    OpportunisticContainerAllocator.PartitionedResourceRequests
        partitionedAsks = containerAllocator
        .partitionAskList(request.getAllocateRequest().getAskList());

    // 本地分配机会型容器
    List<Container> allocatedContainers =
        containerAllocator.allocateContainers(
            request.getAllocateRequest().getResourceBlacklistRequest(),
            partitionedAsks.getOpportunistic(), applicationAttemptId,
            oppContainerContext, rmIdentifier, appSubmitter);

    // 重组请求：仅保留保证型请求转发给RM，已分配的机会型容器本地下发
    request.setAllocatedContainers(allocatedContainers);
    request.getAllocateRequest().setAskList(partitionedAsks.getGuaranteed());

    LOG.debug("Forwarding allocate request to the" +
          "Distributed Scheduler Service on YARN RM");

    // 转发重组后的请求给RM
    DistributedSchedulingAllocateResponse dsResp =
        getNextInterceptor().allocateForDistributedScheduling(request);

    // 更新可调度节点列表
    setNodeList(dsResp.getNodesForScheduling());
    // 缓存RM返回的NM令牌
    List<NMToken> nmTokens = dsResp.getAllocateResponse().getNMTokens();
    for (NMToken nmToken : nmTokens) {
      nodeTokens.put(nmToken.getNodeId(), nmToken);
    }

    // 补全NMToken，将本地分配的容器加入响应
    updateAllocateResponse(
        dsResp.getAllocateResponse(), nmTokens, allocatedContainers);

    return dsResp;
  }
}