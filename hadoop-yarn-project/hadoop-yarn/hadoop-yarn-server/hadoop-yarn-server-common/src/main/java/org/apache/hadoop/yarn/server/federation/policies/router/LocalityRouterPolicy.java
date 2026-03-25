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
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.federation.policies.router;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Collections;

import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.ExecutionTypeRequest;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.policies.FederationPolicyInitializationContext;
import org.apache.hadoop.yarn.server.federation.policies.FederationPolicyUtils;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyException;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyInitializationException;
import org.apache.hadoop.yarn.server.federation.resolver.SubClusterResolver;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterIdInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * YARN联邦环境下基于应用本地性的路由策略，根据应用期望运行的节点位置选择对应子集群。
 * 
 * 正常选中外置子集群的条件：
 * 1. AM容器资源请求正好有3个，按顺序为NODE、RACK、ANY
 * 
 * 回退到加权随机路由策略的场景：
 * 1. AM容器资源请求为空或null
 * 2. 仅有1个资源请求且资源位置为ANY
 * 3. 请求节点所在子集群在黑名单中
 * 
 * 校验失败场景：
 * 1. 请求节点不存在且开启了严格本地址（不允许放宽本地址）
 * 2. 资源请求数量无效（不是0、1、3）
 */
public class LocalityRouterPolicy extends WeightedRandomRouterPolicy {

  public static final Logger LOG =
      LoggerFactory.getLogger(LocalityRouterPolicy.class);

  private SubClusterResolver resolver;
  private List<SubClusterId> enabledSCs;

  @Override
  public void reinitialize(FederationPolicyInitializationContext policyContext)
      throws FederationPolicyInitializationException {
    // 调用父类完成初始化
    super.reinitialize(policyContext);
    // 获取子集群解析器，用于解析节点/rack对应的子集群
    resolver = policyContext.getFederationSubclusterResolver();
    // 获取配置中所有子集群的权重信息
    Map<SubClusterIdInfo, Float> weights =
        getPolicyInfo().getRouterPolicyWeights();
    // 初始化可用子集群列表
    enabledSCs = new ArrayList<>();
    // 遍历权重配置，收集权重大于0的启用子集群
    for (Map.Entry<SubClusterIdInfo, Float> entry : weights.entrySet()) {
      if (entry != null && entry.getValue() > 0) {
        enabledSCs.add(entry.getKey().toId());
      }
    }
  }

  @Override
  public SubClusterId getHomeSubcluster(
      ApplicationSubmissionContext appSubmissionContext,
      List<SubClusterId> blackListSubClusters) throws YarnException {

    // 空值校验和默认队列处理
    validate(appSubmissionContext);

    // 获取AM容器的资源请求列表
    List<ResourceRequest> rrList =
        appSubmissionContext.getAMContainerResourceRequests();

    // 满足回退条件，直接走父类加权随机路由
    if (rrList == null || rrList.isEmpty() || (rrList.size() == 1
        && ResourceRequest.isAnyLocation(rrList.get(0).getResourceName()))) {
      return super.getHomeSubcluster(appSubmissionContext, blackListSubClusters);
    }

    // 资源请求数量不等于3，抛出异常
    if (rrList.size() != 3) {
      throw new FederationPolicyException(
          "Invalid number of resource requests: " + rrList.size());
    }

    // 获取所有活跃子集群信息
    Map<SubClusterId, SubClusterInfo> activeSubClusters = getActiveSubclusters();
    // 获取活跃子集群ID集合
    Set<SubClusterId> validSubClusters = activeSubClusters.keySet();
    // 校验子集群可用性，确保至少有可用子集群
    FederationPolicyUtils.validateSubClusterAvailability(activeSubClusters.keySet(),
        blackListSubClusters);

    if (blackListSubClusters != null) {
      // 从可用集合中移除黑名单内的子集群
      validSubClusters.removeAll(blackListSubClusters);
    }

    try {
      // 三个资源请求分别对应node、rack、any，由ResourceRequestInterceptorREST预处理
      SubClusterId targetId = null;
      ResourceRequest nodeRequest = null;
      ResourceRequest rackRequest = null;
      ResourceRequest anyRequest = null;

      // 遍历解析每个资源请求，分类存储
      for (ResourceRequest rr : rrList) {
        // 尝试解析节点对应的子集群
        try {
          targetId = resolver.getSubClusterForNode(rr.getResourceName());
          nodeRequest = rr;
        } catch (YarnException e) {
          LOG.error("Cannot resolve node : {}.", e.getMessage());
        }
        // 尝试解析rack对应的子集群
        try {
          resolver.getSubClustersForRack(rr.getResourceName());
          rackRequest = rr;
        } catch (YarnException e) {
          LOG.error("Cannot resolve rack : {}.", e.getMessage());
        }
        // 标识ANY位置请求
        if (ResourceRequest.isAnyLocation(rr.getResourceName())) {
          anyRequest = rr;
          continue;
        }
      }

      // 校验三个请求都存在
      if (nodeRequest == null) {
        throw new YarnException("Missing node request.");
      }
      if (rackRequest == null) {
        throw new YarnException("Missing rack request.");
      }
      if (anyRequest == null) {
        throw new YarnException("Missing any request.");
      }

      LOG.info("Node request: {} , Rack request: {} , Any request: {}.",
          nodeRequest.getResourceName(), rackRequest.getResourceName(),
          anyRequest.getResourceName());

      // 检查节点所在子集群是否可用（活跃且不在黑名单、已启用）
      if (validSubClusters.contains(targetId) && enabledSCs
          .contains(targetId)) {
        LOG.info("Node {} is in SubCluster: {}.", nodeRequest.getResourceName(), targetId);
        // 返回对应子集群作为AM运行位置
        return targetId;
      } else {
        throw new YarnException("The node " + nodeRequest.getResourceName()
            + " is in a blacklist SubCluster or not active. ");
      }
    } catch (YarnException e) {
      LOG.error("Validating resource requests failed, " +
          "Falling back to WeightedRandomRouterPolicy placement : {}.", e.getMessage());
      // 校验失败，回退到加权随机路由，替换请求为默认ANY位置
      ResourceRequest amReq = Records.newRecord(ResourceRequest.class);
      amReq.setPriority(appSubmissionContext.getPriority());
      amReq.setResourceName(ResourceRequest.ANY);
      amReq.setCapability(appSubmissionContext.getResource());
      amReq.setNumContainers(1);
      amReq.setRelaxLocality(true);
      amReq.setNodeLabelExpression(appSubmissionContext.getNodeLabelExpression());
      amReq.setExecutionTypeRequest(ExecutionTypeRequest.newInstance(ExecutionType.GUARANTEED));
      appSubmissionContext.setAMContainerResourceRequests(Collections.singletonList(amReq));
      // 调用父类方法选择子集群
      return super.getHomeSubcluster(appSubmissionContext, blackListSubClusters);
    }
  }
}