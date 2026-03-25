// 这个文件已经全部加上中文注释
/*
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

package org.apache.hadoop.yarn.server.federation.policies.router;

import java.util.List;
import java.util.Map;
import java.util.Collections;

import org.apache.hadoop.yarn.api.protocolrecords.ReservationSubmissionRequest;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.policies.AbstractConfigurableFederationPolicy;
import org.apache.hadoop.yarn.server.federation.policies.FederationPolicyUtils;
import org.apache.hadoop.yarn.server.federation.policies.dao.WeightedPolicyInfo;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyException;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyInitializationException;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterIdInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;

/**
 * YARN联邦路由策略的抽象基类，提供路由策略公共能力，包括初始化校验、输入校验、子集群预过滤等通用逻辑，具体选择逻辑由子类实现。
 */
public abstract class AbstractRouterPolicy extends
    AbstractConfigurableFederationPolicy implements FederationRouterPolicy {

  @Override
  public void validate(WeightedPolicyInfo newPolicyInfo)
      throws FederationPolicyInitializationException {
    super.validate(newPolicyInfo);
    // 获取路由策略的权重配置
    Map<SubClusterIdInfo, Float> newWeights =
        newPolicyInfo.getRouterPolicyWeights();
    // 校验权重配置不能为空
    if (newWeights == null || newWeights.size() < 1) {
      throw new FederationPolicyInitializationException(
          "Weight vector cannot be null/empty.");
    }
  }

  /**
   * 校验并补全应用提交上下文，若未指定队列则设置为默认队列兼容原生YARN行为。
   * @param appSubmissionContext 应用提交上下文
   * @throws FederationPolicyException 上下文为空时抛出异常
   */
  public void validate(ApplicationSubmissionContext appSubmissionContext)
      throws FederationPolicyException {

    if (appSubmissionContext == null) {
      throw new FederationPolicyException(
          "Cannot route an application with null context.");
    }

    // if the queue is not specified we set it to default value, to be
    // compatible with YARN behavior.
    String queue = appSubmissionContext.getQueue();
    if (queue == null) {
      appSubmissionContext.setQueue(YarnConfiguration.DEFAULT_QUEUE_NAME);
    }
  }

  /**
   * 由具体路由策略实现，从预过滤后的子集群集合中选择目标子集群，同时支持应用和预约两种场景的路由。
   *
   * @param queue 应用/预约所属队列
   * @param preSelectSubClusters 预过滤后的可用子集群集合
   * @return 选择出的目标子集群ID
   *
   * @throws YarnException 选择子集群失败时抛出异常
   */
  protected abstract SubClusterId chooseSubCluster(String queue,
      Map<SubClusterId, SubClusterInfo> preSelectSubClusters) throws YarnException;

  /**
   * 根据预约ID预过滤可用子集群，存在预约ID时将范围限定到预约所属子集群。
   *
   * @param reservationId 预约的全局唯一标识
   * @param activeSubClusters 当前所有活跃子集群信息
   * @return 过滤后的可用子集群集合
   * @throws YarnException 未知预约ID时抛出异常
   */
  protected Map<SubClusterId, SubClusterInfo> prefilterSubClusters(
      ReservationId reservationId, Map<SubClusterId, SubClusterInfo> activeSubClusters)
      throws YarnException {

    // if a reservation exists limit scope to the sub-cluster this
    // reservation is mapped to
    if (reservationId != null) {
      // note this might throw YarnException if the reservation is
      // unknown. This is to be expected, and should be handled by
      // policy invoker.
      // 获取状态存储门面，查询预约归属信息
      FederationStateStoreFacade stateStoreFacade =
          getPolicyContext().getFederationStateStoreFacade();
      // 查询预约所属子集群
      SubClusterId resSubCluster = stateStoreFacade.getReservationHomeSubCluster(reservationId);
      // 获取该子集群的当前信息
      SubClusterInfo subClusterInfo = activeSubClusters.get(resSubCluster);
      // 返回只包含该预约所属子集群的单元素集合
      return Collections.singletonMap(resSubCluster, subClusterInfo);
    }

    return activeSubClusters;
  }

  @Override
  public SubClusterId getHomeSubcluster(ApplicationSubmissionContext appContext,
      List<SubClusterId> blackLists) throws YarnException {

    // 校验应用提交上下文，补全默认队列
    validate(appContext);

    // 根据预约ID预过滤可用子集群，基于当前活跃子集群列表
    Map<SubClusterId, SubClusterInfo> filteredSubClusters = prefilterSubClusters(
        appContext.getReservationID(), getActiveSubclusters());

    // 校验过滤后是否存在可用子集群
    FederationPolicyUtils.validateSubClusterAvailability(filteredSubClusters.keySet(), blackLists);

    // 移除黑名单中的子集群
    if (blackLists != null) {
      blackLists.forEach(filteredSubClusters::remove);
    }

    // 调用具体策略选择目标子集群
    return chooseSubCluster(appContext.getQueue(), filteredSubClusters);
  }

  @Override
  public SubClusterId getReservationHomeSubcluster(ReservationSubmissionRequest request)
      throws YarnException {
    // 校验请求不为空
    if (request == null) {
      throw new FederationPolicyException("The ReservationSubmissionRequest cannot be null.");
    }

    // 未指定队列时设置默认队列
    if (request.getQueue() == null) {
      request.setQueue(YarnConfiguration.DEFAULT_QUEUE_NAME);
    }

    // 获取所有活跃子集群作为候选（预约提交场景无需预过滤，直接选择）
    Map<SubClusterId, SubClusterInfo> filteredSubClusters = getActiveSubclusters();

    // 调用具体策略选择目标子集群
    return chooseSubCluster(request.getQueue(), filteredSubClusters);
  }
}