// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.yarn.server.federation.policies.router;

import java.util.Map;

import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.policies.FederationPolicyInitializationContext;
import org.apache.hadoop.yarn.server.federation.policies.FederationPolicyInitializationContextValidator;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyException;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyInitializationException;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;

/**
 * 拒绝所有路由请求的YARN联邦路由器策略，用于禁止指定队列的应用在联邦集群中运行。
 * 实现了{@link FederationRouterPolicy}，拒绝所有传入的路由请求，
 * 防止配置了该策略队列的应用调度到联邦集群任何子集群运行。
 */
public class RejectRouterPolicy extends AbstractRouterPolicy {

  /**
   * 重初始化拒绝路由策略，验证上下文并保存策略配置。
   * @param federationPolicyContext 联邦策略初始化上下文
   * @throws FederationPolicyInitializationException 初始化验证失败时抛出异常
   */
  @Override
  public void reinitialize(
      FederationPolicyInitializationContext federationPolicyContext)
      throws FederationPolicyInitializationException {
    // 验证初始化上下文合法性
    FederationPolicyInitializationContextValidator
        .validate(federationPolicyContext, this.getClass().getCanonicalName());
    // 保存策略上下文
    setPolicyContext(federationPolicyContext);
  }

  /**
   * 选择目标子集群，本策略始终抛出异常拒绝所有路由请求。
   * @param queue 目标队列名称
   * @param preSelectSubclusters 候选子集群集合
   * @return 永远不会返回正常结果，直接抛出异常
   * @throws YarnException 总是抛出策略异常拒绝路由
   */
  @Override
  protected SubClusterId chooseSubCluster(
      String queue, Map<SubClusterId, SubClusterInfo> preSelectSubclusters) throws YarnException {
    throw new FederationPolicyException(
        "The policy configured for this queue (" + queue + ") "
        + "reject all routing requests by construction. Application in "
        + queue + " cannot be routed to any RM.");
  }
}