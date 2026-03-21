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

package org.apache.hadoop.yarn.server.federation.policies.amrmproxy;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.policies.FederationPolicyInitializationContext;
import org.apache.hadoop.yarn.server.federation.policies.FederationPolicyInitializationContextValidator;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyException;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyInitializationException;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;

/**
 * 文件说明：YARN联邦环境下AMRMProxy策略实现类，实现了拒绝所有资源请求的路由策略
 * 
 * 核心功能：该策略会拒绝所有传入的资源请求，用于禁止应用访问任何联邦子集群，满足队列级别的权限隔离需求
 */
public class RejectAMRMProxyPolicy extends AbstractAMRMProxyPolicy {

  /**
   * 重新初始化策略，跳过权重校验逻辑
   * @param policyContext 联邦策略初始化上下文
   * @throws FederationPolicyInitializationException 初始化失败时抛出异常
   */
  @Override
  public void reinitialize(FederationPolicyInitializationContext policyContext)
      throws FederationPolicyInitializationException {
    // 重载初始化方法，跳过不适用于本策略的权重校验
    FederationPolicyInitializationContextValidator.validate(policyContext,
        this.getClass().getCanonicalName());
    setPolicyContext(policyContext);
  }

  /**
   * 拆分资源请求到各个子集群，本策略直接拒绝所有请求
   * @param resourceRequests 待分发的资源请求列表
   * @param timedOutSubClusters 已超时的子集群集合
   * @return 拆分后的<子集群, 资源请求列表>映射（本方法不会正常返回）
   * @throws YarnException 直接抛出策略异常拒绝请求
   */
  @Override
  public Map<SubClusterId, List<ResourceRequest>> splitResourceRequests(
      List<ResourceRequest> resourceRequests,
      Set<SubClusterId> timedOutSubClusters) throws YarnException {
    throw new FederationPolicyException("The policy configured for this queue "
        + "rejects all routing requests by construction.");
  }

}