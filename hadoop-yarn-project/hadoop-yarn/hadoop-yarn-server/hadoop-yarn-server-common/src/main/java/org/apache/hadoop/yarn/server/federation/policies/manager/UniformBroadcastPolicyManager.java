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

package org.apache.hadoop.yarn.server.federation.policies.manager;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.hadoop.yarn.server.federation.policies.amrmproxy.BroadcastAMRMProxyPolicy;
import org.apache.hadoop.yarn.server.federation.policies.dao.WeightedPolicyInfo;
import org.apache.hadoop.yarn.server.federation.policies.router.UniformRandomRouterPolicy;

/**
 * YARN联邦环境下的均匀广播策略管理器，实现了FederationPolicyManager接口。
 * 组合均匀随机路由策略和广播AMRM代理策略，将作业负载均匀分散到各个子集群。
 * 该策略会将所有请求广播到所有子集群，可能会对RM造成较大负载，返回超出请求的容器数量。
 */
public class UniformBroadcastPolicyManager extends AbstractPolicyManager {

  /**
   * 构造函数，硬编码绑定路由策略和AMRM代理策略。
   */
  public UniformBroadcastPolicyManager() {
    // this structurally hard-codes two compatible policies for Router and
    // AMRMProxy.
    routerFederationPolicy = UniformRandomRouterPolicy.class;
    amrmProxyFederationPolicy = BroadcastAMRMProxyPolicy.class;
  }

  @Override
  public WeightedPolicyInfo getWeightedPolicyInfo() {
    throw new NotImplementedException(
        "UniformBroadcastPolicyManager does not implement getWeightedPolicyInfo.");
  }

  @Override
  public void setWeightedPolicyInfo(WeightedPolicyInfo weightedPolicyInfo) {
    throw new NotImplementedException(
        "UniformBroadcastPolicyManager does not implement setWeightedPolicyInfo.");
  }

  @Override
  public boolean isSupportWeightedPolicyInfo() {
    return false;
  }
}