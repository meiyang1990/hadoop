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
import org.apache.hadoop.yarn.server.federation.policies.router.HashBasedRouterPolicy;

/**
 * YARN联邦环境下，基于队列名哈希路由+广播请求的策略管理器，
 * 为路由层和AMRM代理层预先绑定兼容的策略实现。
 * 该策略通过队列名哈希选择目标子集群，同时向所有子集群广播AMRM资源请求。
 */
public class HashBroadcastPolicyManager extends AbstractPolicyManager {

  /**
   * 构造函数，硬编码绑定路由层和AMRM代理层的匹配策略。
   */
  public HashBroadcastPolicyManager() {
    // this structurally hard-codes two compatible policies for Router and
    // AMRMProxy.
    routerFederationPolicy = HashBasedRouterPolicy.class;
    amrmProxyFederationPolicy = BroadcastAMRMProxyPolicy.class;
  }

  @Override
  public WeightedPolicyInfo getWeightedPolicyInfo() {
    throw new NotImplementedException(
        "HashBroadcastPolicyManager does not implement getWeightedPolicyInfo.");
  }

  @Override
  public void setWeightedPolicyInfo(WeightedPolicyInfo weightedPolicyInfo) {
    throw new NotImplementedException(
        "HashBroadcastPolicyManager does not implement setWeightedPolicyInfo.");
  }

  @Override
  public boolean isSupportWeightedPolicyInfo() {
    return false;
  }
}