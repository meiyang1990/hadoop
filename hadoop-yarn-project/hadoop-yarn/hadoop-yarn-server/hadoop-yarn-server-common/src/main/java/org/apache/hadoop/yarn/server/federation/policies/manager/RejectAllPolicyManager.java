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
import org.apache.hadoop.yarn.server.federation.policies.amrmproxy.RejectAMRMProxyPolicy;
import org.apache.hadoop.yarn.server.federation.policies.router.RejectRouterPolicy;
import org.apache.hadoop.yarn.server.federation.policies.dao.WeightedPolicyInfo;

/**
 * 拒绝所有请求的联邦策略管理器，实现了FederationPolicyManager接口
 * 
 * 该策略会拒绝所有来自Router和AMRMProxy的路由请求，用于禁止特定队列（或作为未配置队列的默认策略）的应用访问集群资源
 */
public class RejectAllPolicyManager extends AbstractPolicyManager {

  /**
   * 构造函数，硬编码绑定拒绝策略
   */
  public RejectAllPolicyManager() {
    // 硬编码绑定Router和AMRMProxy的拒绝策略
    routerFederationPolicy = RejectRouterPolicy.class;
    amrmProxyFederationPolicy = RejectAMRMProxyPolicy.class;
  }

  @Override
  public WeightedPolicyInfo getWeightedPolicyInfo() {
    throw new NotImplementedException(
        "RejectAllPolicyManager does not implement getWeightedPolicyInfo.");
  }

  @Override
  public void setWeightedPolicyInfo(WeightedPolicyInfo weightedPolicyInfo) {
    throw new NotImplementedException(
        "RejectAllPolicyManager does not implement setWeightedPolicyInfo.");
  }

  @Override
  public boolean isSupportWeightedPolicyInfo() {
    return false;
  }
}