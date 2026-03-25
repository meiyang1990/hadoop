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

import java.nio.ByteBuffer;

import org.apache.hadoop.yarn.server.federation.policies.amrmproxy.BroadcastAMRMProxyPolicy;
import org.apache.hadoop.yarn.server.federation.policies.dao.WeightedPolicyInfo;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyInitializationException;
import org.apache.hadoop.yarn.server.federation.policies.router.PriorityRouterPolicy;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterPolicyConfiguration;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * YARN联邦优先级广播策略管理器，基于权重进行路由。
 * 路由层使用{@link PriorityRouterPolicy}，AMRMProxy层使用{@link BroadcastAMRMProxyPolicy}，两个策略设计为配合工作。
 */
public class PriorityBroadcastPolicyManager extends AbstractPolicyManager {

  private WeightedPolicyInfo weightedPolicyInfo;

  /**
   * 构造函数，硬编码绑定一对匹配的路由和AMRMProxy策略。
   */
  public PriorityBroadcastPolicyManager() {
    // this structurally hard-codes two compatible policies for Router and
    // AMRMProxy.
    routerFederationPolicy = PriorityRouterPolicy.class;
    amrmProxyFederationPolicy = BroadcastAMRMProxyPolicy.class;
    weightedPolicyInfo = new WeightedPolicyInfo();
  }

  @Override
  public SubClusterPolicyConfiguration serializeConf()
      throws FederationPolicyInitializationException {
    // 将权重策略信息序列化为字节缓冲
    ByteBuffer buf = weightedPolicyInfo.toByteBuffer();
    // 构造并返回序列化后的子集群策略配置
    return SubClusterPolicyConfiguration.newInstance(getQueue(),
        this.getClass().getCanonicalName(), buf);
  }

  @VisibleForTesting
  public WeightedPolicyInfo getWeightedPolicyInfo() {
    return weightedPolicyInfo;
  }

  @VisibleForTesting
  public void setWeightedPolicyInfo(WeightedPolicyInfo weightedPolicyInfo) {
    this.weightedPolicyInfo = weightedPolicyInfo;
  }

  @Override
  public boolean isSupportWeightedPolicyInfo() {
    // 当前策略管理器支持权重策略配置
    return true;
  }
}