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

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.yarn.server.federation.policies.amrmproxy.HomeAMRMProxyPolicy;
import org.apache.hadoop.yarn.server.federation.policies.dao.WeightedPolicyInfo;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyInitializationException;
import org.apache.hadoop.yarn.server.federation.policies.router.WeightedRandomRouterPolicy;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterPolicyConfiguration;

import java.nio.ByteBuffer;

/**
 * 加权归属策略管理器，为YARN联邦路由和AMRMProxy阶段预配置加权随机路由策略和归属子集群AM转发策略
 * 该策略允许集群管理员为不同子集群配置路由权重，将应用提交到应用归属子集群运行
 * 路由层使用{@link WeightedRandomRouterPolicy}按权重选择子集群，AM转发层使用{@link HomeAMRMProxyPolicy}将请求转发到归属子集群
 */
public class WeightedHomePolicyManager extends AbstractPolicyManager {

  private WeightedPolicyInfo weightedPolicyInfo;

  /**
   * 构造加权归属策略管理器，硬绑定路由和AMRMProxy使用的策略实现类
   */
  public WeightedHomePolicyManager() {
    // this structurally hard-codes two compatible policies for Router and
    // AMRMProxy.
    routerFederationPolicy =  WeightedRandomRouterPolicy.class;
    amrmProxyFederationPolicy = HomeAMRMProxyPolicy.class;
    weightedPolicyInfo = new WeightedPolicyInfo();
  }

  @Override
  public SubClusterPolicyConfiguration serializeConf()
      throws FederationPolicyInitializationException {
    // 将权重配置序列化为ByteBuffer
    ByteBuffer buf = weightedPolicyInfo.toByteBuffer();
    // 构造并返回子集群策略配置对象
    return SubClusterPolicyConfiguration
        .newInstance(getQueue(), this.getClass().getCanonicalName(), buf);
  }

  @VisibleForTesting
  public WeightedPolicyInfo getWeightedPolicyInfo() {
    return weightedPolicyInfo;
  }

  @VisibleForTesting
  public void setWeightedPolicyInfo(
      WeightedPolicyInfo weightedPolicyInfo) {
    this.weightedPolicyInfo = weightedPolicyInfo;
  }

  @Override
  public boolean isSupportWeightedPolicyInfo() {
    return true;
  }
}