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

import org.apache.hadoop.yarn.server.federation.policies.amrmproxy.LocalityMulticastAMRMProxyPolicy;
import org.apache.hadoop.yarn.server.federation.policies.dao.WeightedPolicyInfo;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyInitializationException;
import org.apache.hadoop.yarn.server.federation.policies.router.LocalityRouterPolicy;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterPolicyConfiguration;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * YARN联邦加权位置感知路由策略管理器，支持管理员为不同子集群配置权重进行调度。
 * 固定绑定位置感知路由策略和位置感知AMRM代理策略，二者专为加权位置感知场景设计协同工作。
 */
public class WeightedLocalityPolicyManager
    extends AbstractPolicyManager {

  private WeightedPolicyInfo weightedPolicyInfo;

  /**
   * 构造函数，硬编码绑定路由层和代理层兼容的位置感知策略实现。
   */
  public WeightedLocalityPolicyManager() {
    //this structurally hard-codes two compatible policies for Router and
    // AMRMProxy.
    routerFederationPolicy =  LocalityRouterPolicy.class;
    amrmProxyFederationPolicy = LocalityMulticastAMRMProxyPolicy.class;
    weightedPolicyInfo = new WeightedPolicyInfo();
  }

  @Override
  public SubClusterPolicyConfiguration serializeConf()
      throws FederationPolicyInitializationException {
    // 将权重策略信息序列化为ByteBuffer
    ByteBuffer buf = weightedPolicyInfo.toByteBuffer();
    // 构造并返回序列化后的子集群策略配置
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