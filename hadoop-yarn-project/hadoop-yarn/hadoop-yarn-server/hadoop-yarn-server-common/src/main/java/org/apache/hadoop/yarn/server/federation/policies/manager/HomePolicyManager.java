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
import java.util.Collections;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.hadoop.yarn.server.federation.policies.amrmproxy.HomeAMRMProxyPolicy;
import org.apache.hadoop.yarn.server.federation.policies.dao.WeightedPolicyInfo;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyInitializationException;
import org.apache.hadoop.yarn.server.federation.policies.router.UniformRandomRouterPolicy;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterIdInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterPolicyConfiguration;

/**
 * 联邦集群归属子集群策略管理器，为路由器使用{@link UniformRandomRouterPolicy}均匀随机路由策略，
 * 为AMRMProxy使用{@link HomeAMRMProxyPolicy}归属子集群策略查找目标ResourceManager。
 */
public class HomePolicyManager extends AbstractPolicyManager {

  /** 满足父类要求的占位权重策略信息对象 */
  private WeightedPolicyInfo weightedPolicyInfo;

  /**
   * 构造归属子集群策略管理器，硬编码预设路由和AMRMProxy策略类型
   */
  public HomePolicyManager() {

    weightedPolicyInfo = new WeightedPolicyInfo();
    weightedPolicyInfo.setRouterPolicyWeights(
        Collections.singletonMap(new SubClusterIdInfo(""), 1.0f));
    weightedPolicyInfo.setAMRMPolicyWeights(
        Collections.singletonMap(new SubClusterIdInfo(""), 1.0f));

    // 硬编码兼容的路由器策略和AMRMProxy策略类型
    routerFederationPolicy = UniformRandomRouterPolicy.class;
    amrmProxyFederationPolicy = HomeAMRMProxyPolicy.class;
  }

  @Override
  /**
   * 将当前策略配置序列化为可持久化存储的对象
   * @return 序列化后的子集群策略配置
   * @throws FederationPolicyInitializationException 序列化失败时抛出
   */
  public SubClusterPolicyConfiguration serializeConf()
      throws FederationPolicyInitializationException {

    // 将权重信息序列化转为ByteBuffer
    ByteBuffer buf = weightedPolicyInfo.toByteBuffer();
    // 构建并返回完整策略配置对象
    return SubClusterPolicyConfiguration.newInstance(
        getQueue(), this.getClass().getCanonicalName(), buf);
  }

  @Override
  public WeightedPolicyInfo getWeightedPolicyInfo() {
    throw new NotImplementedException(
        "HomePolicyManager does not implement getWeightedPolicyInfo.");
  }

  @Override
  public void setWeightedPolicyInfo(WeightedPolicyInfo weightedPolicyInfo) {
    throw new NotImplementedException(
        "HomePolicyManager does not implement setWeightedPolicyInfo.");
  }

  @Override
  public boolean isSupportWeightedPolicyInfo() {
    return false;
  }
}