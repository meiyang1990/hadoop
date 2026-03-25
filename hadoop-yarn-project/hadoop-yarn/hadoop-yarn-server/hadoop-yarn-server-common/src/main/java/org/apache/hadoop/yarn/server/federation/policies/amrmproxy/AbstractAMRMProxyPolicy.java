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

import java.util.Map;

import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.policies.AbstractConfigurableFederationPolicy;
import org.apache.hadoop.yarn.server.federation.policies.dao.WeightedPolicyInfo;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyInitializationException;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterIdInfo;

/**
 * YARN联邦AMRMProxy路由策略的抽象基类，为所有具体实现提供公共的初始化校验逻辑。
 * 实现了FederationAMRMProxyPolicy接口，提供了默认的响应通知处理。
 */
public abstract class AbstractAMRMProxyPolicy extends
    AbstractConfigurableFederationPolicy implements FederationAMRMProxyPolicy {

  /**
   * 校验权重策略配置信息，验证AMRM路由权重配置合法性。
   * @param newPolicyInfo 新的权重策略信息
   * @throws FederationPolicyInitializationException 如果权重配置为空或无效时抛出异常
   */
  @Override
  public void validate(WeightedPolicyInfo newPolicyInfo)
      throws FederationPolicyInitializationException {
    super.validate(newPolicyInfo);
    // 获取AMRM路由策略的权重映射
    Map<SubClusterIdInfo, Float> newWeights =
        newPolicyInfo.getAMRMPolicyWeights();
    // 校验权重映射非空且至少包含一个权重配置
    if (newWeights == null || newWeights.size() < 1) {
      throw new FederationPolicyInitializationException(
          "Weight vector cannot be null/empty.");
    }
  }

  /**
   * 默认处理子集群返回的Allocate响应，无状态策略默认无需处理响应信息。
   * @param subClusterId 响应来源的子集群ID
   * @param response 子集群返回的Allocate响应
   * @throws YarnException Yarn异常
   */
  @Override
  public void notifyOfResponse(SubClusterId subClusterId,
      AllocateResponse response) throws YarnException {
    // By default, a stateless policy does not care about responses
  }
}