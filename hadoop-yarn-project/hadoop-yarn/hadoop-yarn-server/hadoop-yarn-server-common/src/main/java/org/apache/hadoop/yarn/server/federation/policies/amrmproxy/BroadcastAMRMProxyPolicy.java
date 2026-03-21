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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.policies.FederationPolicyInitializationContext;
import org.apache.hadoop.yarn.server.federation.policies.FederationPolicyInitializationContextValidator;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyInitializationException;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;

/**
 * YARN联邦AMRMProxy策略实现，将所有资源请求广播到所有可用子集群。
 */
public class BroadcastAMRMProxyPolicy extends AbstractAMRMProxyPolicy {

  @Override
  public void reinitialize(
      FederationPolicyInitializationContext policyContext)
      throws FederationPolicyInitializationException {
    // 重写初始化方法，跳过不适用本策略的权重校验
    FederationPolicyInitializationContextValidator
        .validate(policyContext, this.getClass().getCanonicalName());
    setPolicyContext(policyContext);
  }

  @Override
  public Map<SubClusterId, List<ResourceRequest>> splitResourceRequests(
      List<ResourceRequest> resourceRequests,
      Set<SubClusterId> timedOutSubClusters) throws YarnException {

    // 获取当前所有活跃子集群信息
    Map<SubClusterId, SubClusterInfo> activeSubclusters =
        getActiveSubclusters();

    Map<SubClusterId, List<ResourceRequest>> answer = new HashMap<>();

    // 将资源请求广播到所有活跃子集群
    for (SubClusterId subClusterId : activeSubclusters.keySet()) {
      answer.put(subClusterId, resourceRequests);
    }

    return answer;
  }

}