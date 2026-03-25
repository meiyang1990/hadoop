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

package org.apache.hadoop.yarn.server.federation.policies.router;

import java.util.Map;

import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyException;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterIdInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;

/**
 * YARN联邦优先级路由策略实现，将子集群权重作为优先级，从活跃子集群中选择权重最高的子集群路由应用提交请求。
 */
public class PriorityRouterPolicy extends AbstractRouterPolicy {

  @Override
  protected SubClusterId chooseSubCluster(
      String queue, Map<SubClusterId, SubClusterInfo> preSelectSubclusters) throws YarnException {
    // 获取当前路由策略配置的各子集群权重
    Map<SubClusterIdInfo, Float> weights = getPolicyInfo().getRouterPolicyWeights();
    // 存储最终选中的子集群ID
    SubClusterId chosen = null;
    // 记录当前找到的最大权重，初始化为最小浮点值
    Float currentBest = Float.MIN_VALUE;
    // 遍历所有预选的活跃子集群，找出权重最高的子集群
    for (SubClusterId id : preSelectSubclusters.keySet()) {
      SubClusterIdInfo idInfo = new SubClusterIdInfo(id);
      // 检查子集群是否有权重配置，且权重高于当前最优值
      if (weights.containsKey(idInfo) && weights.get(idInfo) > currentBest) {
        currentBest = weights.get(idInfo);
        chosen = id;
      }
    }
    // 未找到符合条件的活跃子集群，抛出异常
    if (chosen == null) {
      throw new FederationPolicyException(
          "No Active Subcluster with weight vector greater than zero.");
    }
    // 返回选中的权重最高的子集群ID
    return chosen;
  }
}