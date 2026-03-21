// 这个文件已经全部加上中文注释
/**
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

package org.apache.hadoop.yarn.server.federation.policies.router;

import java.util.ArrayList;
import java.util.Map;

import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.policies.FederationPolicyUtils;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyException;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterIdInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;

/**
 * YARN联邦路由策略实现，基于加权随机算法从当前活跃子集群中选择目标子集群。
 */
public class WeightedRandomRouterPolicy extends AbstractRouterPolicy {
  @Override
  protected SubClusterId chooseSubCluster(
      String queue, Map<SubClusterId, SubClusterInfo> preSelectSubclusters) throws YarnException {

    // 权重无法预计算，因为活跃子集群集合动态变化，需要每次重新计算选择
    Map<SubClusterIdInfo, Float> weights = getPolicyInfo().getRouterPolicyWeights();

    // 保存符合条件的子集群权重列表
    ArrayList<Float> weightList = new ArrayList<>();
    // 保存符合条件的子集群ID列表
    ArrayList<SubClusterId> scIdList = new ArrayList<>();
    // 遍历所有配置的权重，筛选出当前活跃的子集群
    for (Map.Entry<SubClusterIdInfo, Float> entry : weights.entrySet()) {
      SubClusterIdInfo key = entry.getKey();
      // 仅保留子集群不为空且当前处于活跃状态的权重
      if (key != null && preSelectSubclusters.containsKey(key.toId())) {
        weightList.add(entry.getValue());
        scIdList.add(key.toId());
      }
    }

    // 根据权重随机选择一个子集群的索引
    int pickedIndex = FederationPolicyUtils.getWeightedRandom(weightList);
    // 没有有效正权重时抛出异常
    if (pickedIndex == -1) {
      throw new FederationPolicyException("No positive weight found on active subclusters");
    }
    // 返回选中的子集群ID
    return scIdList.get(pickedIndex);
  }
}