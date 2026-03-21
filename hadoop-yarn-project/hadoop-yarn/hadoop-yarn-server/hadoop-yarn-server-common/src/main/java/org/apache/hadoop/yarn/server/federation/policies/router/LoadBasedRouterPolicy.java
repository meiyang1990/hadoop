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
import org.apache.hadoop.yarn.server.federation.policies.FederationPolicyInitializationContext;
import org.apache.hadoop.yarn.server.federation.policies.dao.WeightedPolicyInfo;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyException;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyInitializationException;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterIdInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

/**
 * 文件说明：YARN联邦路由负载均衡策略实现类，基于子集群当前可用内存选择目标子集群分发应用
 * 
 * 本实现是基于负载的路由策略，权重仅为0/1（表示是否启用该子集群），
 * 会选择当前可用内存最多的已启用子集群来提交新应用，实现负载均衡。
 */
public class LoadBasedRouterPolicy extends AbstractRouterPolicy {

  /**
   * 重新初始化负载路由策略，验证权重配置合法性
   * @param policyContext 联邦策略初始化上下文
   * @throws FederationPolicyInitializationException 当权重配置不合法时抛出异常
   */
  @Override
  public void reinitialize(FederationPolicyInitializationContext policyContext)
      throws FederationPolicyInitializationException {

    // 保存原有策略信息，校验失败时回滚
    WeightedPolicyInfo tempPolicy = getPolicyInfo();

    // 调用父类完成基础初始化
    super.reinitialize(policyContext);

    // 校验所有权重必须为0或1
    for (Float weight : getPolicyInfo().getRouterPolicyWeights().values()) {
      if (weight != 0 && weight != 1) {
        // 校验失败，恢复原有策略信息
        setPolicyInfo(tempPolicy);
        throw new FederationPolicyInitializationException(
            this.getClass().getCanonicalName()
                + " policy expects all weights to be either "
                + "\"0\" or \"1\"");
      }
    }
  }

  /**
   * 根据负载情况从预选子集群中选择最合适的目标子集群
   * @param queue 提交应用的队列名称
   * @param preSelectSubclusters 预选的可用子集群集合
   * @return 选中的目标子集群ID
   * @throws YarnException 无可用已启用子集群时抛出异常
   */
  @Override
  protected SubClusterId chooseSubCluster(
      String queue, Map<SubClusterId, SubClusterInfo> preSelectSubclusters) throws YarnException {
    // 获取策略配置的权重表
    Map<SubClusterIdInfo, Float> weights = getPolicyInfo().getRouterPolicyWeights();
    // 记录选中的子集群和当前最大可用内存
    SubClusterIdInfo chosen = null;
    long currBestMem = -1;
    // 遍历所有预选子集群，寻找可用内存最大的已启用子集群
    for (Map.Entry<SubClusterId, SubClusterInfo> entry : preSelectSubclusters.entrySet()) {
      SubClusterIdInfo id = new SubClusterIdInfo(entry.getKey());
      // 仅考虑权重配置为1（已启用）的子集群
      if (weights.containsKey(id) && weights.get(id) > 0) {
        long availableMemory = getAvailableMemory(entry.getValue());
        // 更新最优解，保留可用内存最大的子集群
        if (availableMemory > currBestMem) {
          currBestMem = availableMemory;
          chosen = id;
        }
      }
    }
    // 没有符合条件的子集群，抛出异常
    if (chosen == null) {
      throw new FederationPolicyException(
          "Zero Active Subcluster with weight 1.");
    }
    // 返回转换后的子集群ID
    return chosen.toId();
  }

  /**
   * 从子集群信息中解析得到当前可用内存大小
   * @param value 子集群信息对象
   * @return 可用内存大小，单位MB
   * @throws YarnException 解析子集群能力JSON失败时抛出异常
   */
  private long getAvailableMemory(SubClusterInfo value) throws YarnException {
    try {
      long mem = -1;
      // 解析子集群能力JSON，从集群指标中获取可用内存
      JSONObject obj = new JSONObject(value.getCapability());
      mem = obj.getJSONObject("clusterMetrics").getLong("availableMB");
      return mem;
    } catch (JSONException j) {
      throw new YarnException("FederationSubClusterInfo cannot be parsed", j);
    }
  }
}