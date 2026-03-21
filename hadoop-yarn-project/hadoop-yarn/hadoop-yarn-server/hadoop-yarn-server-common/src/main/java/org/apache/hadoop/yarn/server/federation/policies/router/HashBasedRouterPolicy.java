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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.policies.FederationPolicyInitializationContext;
import org.apache.hadoop.yarn.server.federation.policies.FederationPolicyInitializationContextValidator;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyInitializationException;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;

/**
 * YARN联邦基于队列名哈希的路由策略实现类。
 * 根据作业队列名称的哈希值选择目标子集群，保证同一队列的所有作业始终映射到同一个子集群，
 * 有利于数据局部性，适用于系统中队列数量较多的场景提供默认路由行为。
 */
public class HashBasedRouterPolicy extends AbstractRouterPolicy {

  /**
   * 重新初始化哈希路由策略，验证上下文并设置策略环境。
   * @param federationPolicyContext 联邦策略初始化上下文
   * @throws FederationPolicyInitializationException 初始化失败时抛出异常
   */
  @Override
  public void reinitialize(
      FederationPolicyInitializationContext federationPolicyContext)
      throws FederationPolicyInitializationException {
    // 验证上下文信息有效性
    FederationPolicyInitializationContextValidator
        .validate(federationPolicyContext, this.getClass().getCanonicalName());

    // 覆盖父类实现，忽略权重配置，直接设置策略上下文
    setPolicyContext(federationPolicyContext);
  }

  @Override
  protected SubClusterId chooseSubCluster(String queue,
      Map<SubClusterId, SubClusterInfo> preSelectSubclusters) throws YarnException {
    // 计算队列名哈希值对候选子集群数量取模，得到选中位置，取绝对值保证索引非负
    int chosenPosition = Math.abs(queue.hashCode() % preSelectSubclusters.size());
    // 提取候选子集群ID列表
    List<SubClusterId> list = new ArrayList<>(preSelectSubclusters.keySet());
    // 对子集群ID排序，保证哈希结果一致性
    Collections.sort(list);
    // 返回选中位置对应的子集群ID
    return list.get(chosenPosition);
  }
}