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
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.policies.FederationPolicyInitializationContext;
import org.apache.hadoop.yarn.server.federation.policies.FederationPolicyInitializationContextValidator;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyException;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyInitializationException;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;

/**
 * YARN联邦路由均匀随机选择子集群策略实现类，从当前所有活跃子集群中均匀随机选择一个路由请求。
 * 该策略实现简单，常用于测试场景。
 *
 * 注：该策略的功能几乎可以被{@code WeightedRandomRouterPolicy}覆盖，唯一区别是：
 * 当存在不在权重配置中的活跃子集群时，本策略会将流量分发到这些子集群，而加权随机策略不会。
 */
public class UniformRandomRouterPolicy extends AbstractRouterPolicy {

  /** 随机数生成器 */
  private Random rand;

  /**
   * 构造均匀随机路由策略实例，初始化随机数生成器。
   */
  public UniformRandomRouterPolicy() {
    rand = new Random(System.currentTimeMillis());
  }

  /**
   * 重新初始化路由策略，验证上下文并忽略权重配置。
   * @param policyContext 联邦策略初始化上下文
   * @throws FederationPolicyInitializationException 初始化验证失败时抛出异常
   */
  @Override
  public void reinitialize(FederationPolicyInitializationContext policyContext)
      throws FederationPolicyInitializationException {
    FederationPolicyInitializationContextValidator.validate(policyContext,
        this.getClass().getCanonicalName());

    // 覆盖父类实现，忽略权重配置
    setPolicyContext(policyContext);
  }

  @Override
  protected SubClusterId chooseSubCluster(
      String queue, Map<SubClusterId, SubClusterInfo> preSelectSubclusters) throws YarnException {
    // 检查候选子集群列表是否为空，为空则抛出异常
    if (preSelectSubclusters == null || preSelectSubclusters.isEmpty()) {
      throw new FederationPolicyException("No available subcluster to choose from.");
    }
    // 将候选子集群ID转为列表，方便随机选取
    List<SubClusterId> list = new ArrayList<>(preSelectSubclusters.keySet());
    // 均匀随机选择一个子集群返回
    return list.get(rand.nextInt(list.size()));
  }
}