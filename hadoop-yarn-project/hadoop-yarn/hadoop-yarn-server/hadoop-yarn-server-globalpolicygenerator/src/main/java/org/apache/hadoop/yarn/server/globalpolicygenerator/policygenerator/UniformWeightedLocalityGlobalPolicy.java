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
package org.apache.hadoop.yarn.server.globalpolicygenerator.policygenerator;

import org.apache.hadoop.yarn.server.federation.policies.manager.FederationPolicyManager;
import org.apache.hadoop.yarn.server.federation.policies.manager.WeightedLocalityPolicyManager;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterIdInfo;
import org.apache.hadoop.yarn.server.globalpolicygenerator.GPGUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * YARN联邦全局策略生成器的均匀权重位置偏好策略实现，负责生成和更新均匀权重的位置优先调度策略。
 * 该策略会给所有子集群分配相同的调度权重，实现简单的负载均衡。
 */
public class UniformWeightedLocalityGlobalPolicy extends GlobalPolicy {

  private static final Logger LOG =
      LoggerFactory.getLogger(UniformWeightedLocalityGlobalPolicy.class);

  /**
   * 更新指定队列的均匀权重位置调度策略。
   * @param queueName 目标队列名称
   * @param clusterInfo 所有子集群的信息映射表
   * @param currentManager 当前已有的策略管理器实例
   * @return 更新后的策略管理器实例
   */
  @Override
  protected FederationPolicyManager updatePolicy(String queueName,
      Map<SubClusterId, Map<Class, Object>> clusterInfo, FederationPolicyManager currentManager){

    if(currentManager == null){
      // 为所有子集群设置统一均匀权重，创建新的策略管理器
      LOG.info("Creating uniform weighted policy queue {}.", queueName);
      WeightedLocalityPolicyManager manager = new WeightedLocalityPolicyManager();
      manager.setQueue(queueName);
      Map<SubClusterIdInfo, Float> policyWeights =
          GPGUtils.createUniformWeights(clusterInfo.keySet());
      // 设置AMRM调度策略权重
      manager.getWeightedPolicyInfo().setAMRMPolicyWeights(policyWeights);
      // 设置Router路由策略权重
      manager.getWeightedPolicyInfo().setRouterPolicyWeights(policyWeights);
      currentManager = manager;
    }

    if(currentManager instanceof WeightedLocalityPolicyManager){
      // 更新已有策略为默认均匀权重
      LOG.info("Updating policy for queue {} to default weights.", queueName);
      WeightedLocalityPolicyManager wlpmanager = (WeightedLocalityPolicyManager) currentManager;
      Map<SubClusterIdInfo, Float> uniformWeights =
          GPGUtils.createUniformWeights(clusterInfo.keySet());
      wlpmanager.getWeightedPolicyInfo().setAMRMPolicyWeights(uniformWeights);
      wlpmanager.getWeightedPolicyInfo().setRouterPolicyWeights(uniformWeights);
    } else {
      // 类型不匹配，输出警告日志
      LOG.info("Policy for queue {} is of type {}, expected {}",
          queueName, currentManager.getClass(), WeightedLocalityPolicyManager.class);
    }
    return currentManager;
  }
}