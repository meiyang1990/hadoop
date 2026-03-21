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

import org.apache.hadoop.yarn.server.federation.policies.FederationPolicyInitializationContext;
import org.apache.hadoop.yarn.server.federation.policies.amrmproxy.FederationAMRMProxyPolicy;
import org.apache.hadoop.yarn.server.federation.policies.dao.WeightedPolicyInfo;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyInitializationException;
import org.apache.hadoop.yarn.server.federation.policies.router.FederationRouterPolicy;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterPolicyConfiguration;

/**
 * YARN联邦路由策略管理器接口，统一管理AMRMProxy策略和Router策略的生命周期与序列化。
 * 
 * 实现类需要支持将策略及其配置序列化为{@link SubClusterPolicyConfiguration}存储到联邦状态存储，
 * 同时提供对{@link FederationAMRMProxyPolicy}和{@link FederationRouterPolicy}的重新初始化能力。
 * 
 * 将AMRMProxy策略和Router策略绑定在一起统一管理，减少配置错误（避免组合不兼容的策略），
 * 序列化配置用于持久化存储到联邦状态存储，getter方法用于在Router和AMRMProxy服务获取初始化完成的策略实例。
 */
public interface FederationPolicyManager {

  /**
   * 获取并初始化AMRMProxy路由策略实例。
   * 若旧实例兼容则复用并重新初始化，否则创建新实例；旧实例为null则强制重置创建新实例。
   * 初始化失败时旧实例仍然保持可用。
   *
   * @param policyContext 当前策略初始化上下文
   * @param oldInstance 现有策略实例，可为null表示需要新建
   * @return 初始化完成的AMRMProxy策略实例
   * @throws FederationPolicyInitializationException 初始化失败时抛出
   */
  FederationAMRMProxyPolicy getAMRMPolicy(
      FederationPolicyInitializationContext policyContext,
      FederationAMRMProxyPolicy oldInstance)
      throws FederationPolicyInitializationException;

  /**
   * 获取并初始化Router路由策略实例。
   * 若旧实例兼容则复用并重新初始化，否则创建新实例；旧实例为null则强制重置创建新实例。
   * 初始化失败时旧实例仍然保持可用。
   *
   * @param policyContext 当前策略初始化上下文
   * @param oldInstance 现有策略实例，可为null表示需要新建
   * @return 初始化完成的Router策略实例
   * @throws FederationPolicyInitializationException 初始化失败时抛出
   */
  FederationRouterPolicy getRouterPolicy(
      FederationPolicyInitializationContext policyContext,
      FederationRouterPolicy oldInstance)
      throws FederationPolicyInitializationException;

  /**
   * 将当前策略配置序列化为可持久化的配置对象，用于存储到联邦状态存储。
   *
   * @return 序列化完成的策略配置
   * @throws FederationPolicyInitializationException 当前状态无法正确序列化时抛出
   */
  SubClusterPolicyConfiguration serializeConf()
      throws FederationPolicyInitializationException;

  /**
   * 获取当前策略绑定的队列名称。
   *
   * @return 队列名称
   */
  String getQueue();

  /**
   * 设置当前策略绑定的队列名称。
   *
   * @param queue 队列名称
   */
  void setQueue(String queue);

  /**
   * 获取当前策略配置的加权策略信息。
   *
   * @return 加权策略信息
   */
  WeightedPolicyInfo getWeightedPolicyInfo();

  /**
   * 设置当前策略的加权策略信息。
   *
   * @param weightedPolicyInfo 子集群加权策略信息
   */
  void setWeightedPolicyInfo(WeightedPolicyInfo weightedPolicyInfo);

  /**
   * 检查当前策略管理器是否支持加权策略信息，部分策略管理器不支持加权配置。
   *
   * @return true表示支持加权策略信息，false表示不支持
   */
  boolean isSupportWeightedPolicyInfo();
}