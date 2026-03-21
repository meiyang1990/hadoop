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

package org.apache.hadoop.yarn.server.federation.policies;

import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyInitializationException;

/**
 * YARN联邦环境下可配置策略的基础接口，定义了策略热更新的通用规范。
 * 采用尝试-交换(try-n-swap)语义，初始化失败时必须保证原有配置和状态不受影响。
 */
public interface ConfigurableFederationPolicy {

  /**
   * 初始化或更新策略配置，采用尝试-交换语义，初始化失败时需保留原有状态。
   *
   * @param policyContext 新的策略初始化上下文，包含更新后的配置和环境信息
   *
   * @throws FederationPolicyInitializationException 初始化或更新失败时抛出
   */
  void reinitialize(FederationPolicyInitializationContext policyContext)
      throws FederationPolicyInitializationException;
}