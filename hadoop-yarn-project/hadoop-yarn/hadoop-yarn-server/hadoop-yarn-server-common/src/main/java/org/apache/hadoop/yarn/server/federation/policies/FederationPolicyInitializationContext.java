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

import org.apache.hadoop.yarn.server.federation.resolver.SubClusterResolver;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterPolicyConfiguration;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;

/**
 * YARN联邦路由策略初始化上下文，为{@code FederationAMRMProxyPolicy}和{@code
 * FederationRouterPolicy}的初始化/重新初始化提供所需的全部依赖信息。
 */
public class FederationPolicyInitializationContext {

  // 联邦策略配置信息
  private SubClusterPolicyConfiguration federationPolicyConfiguration;
  // 子集群解析器，用于解析任务对应的目标子集群
  private SubClusterResolver federationSubclusterResolver;
  // 联邦状态存储门面，提供访问联邦状态存储的高层接口
  private FederationStateStoreFacade federationStateStoreFacade;
  // 当前Router所在的本地子集群ID
  private SubClusterId homeSubcluster;

  /**
   * 空构造函数，所有依赖初始化为空，后续通过setter方法注入。
   */
  public FederationPolicyInitializationContext() {
    federationPolicyConfiguration = null;
    federationSubclusterResolver = null;
    federationStateStoreFacade = null;
  }

  /**
   * 全参数构造函数，一次性注入所有初始化所需依赖。
   * @param policy 联邦策略配置
   * @param resolver 子集群解析器
   * @param storeFacade 联邦状态存储门面
   * @param home 当前Router所属本地子集群ID
   */
  public FederationPolicyInitializationContext(
      SubClusterPolicyConfiguration policy, SubClusterResolver resolver,
      FederationStateStoreFacade storeFacade, SubClusterId home) {
    this.federationPolicyConfiguration = policy;
    this.federationSubclusterResolver = resolver;
    this.federationStateStoreFacade = storeFacade;
    this.homeSubcluster = home;
  }

  /**
   * 获取用于策略初始化的子集群策略配置对象。
   *
   * @return 子集群策略配置
   */
  public SubClusterPolicyConfiguration getSubClusterPolicyConfiguration() {
    return federationPolicyConfiguration;
  }

  /**
   * 设置用于策略初始化的子集群策略配置对象。
   *
   * @param fedPolicyConfiguration 子集群策略配置
   */
  public void setSubClusterPolicyConfiguration(
      SubClusterPolicyConfiguration fedPolicyConfiguration) {
    this.federationPolicyConfiguration = fedPolicyConfiguration;
  }

  /**
   * 获取用于策略初始化的子集群解析器。
   *
   * @return 子集群解析器
   */
  public SubClusterResolver getFederationSubclusterResolver() {
    return federationSubclusterResolver;
  }

  /**
   * 设置用于策略初始化的子集群解析器。
   *
   * @param federationSubclusterResolver 子集群解析器
   */
  public void setFederationSubclusterResolver(
      SubClusterResolver federationSubclusterResolver) {
    this.federationSubclusterResolver = federationSubclusterResolver;
  }

  /**
   * 获取用于策略初始化的联邦状态存储门面。
   *
   * @return 联邦状态存储门面
   */
  public FederationStateStoreFacade getFederationStateStoreFacade() {
    return federationStateStoreFacade;
  }

  /**
   * 设置用于策略初始化的联邦状态存储门面。
   *
   * @param federationStateStoreFacade 联邦状态存储门面
   */
  public void setFederationStateStoreFacade(
      FederationStateStoreFacade federationStateStoreFacade) {
    this.federationStateStoreFacade = federationStateStoreFacade;
  }

  /**
   * 获取当前Router所属的本地子集群，默认策略通常会优先使用本地子集群调度。
   *
   * @return 本地子集群ID
   */
  public SubClusterId getHomeSubcluster() {
    return homeSubcluster;
  }

  /**
   * 设置当前Router所属的本地子集群，默认策略通常会优先使用本地子集群调度。
   *
   * @param homeSubcluster 本地子集群ID
   */
  public void setHomeSubcluster(SubClusterId homeSubcluster) {
    this.homeSubcluster = homeSubcluster;
  }

}