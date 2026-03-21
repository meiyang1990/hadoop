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

import org.apache.hadoop.yarn.server.federation.policies.ConfigurableFederationPolicy;
import org.apache.hadoop.yarn.server.federation.policies.FederationPolicyInitializationContext;
import org.apache.hadoop.yarn.server.federation.policies.FederationPolicyInitializationContextValidator;
import org.apache.hadoop.yarn.server.federation.policies.amrmproxy.FederationAMRMProxyPolicy;
import org.apache.hadoop.yarn.server.federation.policies.dao.WeightedPolicyInfo;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyInitializationException;
import org.apache.hadoop.yarn.server.federation.policies.router.FederationRouterPolicy;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterPolicyConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * 文件说明: YARN联邦策略管理器抽象基类，提供多个策略实现共用的基础方法，减少重复代码
 * 核心职责: 统一管理路由策略和AMRM代理策略的初始化、重新实例化逻辑，子类只需实现权重信息相关抽象方法
 */
public abstract class AbstractPolicyManager implements
    FederationPolicyManager {

  private String queue;
  @SuppressWarnings("checkstyle:visibilitymodifier")
  protected Class routerFederationPolicy;
  @SuppressWarnings("checkstyle:visibilitymodifier")
  protected Class amrmProxyFederationPolicy;

  public static final Logger LOG =
      LoggerFactory.getLogger(AbstractPolicyManager.class);

  /**
   * 获取并初始化AMRM代理策略，默认实现已完成通用初始化校验和实例复用逻辑
   * @param federationPolicyContext 当前联邦策略初始化上下文
   * @param oldInstance 已存在的策略实例，可为null
   * @return 初始化完成的有效AMRM代理策略实例
   * @throws FederationPolicyInitializationException 初始化失败时抛出，保证原状态不变
   */
  public FederationAMRMProxyPolicy getAMRMPolicy(
      FederationPolicyInitializationContext federationPolicyContext,
      FederationAMRMProxyPolicy oldInstance)
      throws FederationPolicyInitializationException {

    // 检查子类是否已在构造函数中初始化策略类型
    if (amrmProxyFederationPolicy == null) {
      throw new FederationPolicyInitializationException("The parameter "
          + "amrmProxyFederationPolicy should be initialized in "
          + this.getClass().getSimpleName() + " constructor.");
    }

    try {
      return (FederationAMRMProxyPolicy) internalPolicyGetter(
          federationPolicyContext, oldInstance, amrmProxyFederationPolicy);
    } catch (ClassCastException e) {
      throw new FederationPolicyInitializationException(e);
    }

  }

  /**
   * 获取并初始化路由策略，默认实现已完成通用初始化校验和实例复用逻辑
   * @param federationPolicyContext 当前联邦策略初始化上下文
   * @param oldInstance 已存在的策略实例，可为null
   * @return 初始化完成的有效路由策略实例
   * @throws FederationPolicyInitializationException 初始化失败时抛出，保证原状态不变
   */

  public FederationRouterPolicy getRouterPolicy(
      FederationPolicyInitializationContext federationPolicyContext,
      FederationRouterPolicy oldInstance)
      throws FederationPolicyInitializationException {

    // 检查子类是否已在构造函数中初始化策略类型
    if (routerFederationPolicy == null) {
      throw new FederationPolicyInitializationException("The policy "
          + "type should be initialized in " + this.getClass().getSimpleName()
          + " constructor.");
    }

    try {
      return (FederationRouterPolicy) internalPolicyGetter(
          federationPolicyContext, oldInstance, routerFederationPolicy);
    } catch (ClassCastException e) {
      throw new FederationPolicyInitializationException(e);
    }
  }

  @Override
  public SubClusterPolicyConfiguration serializeConf()
      throws FederationPolicyInitializationException {
    // 默认实现仅适用于不需要额外参数的子类，配置为空字节缓冲
    ByteBuffer buf = ByteBuffer.allocate(0);
    return SubClusterPolicyConfiguration
        .newInstance(getQueue(), this.getClass().getCanonicalName(), buf);
  }

  @Override
  public String getQueue() {
    return queue;
  }

  @Override
  public void setQueue(String queue) {
    this.queue = queue;
  }

  /**
   * 通用内部方法：负责策略实例的创建、复用和重新初始化
   * @param federationPolicyContext 当前联邦策略初始化上下文
   * @param oldInstance 已存在的策略实例，可为null
   * @param policy 目标策略类对象
   * @return 初始化完成的可配置策略实例
   * @throws FederationPolicyInitializationException 初始化或实例化失败时抛出
   */
  private ConfigurableFederationPolicy internalPolicyGetter(
      final FederationPolicyInitializationContext federationPolicyContext,
      ConfigurableFederationPolicy oldInstance, Class policy)
      throws FederationPolicyInitializationException {

    // 验证初始化上下文合法性
    FederationPolicyInitializationContextValidator
        .validate(federationPolicyContext, this.getClass().getCanonicalName());

    // 实例不存在或类型不匹配，需要重新创建实例
    if (oldInstance == null || !oldInstance.getClass().equals(policy)) {
      try {
        oldInstance = (ConfigurableFederationPolicy) policy.newInstance();
      } catch (InstantiationException e) {
        throw new FederationPolicyInitializationException(e);
      } catch (IllegalAccessException e) {
        throw new FederationPolicyInitializationException(e);
      }
    }

    // 拷贝上下文避免修改原对象产生副作用
    FederationPolicyInitializationContext modifiedContext =
        updateContext(federationPolicyContext,
            oldInstance.getClass().getCanonicalName());

    // 重新初始化策略实例
    oldInstance.reinitialize(modifiedContext);
    return oldInstance;
  }

  /**
   * 拷贝原上下文生成新上下文，修改策略类型后返回，实现写时复制避免副作用
   * @param federationPolicyContext 原始初始化上下文
   * @param type 目标策略类型名称
   * @return 修改后的新上下文对象
   */
  private FederationPolicyInitializationContext updateContext(
      FederationPolicyInitializationContext federationPolicyContext,
      String type) {
    // 深拷贝原配置避免修改原对象
    SubClusterPolicyConfiguration newConf = SubClusterPolicyConfiguration
        .newInstance(federationPolicyContext
            .getSubClusterPolicyConfiguration());
    // 设置当前策略的类型
    newConf.setType(type);

    return new FederationPolicyInitializationContext(newConf,
                  federationPolicyContext.getFederationSubclusterResolver(),
                  federationPolicyContext.getFederationStateStoreFacade(),
                  federationPolicyContext.getHomeSubcluster());
  }

  /**
   * 获取子集群权重策略配置信息，抽象方法由子类具体实现
   * @return 权重策略信息对象
   */
  public abstract WeightedPolicyInfo getWeightedPolicyInfo();

  /**
   * 设置子集群权重策略配置信息，抽象方法由子类具体实现
   * @param weightedPolicyInfo 权重策略信息对象
   */
  public abstract void setWeightedPolicyInfo(WeightedPolicyInfo weightedPolicyInfo);
}