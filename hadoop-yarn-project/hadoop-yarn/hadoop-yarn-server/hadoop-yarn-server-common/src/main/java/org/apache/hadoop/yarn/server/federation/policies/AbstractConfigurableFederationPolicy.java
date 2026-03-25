// 这个文件已经全部加上中文注释
/*
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

package org.apache.hadoop.yarn.server.federation.policies;

import java.util.Map;

import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.policies.dao.WeightedPolicyInfo;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.FederationPolicyInitializationException;
import org.apache.hadoop.yarn.server.federation.policies.exceptions.NoActiveSubclustersException;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;

/**
 * YARN联邦可配置加权路由策略的抽象基类，提供策略初始化、配置存储和活跃子集群获取的通用能力，所有具体加权路由策略需继承此类。
 */
public abstract class AbstractConfigurableFederationPolicy
    implements ConfigurableFederationPolicy {

  private WeightedPolicyInfo policyInfo = null;
  private FederationPolicyInitializationContext policyContext;
  private boolean isDirty;

  public AbstractConfigurableFederationPolicy() {
  }

  @Override
  public void reinitialize(
      FederationPolicyInitializationContext initializationContext)
      throws FederationPolicyInitializationException {
    // 标记配置已变更，需要子类重新初始化
    isDirty = true;
    // 对初始化上下文做合法性校验
    FederationPolicyInitializationContextValidator
        .validate(initializationContext, this.getClass().getCanonicalName());

    // 从配置缓冲区反序列化得到新的策略配置信息
    WeightedPolicyInfo newPolicyInfo = WeightedPolicyInfo.fromByteBuffer(
        initializationContext.getSubClusterPolicyConfiguration().getParams());

    // 如果新老配置一致，无需重新初始化，标记为未变更
    if (policyInfo != null && policyInfo.equals(newPolicyInfo)) {
      isDirty = false;
      return;
    }

    // 校验新配置合法性
    validate(newPolicyInfo);
    // 更新策略配置
    setPolicyInfo(newPolicyInfo);
    // 保存初始化上下文
    this.policyContext = initializationContext;
  }

  /**
   * 对策略配置进行合法性校验，子类可覆盖实现自定义校验逻辑。
   *
   * @param newPolicyInfo 待校验的新策略配置
   *
   * @throws FederationPolicyInitializationException 如果配置非法则抛出异常
   */
  public void validate(WeightedPolicyInfo newPolicyInfo)
      throws FederationPolicyInitializationException {
    if (newPolicyInfo == null) {
      throw new FederationPolicyInitializationException(
          "The policy to " + "validate should not be null.");
    }
  }

  /**
   * 获取策略配置是否变更标记，用于子类判断是否需要执行额外初始化逻辑，若配置未变更可提前退出。
   *
   * @return true表示配置变更需要重新初始化，false表示配置无变化无需额外处理
   */
  public boolean getIsDirty() {
    return isDirty;
  }

  /**
   * 获取当前策略的配置信息对象。
   *
   * @return 代表策略配置的WeightedPolicyInfo对象
   */
  public WeightedPolicyInfo getPolicyInfo() {
    return policyInfo;
  }

  /**
   * 设置当前策略的配置信息对象。
   *
   * @param policyInfo 代表策略配置的WeightedPolicyInfo对象
   */
  public void setPolicyInfo(WeightedPolicyInfo policyInfo) {
    this.policyInfo = policyInfo;
  }

  /**
   * 获取当前策略的初始化上下文。
   *
   * @return 当前策略的上下文对象
   */
  public FederationPolicyInitializationContext getPolicyContext() {
    return policyContext;
  }

  /**
   * 设置当前策略的初始化上下文。
   *
   * @param policyContext 要设置的上下文对象
   */
  public void setPolicyContext(
      FederationPolicyInitializationContext policyContext) {
    this.policyContext = policyContext;
  }

  /**
   * 从联邦状态存储获取所有活跃子集群信息，并校验列表非空。
   *
   * @return 所有活跃子集群的ID到信息的映射表
   *
   * @throws YarnException 如果获取失败或无活跃子集群则抛出异常
   */
  protected Map<SubClusterId, SubClusterInfo> getActiveSubclusters()
      throws YarnException {

    // 从状态存储门面查询所有活跃子集群
    Map<SubClusterId, SubClusterInfo> activeSubclusters =
        getPolicyContext().getFederationStateStoreFacade().getSubClusters(true);

    // 校验活跃子集群列表非空，为空抛出异常
    if (activeSubclusters == null || activeSubclusters.size() < 1) {
      throw new NoActiveSubclustersException(
          "Zero active subclusters, cannot pick where to send job.");
    }
    return activeSubclusters;
  }

}