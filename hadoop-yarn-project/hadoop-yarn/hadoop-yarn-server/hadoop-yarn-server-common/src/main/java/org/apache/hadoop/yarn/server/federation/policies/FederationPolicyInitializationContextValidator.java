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
 * 联邦路由策略初始化上下文的验证工具类，提取了策略初始化的通用校验逻辑。
 */
public final class FederationPolicyInitializationContextValidator {

  private FederationPolicyInitializationContextValidator() {
    // disable constructor per checkstyle
  }

  /**
   * 验证联邦策略初始化上下文的完整性和类型一致性。
   * @param policyContext 策略初始化上下文，包含策略所需的所有依赖组件
   * @param myType 当前策略的实际类型
   * @throws FederationPolicyInitializationException 验证失败时抛出初始化异常
   */
  public static void validate(
      FederationPolicyInitializationContext policyContext, String myType)
      throws FederationPolicyInitializationException {

    // 校验当前策略类型不为空
    if (myType == null) {
      throw new FederationPolicyInitializationException(
          "The myType parameter" + " should not be null.");
    }

    // 校验初始化上下文不为空
    if (policyContext == null) {
      throw new FederationPolicyInitializationException(
          "The FederationPolicyInitializationContext provided is null. Cannot"
              + " reinitialize " + "successfully.");
    }

    // 校验联邦状态存储门面不为空
    if (policyContext.getFederationStateStoreFacade() == null) {
      throw new FederationPolicyInitializationException(
          "The FederationStateStoreFacade provided is null. Cannot"
              + " reinitialize successfully.");
    }

    // 校验子集群解析器不为空
    if (policyContext.getFederationSubclusterResolver() == null) {
      throw new FederationPolicyInitializationException(
          "The FederationSubclusterResolver provided is null. Cannot"
              + " reinitialize successfully.");
    }

    // 校验策略配置不为空
    if (policyContext.getSubClusterPolicyConfiguration() == null) {
      throw new FederationPolicyInitializationException(
          "The SubClusterPolicyConfiguration provided is null. Cannot "
              + "reinitialize successfully.");
    }

    // 从配置中获取预期的策略类型
    String intendedType =
        policyContext.getSubClusterPolicyConfiguration().getType();

    // 校验实际类型与配置预期类型一致，避免类型不匹配
    if (!myType.equals(intendedType)) {
      throw new FederationPolicyInitializationException(
          "The FederationPolicyConfiguration carries a type (" + intendedType
              + ") different then mine (" + myType
              + "). Cannot reinitialize successfully.");
    }

  }

}