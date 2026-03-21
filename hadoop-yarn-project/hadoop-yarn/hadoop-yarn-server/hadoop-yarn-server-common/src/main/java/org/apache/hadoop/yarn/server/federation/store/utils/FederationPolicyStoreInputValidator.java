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

package org.apache.hadoop.yarn.server.federation.store.utils;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.yarn.server.federation.store.exception.FederationStateStoreInvalidInputException;
import org.apache.hadoop.yarn.server.federation.store.records.DeleteSubClusterPoliciesConfigurationsRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterPolicyConfigurationRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SetSubClusterPolicyConfigurationRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterPolicyConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * YARN联邦策略存储输入参数校验工具类，对输入参数进行快速校验，实现无效输入提前失败（fail fast）。
 * 用于校验FederationPolicyStore各类操作的输入参数合法性。
 *
 */
public final class FederationPolicyStoreInputValidator {

  /** 日志实例 */
  private static final Logger LOG =
      LoggerFactory.getLogger(FederationPolicyStoreInputValidator.class);

  /** 工具类禁止实例化 */
  private FederationPolicyStoreInputValidator() {
  }

  /**
   * 校验获取子集群策略配置请求参数的合法性，提前拦截无效输入。
   *
   * @param request 待校验的获取子集群策略配置请求
   * @throws FederationStateStoreInvalidInputException 输入参数无效时抛出异常
   */
  public static void validate(GetSubClusterPolicyConfigurationRequest request)
      throws FederationStateStoreInvalidInputException {
    if (request == null) {
      String message = "Missing GetSubClusterPolicyConfiguration Request."
          + " Please try again by specifying a policy selection information.";
      LOG.warn(message);
      throw new FederationStateStoreInvalidInputException(message);
    }

    // 校验队列标识合法性
    checkQueue(request.getQueue());
  }

  /**
   * 校验设置子集群策略配置请求参数的合法性，提前拦截无效输入。
   *
   * @param request 待校验的设置子集群策略配置请求
   * @throws FederationStateStoreInvalidInputException 输入参数无效时抛出异常
   */
  public static void validate(SetSubClusterPolicyConfigurationRequest request)
      throws FederationStateStoreInvalidInputException {
    if (request == null) {
      String message = "Missing SetSubClusterPolicyConfiguration Request."
          + " Please try again by specifying an policy insertion information.";
      LOG.warn(message);
      throw new FederationStateStoreInvalidInputException(message);
    }

    // 校验子集群策略配置合法性
    checkSubClusterPolicyConfiguration(request.getPolicyConfiguration());
  }

  /**
   * 校验子集群策略配置参数的合法性。
   *
   * @param policyConfiguration 待校验的子集群策略配置
   * @throws FederationStateStoreInvalidInputException 输入参数无效时抛出异常
   */
  private static void checkSubClusterPolicyConfiguration(
      SubClusterPolicyConfiguration policyConfiguration)
      throws FederationStateStoreInvalidInputException {
    if (policyConfiguration == null) {
      String message = "Missing SubClusterPolicyConfiguration."
          + " Please try again by specifying a SubClusterPolicyConfiguration.";
      LOG.warn(message);
      throw new FederationStateStoreInvalidInputException(message);
    }

    // 校验队列标识合法性
    checkQueue(policyConfiguration.getQueue());
    // 校验策略类型合法性
    checkType(policyConfiguration.getType());

  }

  /**
   * 校验策略所属队列标识参数的合法性。
   *
   * @param queue 待校验的策略队列标识
   * @throws FederationStateStoreInvalidInputException 输入参数无效时抛出异常
   */
  private static void checkQueue(String queue)
      throws FederationStateStoreInvalidInputException {
    if (queue == null || queue.isEmpty()) {
      String message = "Missing Queue. Please try again by specifying a Queue.";
      LOG.warn(message);
      throw new FederationStateStoreInvalidInputException(message);
    }
  }

  /**
   * 校验策略类型参数的合法性。
   *
   * @param type 待校验的策略类型
   * @throws FederationStateStoreInvalidInputException 输入参数无效时抛出异常
   */
  private static void checkType(String type)
      throws FederationStateStoreInvalidInputException {
    if (type == null || type.isEmpty()) {
      String message = "Missing Policy Type."
          + " Please try again by specifying a Policy Type.";
      LOG.warn(message);
      throw new FederationStateStoreInvalidInputException(message);
    }
  }

  /**
   * 校验删除子集群策略配置请求参数的合法性，提前拦截无效输入。
   *
   * @param request 待校验的删除子集群策略配置请求
   * @throws FederationStateStoreInvalidInputException 输入参数无效时抛出异常
   */
  public static void validate(DeleteSubClusterPoliciesConfigurationsRequest request)
      throws FederationStateStoreInvalidInputException {
    if (request == null) {
      String message = "Missing DeleteSubClusterPoliciesConfigurationsRequest Request."
          + " Please try again by specifying an policy insertion information.";
      LOG.warn(message);
      throw new FederationStateStoreInvalidInputException(message);
    }

    List<String> queues = request.getQueues();
    if (CollectionUtils.isEmpty(queues)) {
      throw new FederationStateStoreInvalidInputException(
          "The queues that needs to be deleted cannot be empty.");
    }
  }
}