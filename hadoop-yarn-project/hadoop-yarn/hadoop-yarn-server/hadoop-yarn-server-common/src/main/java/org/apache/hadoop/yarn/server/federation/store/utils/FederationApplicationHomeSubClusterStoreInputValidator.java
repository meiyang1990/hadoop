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

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.server.federation.store.exception.FederationStateStoreInvalidInputException;
import org.apache.hadoop.yarn.server.federation.store.records.AddApplicationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.ApplicationHomeSubCluster;
import org.apache.hadoop.yarn.server.federation.store.records.DeleteApplicationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetApplicationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.UpdateApplicationHomeSubClusterRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 应用归属子集群存储输入参数校验工具类，为联邦状态存储提供快速失败的输入校验能力。
 * 负责校验对FederationApplicationHomeSubClusterStore所有操作的入参合法性。
 */
public final class FederationApplicationHomeSubClusterStoreInputValidator {

  private static final Logger LOG = LoggerFactory
      .getLogger(FederationApplicationHomeSubClusterStoreInputValidator.class);

  /**
   * 工具类不允许实例化。
   */
  private FederationApplicationHomeSubClusterStoreInputValidator() {
  }

  /**
   * 校验添加应用归属子集群请求入参合法性。
   *
   * @param request 添加应用归属子集群请求
   * @throws FederationStateStoreInvalidInputException 入参非法时抛出异常
   */
  public static void validate(AddApplicationHomeSubClusterRequest request)
      throws FederationStateStoreInvalidInputException {
    // 校验请求对象不为空
    if (request == null) {
      String message = "Missing AddApplicationHomeSubCluster Request."
          + " Please try again by specifying"
          + " an AddApplicationHomeSubCluster information.";
      LOG.warn(message);
      throw new FederationStateStoreInvalidInputException(message);
    }

    // 校验应用归属子集群信息合法性
    checkApplicationHomeSubCluster(request.getApplicationHomeSubCluster());
  }

  /**
   * 校验更新应用归属子集群请求入参合法性。
   *
   * @param request 更新应用归属子集群请求
   * @throws FederationStateStoreInvalidInputException 入参非法时抛出异常
   */
  public static void validate(UpdateApplicationHomeSubClusterRequest request)
      throws FederationStateStoreInvalidInputException {
    // 校验请求对象不为空
    if (request == null) {
      String message = "Missing UpdateApplicationHomeSubCluster Request."
          + " Please try again by specifying"
          + " an ApplicationHomeSubCluster information.";
      LOG.warn(message);
      throw new FederationStateStoreInvalidInputException(message);
    }

    // 校验应用归属子集群信息合法性
    checkApplicationHomeSubCluster(request.getApplicationHomeSubCluster());
  }

  /**
   * 校验查询应用归属子集群请求入参合法性。
   *
   * @param request 查询应用归属子集群请求
   * @throws FederationStateStoreInvalidInputException 入参非法时抛出异常
   */
  public static void validate(GetApplicationHomeSubClusterRequest request)
      throws FederationStateStoreInvalidInputException {
    // 校验请求对象不为空
    if (request == null) {
      String message = "Missing GetApplicationHomeSubCluster Request."
          + " Please try again by specifying an Application Id information.";
      LOG.warn(message);
      throw new FederationStateStoreInvalidInputException(message);
    }

    // 校验应用ID合法性
    checkApplicationId(request.getApplicationId());
  }

  /**
   * 校验删除应用归属子集群请求入参合法性。
   *
   * @param request 删除应用归属子集群请求
   * @throws FederationStateStoreInvalidInputException 入参非法时抛出异常
   */
  public static void validate(DeleteApplicationHomeSubClusterRequest request)
      throws FederationStateStoreInvalidInputException {
    // 校验请求对象不为空
    if (request == null) {
      String message = "Missing DeleteApplicationHomeSubCluster Request."
          + " Please try again by specifying"
          + " an ApplicationHomeSubCluster information.";
      LOG.warn(message);
      throw new FederationStateStoreInvalidInputException(message);
    }

    // 校验应用ID合法性
    checkApplicationId(request.getApplicationId());
  }

  /**
   * 校验应用归属子集群信息合法性。
   *
   * @param applicationHomeSubCluster 应用归属子集群信息
   * @throws FederationStateStoreInvalidInputException 信息非法时抛出异常
   */
  private static void checkApplicationHomeSubCluster(
      ApplicationHomeSubCluster applicationHomeSubCluster)

      throws FederationStateStoreInvalidInputException {
    // 校验对象不为空
    if (applicationHomeSubCluster == null) {
      String message = "Missing ApplicationHomeSubCluster Info."
          + " Please try again by specifying"
          + " an ApplicationHomeSubCluster information.";
      LOG.warn(message);
      throw new FederationStateStoreInvalidInputException(message);
    }
    // 校验应用ID合法性
    checkApplicationId(applicationHomeSubCluster.getApplicationId());

    // 调用通用工具校验子集群ID合法性
    FederationMembershipStateStoreInputValidator
        .checkSubClusterId(applicationHomeSubCluster.getHomeSubCluster());

  }

  /**
   * 校验应用ID合法性。
   *
   * @param appId 应用ID
   * @throws FederationStateStoreInvalidInputException 应用ID非法时抛出异常
   */
  private static void checkApplicationId(ApplicationId appId)
      throws FederationStateStoreInvalidInputException {
    if (appId == null) {
      String message = "Missing Application Id."
          + " Please try again by specifying an Application Id.";
      LOG.warn(message);
      throw new FederationStateStoreInvalidInputException(message);
    }
  }
}