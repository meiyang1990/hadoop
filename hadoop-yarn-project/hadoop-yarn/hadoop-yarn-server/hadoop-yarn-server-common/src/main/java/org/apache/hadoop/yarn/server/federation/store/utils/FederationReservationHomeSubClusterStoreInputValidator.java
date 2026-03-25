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

import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.server.federation.store.exception.FederationStateStoreInvalidInputException;
import org.apache.hadoop.yarn.server.federation.store.records.AddReservationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.ReservationHomeSubCluster;
import org.apache.hadoop.yarn.server.federation.store.records.GetReservationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.UpdateReservationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.DeleteReservationHomeSubClusterRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 联邦存储预留信息归属子集群输入验证工具类，为 {@code FederationReservationHomeSubClusterStore} 提供快速失败的输入合法性检查。
 * 对用户输入进行提前校验，避免非法请求流入存储层。
 *
 */
public final class FederationReservationHomeSubClusterStoreInputValidator {

  private static final Logger LOG = LoggerFactory
      .getLogger(FederationReservationHomeSubClusterStoreInputValidator.class);

  /**
   * 工具类不允许实例化。
   */
  private FederationReservationHomeSubClusterStoreInputValidator() {
  }

  /**
   * 校验添加预留归属子集群请求的输入合法性，提前检查明显错误实现快速失败。
   *
   * @param request 待校验的添加请求
   * @throws FederationStateStoreInvalidInputException 如果请求非法抛出异常
   */
  public static void validate(AddReservationHomeSubClusterRequest request)
      throws FederationStateStoreInvalidInputException {
    if (request == null) {
      String message = "Missing AddReservationHomeSubCluster Request."
          + " Please try again by specifying"
          + " an AddReservationHomeSubCluster information.";
      LOG.warn(message);
      throw new FederationStateStoreInvalidInputException(message);
    }

    // 校验预留归属子集群信息
    checkReservationHomeSubCluster(request.getReservationHomeSubCluster());
  }

  /**
   * 校验查询预留归属子集群请求的输入合法性，提前检查明显错误实现快速失败。
   *
   * @param request 待校验的查询请求
   * @throws FederationStateStoreInvalidInputException 如果请求非法抛出异常
   */
  public static void validate(GetReservationHomeSubClusterRequest request)
      throws FederationStateStoreInvalidInputException {
    if (request == null) {
      String message = "Missing GetReservationHomeSubCluster Request."
          + " Please try again by specifying an Reservation Id information.";
      LOG.warn(message);
      throw new FederationStateStoreInvalidInputException(message);
    }

    // 校验预留ID合法性
    checkReservationId(request.getReservationId());
  }

  /**
   * 校验预留归属子集群信息是否完整合法。
   *
   * @param reservationHomeSubCluster 待校验的预留归属信息
   * @throws FederationStateStoreInvalidInputException 如果信息非法抛出异常
   */
  private static void checkReservationHomeSubCluster(
      ReservationHomeSubCluster reservationHomeSubCluster)
      throws FederationStateStoreInvalidInputException {
    if (reservationHomeSubCluster == null) {
      String message = "Missing ReservationHomeSubCluster Info."
          + " Please try again by specifying"
          + " an ReservationHomeSubCluster information.";
      LOG.warn(message);
      throw new FederationStateStoreInvalidInputException(message);
    }

    // 校验预留ID合法性
    checkReservationId(reservationHomeSubCluster.getReservationId());

    // 校验子集群ID合法性，复用通用验证逻辑
    FederationMembershipStateStoreInputValidator
        .checkSubClusterId(reservationHomeSubCluster.getHomeSubCluster());
  }

  /**
   * 校验预留ID是否存在。
   *
   * @param reservationId 待校验的预留ID
   * @throws FederationStateStoreInvalidInputException 如果预留ID为空抛出异常
   */
  private static void checkReservationId(ReservationId reservationId)
      throws FederationStateStoreInvalidInputException {
    if (reservationId == null) {
      String message = "Missing ReservationId. Please try again by specifying an ReservationId.";
      LOG.warn(message);
      throw new FederationStateStoreInvalidInputException(message);
    }
  }

  /**
   * 校验更新预留归属子集群请求的输入合法性，提前检查明显错误实现快速失败。
   *
   * @param request 待校验的更新请求
   * @throws FederationStateStoreInvalidInputException 如果请求非法抛出异常
   */
  public static void validate(UpdateReservationHomeSubClusterRequest request)
      throws FederationStateStoreInvalidInputException {
    if (request == null) {
      String message = "Missing UpdateReservationHomeSubCluster Request." +
          " Please try again by specifying an ReservationHomeSubCluster information.";
      LOG.warn(message);
      throw new FederationStateStoreInvalidInputException(message);
    }

    // 校验预留归属子集群信息
    checkReservationHomeSubCluster(request.getReservationHomeSubCluster());
  }

  /**
   * 校验删除预留归属子集群请求的输入合法性，提前检查明显错误实现快速失败。
   *
   * @param request 待校验的删除请求
   * @throws FederationStateStoreInvalidInputException 如果请求非法抛出异常
   */
  public static void validate(DeleteReservationHomeSubClusterRequest request)
      throws FederationStateStoreInvalidInputException {
    if (request == null) {
      String message = "Missing DeleteReservationHomeSubCluster Request." +
          " Please try again by specifying an ReservationHomeSubCluster information.";
      LOG.warn(message);
      throw new FederationStateStoreInvalidInputException(message);
    }

    // 校验预留ID合法性
    checkReservationId(request.getReservationId());
  }
}