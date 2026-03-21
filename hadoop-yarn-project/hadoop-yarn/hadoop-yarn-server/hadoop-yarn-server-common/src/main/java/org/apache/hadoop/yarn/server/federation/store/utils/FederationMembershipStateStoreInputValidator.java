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

import java.net.URI;

import org.apache.hadoop.yarn.server.federation.store.exception.FederationStateStoreInvalidInputException;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterInfoRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterDeregisterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterHeartbeatRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterInfo;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterRegisterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 联邦成员状态存储输入参数验证工具类，实现快速失败机制，提前拦截非法用户输入。
 * 为 FederationMembershipStateStore 提供各类请求的参数合法性检查。
 *
 */
public final class FederationMembershipStateStoreInputValidator {

  private static final Logger LOG = LoggerFactory
      .getLogger(FederationMembershipStateStoreInputValidator.class);

  private FederationMembershipStateStoreInputValidator() {
  }

  /**
   * 验证子集群注册请求参数的合法性，提前拦截明显错误参数实现快速失败。
   *
   * @param request 待验证的子集群注册请求
   * @throws FederationStateStoreInvalidInputException 如果请求参数非法抛出异常
   */
  public static void validate(SubClusterRegisterRequest request)
      throws FederationStateStoreInvalidInputException {

    // 检查请求对象非空
    if (request == null) {
      String message = "Missing SubClusterRegister Request."
          + " Please try again by specifying a"
          + " SubCluster Register Information.";
      LOG.warn(message);
      throw new FederationStateStoreInvalidInputException(message);

    }

    // 验证子集群信息完整性
    checkSubClusterInfo(request.getSubClusterInfo());
  }

  /**
   * 验证子集群注销请求参数的合法性，提前拦截明显错误参数实现快速失败。
   *
   * @param request 待验证的子集群注销请求
   * @throws FederationStateStoreInvalidInputException 如果请求参数非法抛出异常
   */
  public static void validate(SubClusterDeregisterRequest request)
      throws FederationStateStoreInvalidInputException {

    // 检查请求对象非空
    if (request == null) {
      String message = "Missing SubClusterDeregister Request."
          + " Please try again by specifying a"
          + " SubCluster Deregister Information.";
      LOG.warn(message);
      throw new FederationStateStoreInvalidInputException(message);
    }

    // 验证子集群ID合法性
    checkSubClusterId(request.getSubClusterId());
    // 验证子集群状态合法性
    checkSubClusterState(request.getState());
    // 注销操作要求状态必须是最终状态
    if (!request.getState().isFinal()) {
      String message = "Invalid non-final state: " + request.getState();
      LOG.warn(message);
      throw new FederationStateStoreInvalidInputException(message);
    }
  }

  /**
   * 验证子集群心跳请求参数的合法性，提前拦截明显错误参数实现快速失败。
   *
   * @param request 待验证的子集群心跳请求
   * @throws FederationStateStoreInvalidInputException 如果请求参数非法抛出异常
   */
  public static void validate(SubClusterHeartbeatRequest request)
      throws FederationStateStoreInvalidInputException {

    // 检查请求对象非空
    if (request == null) {
      String message = "Missing SubClusterHeartbeat Request."
          + " Please try again by specifying a"
          + " SubCluster Heartbeat Information.";
      LOG.warn(message);
      throw new FederationStateStoreInvalidInputException(message);
    }

    // 验证子集群ID合法性
    checkSubClusterId(request.getSubClusterId());
    // 验证最后心跳时间戳合法性
    checkTimestamp(request.getLastHeartBeat());
    // 验证子集群能力描述合法性
    checkCapability(request.getCapability());
    // 验证子集群状态合法性
    checkSubClusterState(request.getState());

  }

  /**
   * 验证查询子集群信息请求参数的合法性，提前拦截明显错误参数实现快速失败。
   *
   * @param request 待验证的子集群信息查询请求
   * @throws FederationStateStoreInvalidInputException 如果请求参数非法抛出异常
   */
  public static void validate(GetSubClusterInfoRequest request)
      throws FederationStateStoreInvalidInputException {

    // 检查请求对象非空
    if (request == null) {
      String message = "Missing GetSubClusterInfo Request."
          + " Please try again by specifying a Get SubCluster information.";
      LOG.warn(message);
      throw new FederationStateStoreInvalidInputException(message);
    }

    // 验证子集群ID合法性
    checkSubClusterId(request.getSubClusterId());
  }

  /**
   * 验证子集群信息所有必填字段完整性。
   * 注册过程中RM未完成初始化时，Capability允许为空。
   *
   * @param subClusterInfo 待验证的子集群信息
   * @throws FederationStateStoreInvalidInputException 如果子集群信息非法抛出异常
   */
  public static void checkSubClusterInfo(SubClusterInfo subClusterInfo)
      throws FederationStateStoreInvalidInputException {
    if (subClusterInfo == null) {
      String message = "Missing SubCluster Information."
          + " Please try again by specifying SubCluster Information.";
      LOG.warn(message);
      throw new FederationStateStoreInvalidInputException(message);
    }

    // 验证子集群ID合法性
    checkSubClusterId(subClusterInfo.getSubClusterId());

    // 验证AMRM服务地址合法性
    checkAddress(subClusterInfo.getAMRMServiceAddress());
    // 验证ClientRM服务地址合法性
    checkAddress(subClusterInfo.getClientRMServiceAddress());
    // 验证RMAdmin服务地址合法性
    checkAddress(subClusterInfo.getRMAdminServiceAddress());
    // 验证RMWeb服务地址合法性
    checkAddress(subClusterInfo.getRMWebServiceAddress());

    // 验证最后心跳时间戳合法性
    checkTimestamp(subClusterInfo.getLastHeartBeat());
    // 验证最后启动时间戳合法性
    checkTimestamp(subClusterInfo.getLastStartTime());

    // 验证子集群状态合法性
    checkSubClusterState(subClusterInfo.getState());

  }

  /**
   * 验证时间戳为非负值。
   *
   * @param timestamp 待验证的时间戳
   * @throws FederationStateStoreInvalidInputException 如果时间戳为负抛出异常
   */
  private static void checkTimestamp(long timestamp)
      throws FederationStateStoreInvalidInputException {
    if (timestamp < 0) {
      String message = "Invalid timestamp information."
          + " Please try again by specifying valid Timestamp Information.";
      LOG.warn(message);
      throw new FederationStateStoreInvalidInputException(message);
    }
  }

  /**
   * 验证子集群能力描述非空。
   *
   * @param capability 待验证的子集群能力描述
   * @throws FederationStateStoreInvalidInputException 如果能力描述为空抛出异常
   */
  private static void checkCapability(String capability)
      throws FederationStateStoreInvalidInputException {
    if (capability == null || capability.isEmpty()) {
      String message = "Invalid capability information."
          + " Please try again by specifying valid Capability Information.";
      LOG.warn(message);
      throw new FederationStateStoreInvalidInputException(message);
    }
  }

  /**
   * 验证子集群ID非空且合法。
   *
   * @param subClusterId 待验证的子集群ID
   * @throws FederationStateStoreInvalidInputException 如果子集群ID非法抛出异常
   */
  protected static void checkSubClusterId(SubClusterId subClusterId)
      throws FederationStateStoreInvalidInputException {
    // 检查子集群ID非空
    if (subClusterId == null) {
      String message = "Missing SubCluster Id information."
          + " Please try again by specifying Subcluster Id information.";
      LOG.warn(message);
      throw new FederationStateStoreInvalidInputException(message);
    }
    // 检查子集群ID字符串非空
    if (subClusterId.getId().isEmpty()) {
      String message = "Invalid SubCluster Id information."
          + " Please try again by specifying valid Subcluster Id.";
      LOG.warn(message);
      throw new FederationStateStoreInvalidInputException(message);
    }
  }

  /**
   * 验证子集群端点地址为合法的host:port格式URI。
   *
   * @param address 待验证的子集群端点地址
   * @throws FederationStateStoreInvalidInputException 如果地址格式非法抛出异常
   */
  private static void checkAddress(String address)
      throws FederationStateStoreInvalidInputException {
    // 确保地址非空
    if (address == null || address.isEmpty()) {
      String message = "Missing SubCluster Endpoint information."
          + " Please try again by specifying SubCluster Endpoint information.";
      LOG.warn(message);
      throw new FederationStateStoreInvalidInputException(message);
    }
    // 验证URI格式合法性
    boolean hasScheme = address.contains("://");
    URI uri = null;
    try {
      // 无scheme时添加虚拟scheme方便URI解析
      uri = hasScheme ? URI.create(address)
          : URI.create("dummyscheme://" + address);
    } catch (IllegalArgumentException e) {
      String message = "The provided SubCluster Endpoint does not contain a"
          + " valid host:port authority: " + address;
      LOG.warn(message);
      throw new FederationStateStoreInvalidInputException(message);
    }
    String host = uri.getHost();
    int port = uri.getPort();
    String path = uri.getPath();
    // 检查必须包含合法host和port，无scheme时不能包含路径
    if ((host == null) || (port < 0)
        || (!hasScheme && path != null && !path.isEmpty())) {
      String message = "The provided SubCluster Endpoint does not contain a"
          + " valid host:port authority: " + address;
      LOG.warn(message);
      throw new FederationStateStoreInvalidInputException(message);
    }
  }

  /**
   * 验证子集群状态非空。
   *
   * @param state 待验证的子集群状态
   * @throws FederationStateStoreInvalidInputException 如果状态为空抛出异常
   */
  private static void checkSubClusterState(SubClusterState state)
      throws FederationStateStoreInvalidInputException {
    // 检查子集群状态非空
    if (state == null) {
      String message = "Missing SubCluster State information."
          + " Please try again by specifying SubCluster State information.";
      LOG.warn(message);
      throw new FederationStateStoreInvalidInputException(message);
    }
  }

}