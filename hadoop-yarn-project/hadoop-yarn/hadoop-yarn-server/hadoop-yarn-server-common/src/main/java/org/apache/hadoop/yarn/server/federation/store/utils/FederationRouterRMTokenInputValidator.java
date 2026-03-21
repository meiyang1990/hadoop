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

import org.apache.hadoop.yarn.security.client.YARNDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.federation.store.exception.FederationStateStoreInvalidInputException;
import org.apache.hadoop.yarn.server.federation.store.records.RouterRMTokenRequest;
import org.apache.hadoop.yarn.server.federation.store.records.RouterStoreToken;
import org.apache.hadoop.yarn.server.federation.store.records.RouterMasterKeyRequest;
import org.apache.hadoop.yarn.server.federation.store.records.RouterMasterKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 联邦环境下Router资源管理器令牌相关请求的输入参数校验工具类，
 * 用于校验RouterRMTokenRequest和RouterMasterKeyRequest的合法性，防止非法请求写入状态存储。
 */
public final class FederationRouterRMTokenInputValidator {

  private static final Logger LOG =
      LoggerFactory.getLogger(FederationRouterRMTokenInputValidator.class);

  private FederationRouterRMTokenInputValidator() {
  }

  /**
   * 校验RouterRMTokenRequest请求参数的合法性，确保请求和包含的令牌信息不为空。
   *
   * @param request 待校验的RouterRMToken请求对象
   * @throws FederationStateStoreInvalidInputException 如果请求参数非法则抛出此异常
   */
  public static void validate(RouterRMTokenRequest request)
      throws FederationStateStoreInvalidInputException {

    // 校验请求对象本身不为空
    if (request == null) {
      String message = "Missing RouterRMToken Request."
          + " Please try again by specifying a router rm token information.";
      LOG.warn(message);
      throw new FederationStateStoreInvalidInputException(message);
    }

    // 获取请求中的存储令牌并校验不为空
    RouterStoreToken storeToken = request.getRouterStoreToken();
    if (storeToken == null) {
      String message = "Missing RouterStoreToken."
          + " Please try again by specifying a router rm token information.";
      LOG.warn(message);
      throw new FederationStateStoreInvalidInputException(message);
    }

    // 校验令牌标识符不为空，捕获解析过程中的异常
    try {
      YARNDelegationTokenIdentifier identifier = storeToken.getTokenIdentifier();
      if (identifier == null) {
        String message = "Missing YARNDelegationTokenIdentifier."
            + " Please try again by specifying a router rm token information.";
        LOG.warn(message);
        throw new FederationStateStoreInvalidInputException(message);
      }
    } catch (Exception e) {
      throw new FederationStateStoreInvalidInputException(e);
    }
  }

  /**
   * 校验RouterMasterKeyRequest请求参数的合法性，确保请求和包含的主密钥信息不为空。
   *
   * @param request 待校验的RouterMasterKey请求对象
   * @throws FederationStateStoreInvalidInputException 如果请求参数非法则抛出此异常
   */
  public static void validate(RouterMasterKeyRequest request)
      throws FederationStateStoreInvalidInputException {

    // 校验请求对象本身不为空
    if (request == null) {
      String message = "Missing RouterMasterKey Request."
          + " Please try again by specifying a router master key request information.";
      LOG.warn(message);
      throw new FederationStateStoreInvalidInputException(message);
    }

    // 获取请求中的主密钥并校验不为空
    RouterMasterKey masterKey = request.getRouterMasterKey();
    if (masterKey == null) {
      String message = "Missing RouterMasterKey."
          + " Please try again by specifying a router master key information.";
      LOG.warn(message);
      throw new FederationStateStoreInvalidInputException(message);
    }
  }
}