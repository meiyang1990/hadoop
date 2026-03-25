// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.yarn.server.federation.store;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.store.records.RouterMasterKeyRequest;
import org.apache.hadoop.yarn.server.federation.store.records.RouterMasterKeyResponse;
import org.apache.hadoop.yarn.server.federation.store.records.RouterRMTokenRequest;
import org.apache.hadoop.yarn.server.federation.store.records.RouterRMTokenResponse;

import java.io.IOException;

/**
 * 联邦YARN环境的代理令牌状态存储接口，负责维护提交到联邦集群中所有代理令牌的状态信息，
 * 为Router提供跨RM的代理令牌统一存储能力。
 */
@Private
@Unstable
public interface FederationDelegationTokenStateStore {

  /**
   * 存储新生成的代理令牌主密钥。
   * Router生成新的主密钥后，调用该接口将其持久化到状态存储中，供整个联邦集群使用。
   *
   * @param request 请求对象，包含需要存储的Router主密钥，是代理密钥的抽象封装
   * @return 存储操作的响应结果
   * @throws YarnException 如果访问状态存储失败
   * @throws IOException 如果发生IO错误
   */
  RouterMasterKeyResponse storeNewMasterKey(RouterMasterKeyRequest request)
      throws YarnException, IOException;

  /**
   * 从状态存储中移除指定的代理令牌主密钥。
   * 主密钥过期轮转后，调用该接口清理已失效的主密钥。
   *
   * @param request 请求对象，包含需要删除的Router主密钥
   * @return 删除操作的响应结果
   * @throws YarnException 如果访问状态存储失败
   * @throws IOException 如果发生IO错误
   */
  RouterMasterKeyResponse removeStoredMasterKey(RouterMasterKeyRequest request)
      throws YarnException, IOException;

  /**
   * 根据代理密钥从存储中获取对应的主密钥。
   * 验证代理令牌签名时调用该接口查询对应密钥。
   *
   * @param request 请求对象，包含需要查询的代理密钥标识
   * @return 查询得到的主密钥响应结果
   * @throws YarnException 如果访问状态存储失败
   * @throws IOException 如果发生IO错误
   */
  RouterMasterKeyResponse getMasterKeyByDelegationKey(RouterMasterKeyRequest request)
      throws YarnException, IOException;

  /**
   * 存储新的RM代理令牌信息。
   * Router向联邦集群颁发新的RM代理令牌后，调用该接口持久化令牌信息。
   *
   * @param request 请求对象，包含RouterRM令牌信息，封装了RM代理令牌标识符和更新时间
   * @return 存储操作的响应结果
   * @throws YarnException 如果访问状态存储失败
   * @throws IOException 如果发生IO错误
   */
  RouterRMTokenResponse storeNewToken(RouterRMTokenRequest request)
      throws YarnException, IOException;

  /**
   * 更新存储中已有的RM代理令牌信息。
   * 代理令牌续期后，调用该接口更新令牌的续期时间信息。
   *
   * @param request 请求对象，包含需要更新的RouterRM令牌和新的续期时间
   * @return 更新操作的响应结果
   * @throws YarnException 如果访问状态存储失败
   * @throws IOException 如果发生IO错误
   */
  RouterRMTokenResponse updateStoredToken(RouterRMTokenRequest request)
      throws YarnException, IOException;

  /**
   * 从存储中移除指定的RM代理令牌信息。
   * 令牌过期或取消后，调用该接口清理已失效的令牌数据。
   *
   * @param request 请求对象，包含需要删除的RouterRM令牌
   * @return 删除操作的响应结果
   * @throws YarnException 如果访问状态存储失败
   * @throws IOException 如果发生IO错误
   */
  RouterRMTokenResponse removeStoredToken(RouterRMTokenRequest request)
      throws YarnException, IOException;

  /**
   * 根据请求从存储中查询对应的RM代理令牌信息。
   * 验证令牌有效性时调用该接口获取存储的令牌数据。
   *
   * @param request 请求对象，包含需要查询的RM代理令牌标识
   * @return 查询得到的RM代理令牌响应结果
   * @throws YarnException 如果访问状态存储失败
   * @throws IOException 如果发生IO错误
   */
  RouterRMTokenResponse getTokenByRouterStoreToken(RouterRMTokenRequest request)
      throws YarnException, IOException;

  /**
   * 原子递增代理令牌序列号，生成下一个可用的序列号。
   *
   * @return 递增后的代理令牌序列号
   */
  int incrementDelegationTokenSeqNum();

  /**
   * 获取当前代理令牌序列号。
   *
   * @return 当前代理令牌序列号
   */
  int getDelegationTokenSeqNum();

  /**
   * 设置代理令牌序列号，用于初始化或恢复状态。
   *
   * @param seqNum 需要设置的代理令牌序列号
   */
  void setDelegationTokenSeqNum(int seqNum);

  /**
   * 获取当前生效的主密钥ID。
   *
   * @return 当前主密钥ID
   */
  int getCurrentKeyId();

  /**
   * 原子递增当前主密钥ID，生成下一个新主密钥的ID。
   *
   * @return 递增后的当前主密钥ID
   */
  int incrementCurrentKeyId();
}