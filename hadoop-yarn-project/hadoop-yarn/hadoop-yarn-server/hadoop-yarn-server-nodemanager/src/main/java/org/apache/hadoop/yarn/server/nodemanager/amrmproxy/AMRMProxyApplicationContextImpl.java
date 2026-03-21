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

package org.apache.hadoop.yarn.server.nodemanager.amrmproxy;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.registry.client.api.RegistryOperations;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.server.nodemanager.Context;

/**
 * 文件说明：AMRMProxy应用上下文实现类，为请求拦截器链提供当前应用所需的所有上下文信息
 * 封装应用尝试运行所需的核心信息，供AMRMProxy的请求拦截器使用
 *
 */
public class AMRMProxyApplicationContextImpl implements
    AMRMProxyApplicationContext {
  // 节点管理器配置
  private final Configuration conf;
  // NodeManager全局上下文
  private final Context nmContext;
  // 当前应用尝试ID
  private final ApplicationAttemptId applicationAttemptId;
  // 应用提交用户名
  private final String user;
  // 本地AMRM令牌Key ID缓存
  private Integer localTokenKeyId;
  // ResourceManager颁发的原始AMRM令牌
  private Token<AMRMTokenIdentifier> amrmToken;
  // AMRMProxy颁发的本地AMRM令牌
  private Token<AMRMTokenIdentifier> localToken;
  // 应用凭证信息
  private Credentials credentials;
  // YARN服务注册中心客户端操作接口
  private RegistryOperations registry;

  /**
   * 构造AMRMProxy应用上下文实例
   *
   * @param nmContext NodeManager全局上下文
   * @param conf 节点配置信息
   * @param applicationAttemptId 应用尝试ID
   * @param user 应用提交用户名
   * @param amrmToken ResourceManager颁发的原始AMRM令牌
   * @param localToken AMRMProxy颁发的本地AMRM令牌
   * @param credentials 应用凭证信息
   * @param registry YARN服务注册中心客户端
   */
  @SuppressWarnings("checkstyle:parameternumber")
  public AMRMProxyApplicationContextImpl(Context nmContext, Configuration conf,
      ApplicationAttemptId applicationAttemptId, String user,
      Token<AMRMTokenIdentifier> amrmToken,
      Token<AMRMTokenIdentifier> localToken, Credentials credentials,
      RegistryOperations registry) {
    this.nmContext = nmContext;
    this.conf = conf;
    this.applicationAttemptId = applicationAttemptId;
    this.user = user;
    this.amrmToken = amrmToken;
    this.localToken = localToken;
    this.credentials = credentials;
    this.registry = registry;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public ApplicationAttemptId getApplicationAttemptId() {
    return applicationAttemptId;
  }

  @Override
  public String getUser() {
    return user;
  }

  @Override
  public synchronized Token<AMRMTokenIdentifier> getAMRMToken() {
    return amrmToken;
  }

  /**
   * 更新应用的AMRM令牌
   *
   * @param amrmToken 从ResourceManager获取的新AMRM令牌
   * @return 是否成功更新为不同的令牌
   */
  public synchronized boolean setAMRMToken(
      Token<AMRMTokenIdentifier> amrmToken) {
    Token<AMRMTokenIdentifier> oldValue = this.amrmToken;
    this.amrmToken = amrmToken;
    return !this.amrmToken.equals(oldValue);
  }

  @Override
  public synchronized Token<AMRMTokenIdentifier> getLocalAMRMToken() {
    return this.localToken;
  }

  /**
   * 设置AMRMProxy颁发的本地AMRM令牌
   *
   * @param localToken AMRMProxy颁发的本地AMRM令牌
   */
  public synchronized void setLocalAMRMToken(
      Token<AMRMTokenIdentifier> localToken) {
    this.localToken = localToken;
    this.localTokenKeyId = null;
  }

  @Private
  public synchronized int getLocalAMRMTokenKeyId() {
    Integer keyId = this.localTokenKeyId;
    // 缓存未命中时重新解析
    if (keyId == null) {
      try {
        if (this.localToken == null) {
          throw new YarnRuntimeException("Missing AMRM token for "
              + this.applicationAttemptId);
        }
        // 从令牌中解析获取Key ID
        keyId = this.localToken.decodeIdentifier().getKeyId();
        this.localTokenKeyId = keyId;
      } catch (IOException e) {
        throw new YarnRuntimeException("AMRM token decode error for "
            + this.applicationAttemptId, e);
      }
    }
    return keyId;
  }

  @Override
  public Context getNMContext() {
    return nmContext;
  }

  @Override
  public Credentials getCredentials() {
    return this.credentials;
  }

  @Override
  public RegistryOperations getRegistryClient() {
    return this.registry;
  }
}