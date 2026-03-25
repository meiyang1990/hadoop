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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.registry.client.api.RegistryOperations;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.server.nodemanager.Context;

/**
 * AMRMProxy应用上下文接口，供拦截器插件获取单个应用的相关信息。
 * 该接口为AMRMProxy的拦截扩展机制提供了统一的应用信息访问入口。
 */
public interface AMRMProxyApplicationContext {

  /**
   * 获取当前节点的配置对象。
   * @return 配置对象实例
   */
  Configuration getConf();

  /**
   * 获取当前应用尝试的唯一标识。
   * @return 应用尝试标识
   */
  ApplicationAttemptId getApplicationAttemptId();

  /**
   * 获取提交该应用的用户名。
   * @return 应用提交用户名
   */
  String getUser();

  /**
   * 获取ResourceManager颁发给本应用的原始AMRM令牌。
   * @return RM颁发的AMRM令牌
   */
  Token<AMRMTokenIdentifier> getAMRMToken();

  /**
   * 获取AMRMProxy服务为本应用颁发的本地AMRM令牌。
   * @return AMRMProxy颁发的本地AMRM令牌
   */
  Token<AMRMTokenIdentifier> getLocalAMRMToken();

  /**
   * 获取NodeManager的全局上下文对象。
   * @return NodeManager上下文
   */
  Context getNMContext();

  /**
   * 获取当前应用的凭证信息。
   * @return 应用凭证
   */
  Credentials getCredentials();

  /**
   * 获取服务注册中心客户端操作实例。
   * @return 注册中心操作客户端
   */
  RegistryOperations getRegistryClient();

}