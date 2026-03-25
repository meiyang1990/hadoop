// 这个文件已经全部加上中文注释
/**
 *  Licensed to the Apache Software Foundation (ASF) under one
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

package org.apache.hadoop.yarn.server.globalpolicygenerator;

import org.apache.hadoop.yarn.server.federation.utils.FederationRegistryClient;
import org.apache.hadoop.yarn.server.federation.utils.FederationStateStoreFacade;

/**
 * YARN联邦全局策略生成器(GPG)上下文接口，统一持有GPG运行所需的核心组件依赖。
 * 提供全局策略生成过程中各个组件的存取能力，解耦组件实现与业务逻辑。
 */
public interface GPGContext {

  /**
   * 获取联邦状态存储门面，用于读写集群联邦的元数据信息。
   * @return 联邦状态存储门面实例
   */
  FederationStateStoreFacade getStateStoreFacade();

  /**
   * 设置联邦状态存储门面实例。
   * @param facade 联邦状态存储门面实例
   */
  void setStateStoreFacade(FederationStateStoreFacade facade);

  /**
   * 获取全局策略门面，用于生成并持久化全局路由策略。
   * @return 全局策略门面实例
   */
  GPGPolicyFacade getPolicyFacade();

  /**
   * 设置全局策略门面实例。
   * @param facade 全局策略门面实例
   */
  void setPolicyFacade(GPGPolicyFacade facade);

  /**
   * 获取联邦服务注册中心客户端，用于与联邦注册中心交互。
   * @return 联邦注册中心客户端实例
   */
  FederationRegistryClient getRegistryClient();

  /**
   * 设置联邦服务注册中心客户端实例。
   * @param client 联邦注册中心客户端实例
   */
  void setRegistryClient(FederationRegistryClient client);
}