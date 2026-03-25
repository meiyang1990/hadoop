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
 * 全局策略生成器(GPG)上下文实现类，持有GPG运行所需的核心依赖门面和客户端实例。
 */
public class GPGContextImpl implements GPGContext {

  // 联邦状态存储门面实例，用于访问联邦集群状态数据
  private FederationStateStoreFacade facade;
  // 全局策略门面实例，用于生成全局策略
  private GPGPolicyFacade policyFacade;
  // 联邦注册中心客户端，用于与联邦注册中心交互
  private FederationRegistryClient registryClient;

  @Override
  public FederationStateStoreFacade getStateStoreFacade() {
    return facade;
  }

  @Override
  public void setStateStoreFacade(
      FederationStateStoreFacade federationStateStoreFacade) {
    this.facade = federationStateStoreFacade;
  }

  @Override
  public GPGPolicyFacade getPolicyFacade(){
    return policyFacade;
  }

  @Override
  public void setPolicyFacade(GPGPolicyFacade gpgPolicyfacade){
    policyFacade = gpgPolicyfacade;
  }

  @Override
  public FederationRegistryClient getRegistryClient() {
    return registryClient;
  }

  @Override
  public void setRegistryClient(FederationRegistryClient client) {
    registryClient = client;
  }
}