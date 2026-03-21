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

package org.apache.hadoop.yarn.server.federation.store.records;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.util.Records;

/**
 * 获取子集群策略配置响应类，封装联邦策略存储返回的子集群路由策略配置信息。
 * 响应包含指定队列对应的策略配置对象，用于YARN联邦环境中路由决策。
 */
@Private
@Unstable
public abstract class GetSubClusterPolicyConfigurationResponse {

  /**
   * 创建新的获取子集群策略配置响应实例，设置返回的策略配置。
   * @param policy 要返回的子集群策略配置
   * @return 初始化完成的响应实例
   */
  @Private
  @Unstable
  public static GetSubClusterPolicyConfigurationResponse newInstance(
      SubClusterPolicyConfiguration policy) {
    GetSubClusterPolicyConfigurationResponse response =
        Records.newRecord(GetSubClusterPolicyConfigurationResponse.class);
    response.setPolicyConfiguration(policy);
    return response;
  }

  /**
   * 获取指定队列对应的子集群策略配置。
   *
   * @return 指定队列的策略配置对象
   */
  @Public
  @Unstable
  public abstract SubClusterPolicyConfiguration getPolicyConfiguration();

  /**
   * 设置指定队列对应的子集群策略配置。
   *
   * @param policyConfiguration 指定队列的策略配置对象
   */
  @Private
  @Unstable
  public abstract void setPolicyConfiguration(
      SubClusterPolicyConfiguration policyConfiguration);

}