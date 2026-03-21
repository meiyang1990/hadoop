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

import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.util.Records;

/**
 * YARN联邦策略存储获取所有子集群路由策略配置响应类，
 * 封装从FederationPolicyStore查询所有已配置路由策略的返回结果。
 */
@Private
@Unstable
public abstract class GetSubClusterPoliciesConfigurationsResponse {

  /**
   * 创建获取所有子集群策略配置响应实例，设置返回的策略列表。
   *
   * @param policyConfigurations 所有已配置的子集群策略列表
   * @return 新建的响应对象实例
   */
  @Private
  @Unstable
  public static GetSubClusterPoliciesConfigurationsResponse newInstance(
      List<SubClusterPolicyConfiguration> policyConfigurations) {
    GetSubClusterPoliciesConfigurationsResponse response =
        Records.newRecord(GetSubClusterPoliciesConfigurationsResponse.class);
    response.setPoliciesConfigs(policyConfigurations);
    return response;
  }

  /**
   * 获取系统中所有已配置的子集群路由策略。
   *
   * @return 所有已配置的子集群路由策略列表
   */
  @Public
  @Unstable
  public abstract List<SubClusterPolicyConfiguration> getPoliciesConfigs();

  /**
   * 设置系统中所有已配置的子集群路由策略。
   *
   * @param policyConfigurations 所有已配置的子集群路由策略列表
   */
  @Private
  @Unstable
  public abstract void setPoliciesConfigs(
      List<SubClusterPolicyConfiguration> policyConfigurations);

}