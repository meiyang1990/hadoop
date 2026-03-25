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

package org.apache.hadoop.yarn.server.federation.store;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterPoliciesConfigurationsRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterPoliciesConfigurationsResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterPolicyConfigurationRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetSubClusterPolicyConfigurationResponse;
import org.apache.hadoop.yarn.server.federation.store.records.SetSubClusterPolicyConfigurationRequest;
import org.apache.hadoop.yarn.server.federation.store.records.SetSubClusterPolicyConfigurationResponse;
import org.apache.hadoop.yarn.server.federation.store.records.DeleteSubClusterPoliciesConfigurationsRequest;
import org.apache.hadoop.yarn.server.federation.store.records.DeleteSubClusterPoliciesConfigurationsResponse;
import org.apache.hadoop.yarn.server.federation.store.records.DeletePoliciesConfigurationsRequest;
import org.apache.hadoop.yarn.server.federation.store.records.DeletePoliciesConfigurationsResponse;

/**
 * YARN联邦路由策略存储接口，提供对队列级路由策略配置的存取能力。
 * 策略以队列名为键存储，值为序列化的策略配置信息，支持为每个队列单独配置不同的
 * 子集群选择路由策略，策略本身可在运行时基于作业/任务动态决策。
 */
@Private
@Unstable
public interface FederationPolicyStore {

  /**
   * 获取指定队列的路由策略配置。
   *
   * @param request 请求，包含需要查询的队列名
   * @return 指定队列的路由策略配置，不存在该队列时返回null
   * @throws YarnException 请求无效或执行失败时抛出异常
   */
  GetSubClusterPolicyConfigurationResponse getPolicyConfiguration(
      GetSubClusterPolicyConfigurationRequest request) throws YarnException;

  /**
   * 设置指定队列的路由策略配置。
   *
   * @param request 请求，包含队列名和对应的路由策略配置
   * @return 更新成功返回空响应
   * @throws YarnException 请求无效或执行失败时抛出异常
   */
  SetSubClusterPolicyConfigurationResponse setPolicyConfiguration(
      SetSubClusterPolicyConfigurationRequest request) throws YarnException;

  /**
   * 获取当前系统中所有队列的路由策略配置映射。
   *
   * @param request 请求，空请求表示查询所有已配置队列
   * @return 所有当前激活队列的路由策略配置集合
   * @throws YarnException 请求无效或执行失败时抛出异常
   */
  GetSubClusterPoliciesConfigurationsResponse getPoliciesConfigurations(
      GetSubClusterPoliciesConfigurationsRequest request) throws YarnException;

  /**
   * 批量删除指定队列的路由策略配置。
   *
   * @param request 请求，包含待删除的队列列表
   * @return 删除成功返回空响应
   * @throws YarnException 请求无效或执行失败时抛出异常
   */
  DeleteSubClusterPoliciesConfigurationsResponse deletePoliciesConfigurations(
      DeleteSubClusterPoliciesConfigurationsRequest request) throws YarnException;

  /**
   * 删除所有队列的路由策略配置。
   *
   * @param request 删除请求
   * @return 删除成功返回空响应
   * @throws Exception 请求无效或执行失败时抛出异常
   */
  DeletePoliciesConfigurationsResponse deleteAllPoliciesConfigurations(
      DeletePoliciesConfigurationsRequest request) throws Exception;
}