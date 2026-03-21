// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.yarn.server.federation.policies.amrmproxy;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.policies.ConfigurableFederationPolicy;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;

/**
 * YARN联邦AMRMProxy路由策略接口，定义了将ApplicationMaster发出的资源请求分发到多个子集群ResourceManager的策略契约。
 * 实现该接口的类负责将AM收到的资源请求列表拆分路由到不同子集群RM。
 */
public interface FederationAMRMProxyPolicy
    extends ConfigurableFederationPolicy {

  /**
   * 根据当前策略规则，将AM发来的资源请求拆分路由到一个或多个子集群。
   * 常见策略实现包括广播到所有子集群、按负载选择子集群等。
   *
   * @param resourceRequests AM发送的资源请求列表，待拆分路由
   * @param timedOutSubClusters 超时未心跳响应的子集群集合，策略需排除这些不可用子集群
   * @return 子集群ID -> 待转发给该子集群的资源请求列表 映射结果
   * @throws YarnException 请求格式错误或找不到可用子集群时抛出异常
   */
  Map<SubClusterId, List<ResourceRequest>> splitResourceRequests(
      List<ResourceRequest> resourceRequests,
      Set<SubClusterId> timedOutSubClusters) throws YarnException;

  /**
   * 通知策略从子集群收到Allocate响应，供有状态策略基于历史响应做路由决策。
   * 例如基于负载的策略可通过该方法更新各子集群剩余资源统计。
   *
   * @param subClusterId 发送响应的子集群ID
   * @param response 从子集群RM收到的Allocate响应
   *
   * @throws YarnException 响应格式非法时抛出异常
   */
  void notifyOfResponse(SubClusterId subClusterId, AllocateResponse response)
      throws YarnException;

}