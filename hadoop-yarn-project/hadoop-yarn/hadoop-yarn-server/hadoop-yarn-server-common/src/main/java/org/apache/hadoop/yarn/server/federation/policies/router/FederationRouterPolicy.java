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

package org.apache.hadoop.yarn.server.federation.policies.router;

import java.util.List;

import org.apache.hadoop.yarn.api.protocolrecords.ReservationSubmissionRequest;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.federation.policies.ConfigurableFederationPolicy;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;

/**
 * YARN联邦路由器策略接口，定义根据指定策略决定应用提交路由到哪个子集群的核心逻辑。
 */
public interface FederationRouterPolicy extends ConfigurableFederationPolicy {

  /**
   * 根据策略确定用户应用提交应该路由到的目标子集群。
   *
   * @param appSubmissionContext 应用提交上下文，包含需要路由的应用信息
   * @param blackListSubClusters 需要排除的黑名单子集群列表，这些子集群不会被选中
   * @return 本次应用执行的目标子集群ID
   * @throws YarnException 当策略无法找到可用子集群时抛出异常
   */
  SubClusterId getHomeSubcluster(
      ApplicationSubmissionContext appSubmissionContext,
      List<SubClusterId> blackListSubClusters) throws YarnException;

  /**
   * 根据策略确定资源配额提交请求应该路由到的目标子集群。
   *
   * @param request 原始资源配额提交请求
   * @return 本次配额提交的目标子集群ID
   * @throws YarnException 当策略无法选择目标子集群时抛出异常
   */
  SubClusterId getReservationHomeSubcluster(
      ReservationSubmissionRequest request) throws YarnException;
}