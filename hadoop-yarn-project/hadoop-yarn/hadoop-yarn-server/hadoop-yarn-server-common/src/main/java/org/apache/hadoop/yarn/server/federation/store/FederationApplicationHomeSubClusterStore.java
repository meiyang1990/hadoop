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
import org.apache.hadoop.yarn.server.federation.store.records.AddApplicationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.AddApplicationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.DeleteApplicationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.DeleteApplicationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetApplicationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetApplicationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetApplicationsHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetApplicationsHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.UpdateApplicationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.UpdateApplicationHomeSubClusterResponse;

/**
 * YARN联邦环境下应用归属子集群存储接口，维护所有提交到联邦集群的应用与运行子集群的映射关系。
 * 存储的核心信息为每个应用ID对应的归属子集群ID，用于联邦路由时快速定位应用所在子集群。
 *
 */
@Private
@Unstable
public interface FederationApplicationHomeSubClusterStore {

  /**
   * 为新提交的应用注册归属子集群映射。
   * 若应用已存在映射关系，将返回原有映射而非覆盖。操作成功响应为空，失败抛出异常。
   *
   * @param request 新增应用映射请求，包含应用ID和待注册的归属子集群ID
   * @return 操作成功返回包含应用最终归属子集群的响应，若已有旧映射则返回旧映射
   * @throws YarnException 请求无效或操作失败时抛出异常
   */
  AddApplicationHomeSubClusterResponse addApplicationHomeSubCluster(
      AddApplicationHomeSubClusterRequest request) throws YarnException;

  /**
   * 更新已有应用的归属子集群映射。
   * 操作成功响应为空，失败抛出异常。
   *
   * @param request 更新应用归属请求，包含应用ID和新的归属子集群ID
   * @return 操作成功返回空响应
   * @throws YarnException 请求无效或操作失败时抛出异常
   */
  UpdateApplicationHomeSubClusterResponse updateApplicationHomeSubCluster(
      UpdateApplicationHomeSubClusterRequest request) throws YarnException;

  /**
   * 根据应用ID查询单个应用的归属子集群信息。
   *
   * @param request 查询请求，包含待查询的应用ID
   * @return 返回包含应用归属子集群信息的响应
   * @throws YarnException 请求无效或查询失败时抛出异常
   */
  GetApplicationHomeSubClusterResponse getApplicationHomeSubCluster(
      GetApplicationHomeSubClusterRequest request) throws YarnException;

  /**
   * 查询存储中所有应用的归属子集群映射关系。
   *
   * @param request 空请求，表示查询所有应用
   * @return 返回所有应用到归属子集群的完整映射列表
   * @throws YarnException 请求无效或查询失败时抛出异常
   */
  GetApplicationsHomeSubClusterResponse getApplicationsHomeSubCluster(
      GetApplicationsHomeSubClusterRequest request) throws YarnException;

  /**
   * 删除指定应用的归属子集群映射关系。
   * 操作成功响应为空，失败抛出异常。
   *
   * @param request 删除请求，包含待删除的应用ID
   * @return 操作成功返回空响应
   * @throws YarnException 请求无效或操作失败时抛出异常
   */
  DeleteApplicationHomeSubClusterResponse deleteApplicationHomeSubCluster(
      DeleteApplicationHomeSubClusterRequest request) throws YarnException;

}