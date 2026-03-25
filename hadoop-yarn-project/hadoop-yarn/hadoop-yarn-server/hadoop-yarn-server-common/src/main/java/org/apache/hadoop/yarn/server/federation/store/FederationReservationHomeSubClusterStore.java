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
import org.apache.hadoop.yarn.server.federation.store.records.GetReservationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetReservationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.GetReservationsHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.GetReservationsHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.AddReservationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.AddReservationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.UpdateReservationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.UpdateReservationHomeSubClusterResponse;
import org.apache.hadoop.yarn.server.federation.store.records.DeleteReservationHomeSubClusterRequest;
import org.apache.hadoop.yarn.server.federation.store.records.DeleteReservationHomeSubClusterResponse;

/**
 * YARN联邦预留资源归属子集群存储接口，维护联邦集群中所有已提交预留资源的归属状态。
 * <p>
 * 存储的映射信息包含：
 * <ul>
 * <li>预留ID {@code ReservationId}</li>
 * <li>归属子集群ID {@code SubClusterId}</li>
 * </ul>
 *
 */
@Private
@Unstable
public interface FederationReservationHomeSubClusterStore {

  /**
   * 注册新提交预留的归属子集群。
   * 操作成功则返回响应（若预留已存在则返回原有映射关系，可能与请求中不同），失败则抛出异常。
   *
   * @param request 注册新预留及其归属子集群的请求
   * @return 注册成功后返回包含预留归属子集群的响应，失败抛出异常
   * @throws YarnException 请求无效或操作失败时抛出
   */
  AddReservationHomeSubClusterResponse addReservationHomeSubCluster(
      AddReservationHomeSubClusterRequest request) throws YarnException;

  /**
   * 根据预留ID查询该预留的归属子集群信息。
   *
   * @param request 包含待查询预留ID的请求
   * @return 包含预留归属子集群信息的响应
   * @throws YarnException 请求无效或操作失败时抛出
   */
  GetReservationHomeSubClusterResponse getReservationHomeSubCluster(
      GetReservationHomeSubClusterRequest request) throws YarnException;

  /**
   * 获取所有已提交预留与其归属子集群的完整映射列表。
   *
   * @param request 空请求，表示查询所有预留
   * @return 所有已提交预留与其归属子集群的映射列表
   * @throws YarnException 请求无效或操作失败时抛出
   */
  GetReservationsHomeSubClusterResponse getReservationsHomeSubCluster(
      GetReservationsHomeSubClusterRequest request) throws YarnException;

  /**
   * 更新已有预留的归属子集群信息。
   * 操作成功返回空响应，失败抛出异常。
   *
   * @param request 更新预留归属子集群的请求
   * @return 更新成功返回空响应，失败抛出异常
   * @throws YarnException 请求无效或操作失败时抛出
   */
  UpdateReservationHomeSubClusterResponse updateReservationHomeSubCluster(
      UpdateReservationHomeSubClusterRequest request) throws YarnException;


  /**
   * 删除已有预留的归属子集群映射。
   * 操作成功返回空响应，失败抛出异常。
   *
   * @param request 删除预留归属子集群映射的请求
   * @return 删除成功返回空响应，失败抛出异常
   * @throws YarnException 请求无效或操作失败时抛出
   */
  DeleteReservationHomeSubClusterResponse deleteReservationHomeSubCluster(
      DeleteReservationHomeSubClusterRequest request) throws YarnException;
}