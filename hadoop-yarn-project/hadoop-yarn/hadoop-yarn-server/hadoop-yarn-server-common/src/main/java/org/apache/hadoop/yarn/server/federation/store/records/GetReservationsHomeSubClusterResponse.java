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

package org.apache.hadoop.yarn.server.federation.store.records;

import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.util.Records;

/**
 * <p>
 * YARN联邦状态存储响应类，用于响应查询所有已提交预留资源归属子集群的请求。
 *
 * <p>
 * 响应包含预留资源与归属子集群的映射信息，具体包括：
 * <ul>
 * <li>{@code ReservationId} 预留资源ID</li>
 * <li>{@code SubClusterId} 归属子集群ID</li>
 * </ul>
 */
@Private
@Unstable
public abstract class GetReservationsHomeSubClusterResponse {

  /**
   * 创建包含所有预留归属子集群映射的响应实例。
   * @param appsHomeSubClusters 预留资源-归属子集群映射列表
   * @return 新建的响应对象
   */
  @Private
  @Unstable
  public static GetReservationsHomeSubClusterResponse newInstance(
      List<ReservationHomeSubCluster> appsHomeSubClusters) {
    GetReservationsHomeSubClusterResponse mapResponse =
        Records.newRecord(GetReservationsHomeSubClusterResponse.class);
    mapResponse.setAppsHomeSubClusters(appsHomeSubClusters);
    return mapResponse;
  }

  /**
   * 获取所有已提交预留资源与其归属子集群的映射列表。
   *
   * @return 预留资源-归属子集群映射列表
   */
  @Public
  @Unstable
  public abstract List<ReservationHomeSubCluster> getAppsHomeSubClusters();

  /**
   * 设置所有已提交预留资源与其归属子集群的映射列表。
   *
   * @param reservationsHomeSubClusters 预留资源-归属子集群映射列表
   */
  @Private
  @Unstable
  public abstract void setAppsHomeSubClusters(
      List<ReservationHomeSubCluster> reservationsHomeSubClusters);
}