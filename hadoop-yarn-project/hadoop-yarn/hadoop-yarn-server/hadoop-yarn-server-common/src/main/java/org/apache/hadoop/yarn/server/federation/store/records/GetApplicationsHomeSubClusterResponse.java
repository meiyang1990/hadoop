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
 * 联邦状态存储查询所有已提交应用归属子集群的响应封装
 * 
 * <p>
 * 响应包含所有应用与归属子集群的映射信息，具体包括：
 * <ul>
 * <li>应用编号 {@code ApplicationId}</li>
 * <li>子集群编号 {@code SubClusterId}</li>
 * </ul>
 */
@Private
@Unstable
public abstract class GetApplicationsHomeSubClusterResponse {

  /**
   * 创建查询所有应用归属子集群响应实例，设置映射列表。
   * 
   * @param appsHomeSubClusters 应用归属子集群映射列表
   * @return 完整的查询响应对象
   */
  @Private
  @Unstable
  public static GetApplicationsHomeSubClusterResponse newInstance(
      List<ApplicationHomeSubCluster> appsHomeSubClusters) {
    GetApplicationsHomeSubClusterResponse mapResponse =
        Records.newRecord(GetApplicationsHomeSubClusterResponse.class);
    mapResponse.setAppsHomeSubClusters(appsHomeSubClusters);
    return mapResponse;
  }

  /**
   * 获取所有已提交应用到归属子集群的映射列表。
   *
   * @return 应用与归属子集群的映射列表
   */
  @Public
  @Unstable
  public abstract List<ApplicationHomeSubCluster> getAppsHomeSubClusters();

  /**
   * 设置所有已提交应用到归属子集群的映射列表。
   *
   * @param appsHomeSubClusters 应用与归属子集群的映射列表
   */
  @Private
  @Unstable
  public abstract void setAppsHomeSubClusters(
      List<ApplicationHomeSubCluster> appsHomeSubClusters);
}