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

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.util.Records;

/**
 * 获取应用归属子集群响应类，YARN联邦状态存储查询应用归属子集群后返回的结果封装。
 * 
 * <p>
 * 响应包含应用与归属子集群的映射信息，包括：
 * <ul>
 * <li>{@code ApplicationId} 应用ID</li>
 * <li>{@code SubClusterId} 归属子集群ID</li>
 * </ul>
 */
@Private
@Unstable
public abstract class GetApplicationHomeSubClusterResponse {

  /**
   * 创建仅包含应用ID和归属子集群ID的响应实例。
   * @param appId 应用ID
   * @param homeSubCluster 归属子集群ID
   * @return 新建的查询响应对象
   */
  @Private
  @Unstable
  public static GetApplicationHomeSubClusterResponse newInstance(
      ApplicationId appId, SubClusterId homeSubCluster) {
    ApplicationHomeSubCluster applicationHomeSubCluster =
        ApplicationHomeSubCluster.newInstance(appId, homeSubCluster);
    GetApplicationHomeSubClusterResponse mapResponse =
        Records.newRecord(GetApplicationHomeSubClusterResponse.class);
    mapResponse.setApplicationHomeSubCluster(applicationHomeSubCluster);
    return mapResponse;
  }

  /**
   * 创建包含应用ID、创建时间和归属子集群ID的响应实例。
   * @param appId 应用ID
   * @param homeSubCluster 归属子集群ID
   * @param createTime 应用创建时间
   * @return 新建的查询响应对象
   */
  @Private
  @Unstable
  public static GetApplicationHomeSubClusterResponse newInstance(
      ApplicationId appId, SubClusterId homeSubCluster, long createTime) {
    ApplicationHomeSubCluster applicationHomeSubCluster =
        ApplicationHomeSubCluster.newInstance(appId, createTime, homeSubCluster);
    GetApplicationHomeSubClusterResponse mapResponse =
        Records.newRecord(GetApplicationHomeSubClusterResponse.class);
    mapResponse.setApplicationHomeSubCluster(applicationHomeSubCluster);
    return mapResponse;
  }

  /**
   * 创建包含完整应用信息的响应实例，包含应用提交上下文。
   * @param appId 应用ID
   * @param homeSubCluster 归属子集群ID
   * @param createTime 应用创建时间
   * @param context 应用提交上下文
   * @return 新建的查询响应对象
   */
  @Private
  @Unstable
  public static GetApplicationHomeSubClusterResponse newInstance(
      ApplicationId appId, SubClusterId homeSubCluster, long createTime,
      ApplicationSubmissionContext context) {
    ApplicationHomeSubCluster applicationHomeSubCluster =
        ApplicationHomeSubCluster.newInstance(appId, createTime, homeSubCluster, context);
    GetApplicationHomeSubClusterResponse mapResponse =
        Records.newRecord(GetApplicationHomeSubClusterResponse.class);
    mapResponse.setApplicationHomeSubCluster(applicationHomeSubCluster);
    return mapResponse;
  }

  /**
   * 获取应用到归属子集群的完整映射信息。
   *
   * @return 应用与归属子集群映射对象
   */
  @Public
  @Unstable
  public abstract ApplicationHomeSubCluster getApplicationHomeSubCluster();

  /**
   * 设置应用到归属子集群的映射信息。
   *
   * @param applicationHomeSubCluster 应用与归属子集群映射对象
   */
  @Private
  @Unstable
  public abstract void setApplicationHomeSubCluster(
      ApplicationHomeSubCluster applicationHomeSubCluster);
}