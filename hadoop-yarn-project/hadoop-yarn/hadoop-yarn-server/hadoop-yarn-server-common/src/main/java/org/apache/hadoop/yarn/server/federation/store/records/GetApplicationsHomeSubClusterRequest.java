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

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.util.Records;

/**
 * YARN联邦存储获取活跃应用归属子集群映射请求类，用于从联邦状态存储查询应用归属子集群信息。
 */
@Private
@Unstable
public abstract class GetApplicationsHomeSubClusterRequest {

  /**
   * 创建不指定子集群的获取应用归属请求实例，查询所有活跃应用。
   *
   * @return 获取应用归属请求实例
   */
  @Private
  @Unstable
  public static GetApplicationsHomeSubClusterRequest newInstance() {
    GetApplicationsHomeSubClusterRequest request =
        Records.newRecord(GetApplicationsHomeSubClusterRequest.class);
    return request;
  }

  /**
   * 创建指定子集群的获取应用归属请求实例，仅查询该子集群上的活跃应用。
   *
   * @param subClusterId 目标子集群标识
   * @return 获取应用归属请求实例
   */
  @Private
  @Unstable
  public static GetApplicationsHomeSubClusterRequest
      newInstance(SubClusterId subClusterId) {
    GetApplicationsHomeSubClusterRequest request =
        Records.newRecord(GetApplicationsHomeSubClusterRequest.class);
    request.setSubClusterId(subClusterId);
    return request;
  }

  /**
   * 获取请求查询的目标子集群标识。
   *
   * @return 目标子集群标识，空表示查询所有子集群
   */
  @Public
  @Unstable
  public abstract SubClusterId getSubClusterId();

  /**
   * 设置请求查询的目标子集群标识。
   *
   * @param subClusterId 目标子集群标识
   */
  @Public
  @Unstable
  public abstract void setSubClusterId(SubClusterId subClusterId);
}