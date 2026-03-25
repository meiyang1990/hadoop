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
import org.apache.hadoop.yarn.util.Records;

/**
 * <p>
 * 更新应用所属主子集群的请求，由Router发送给联邦状态存储，用于更新新提交应用的主子集群映射。
 *
 * <p>
 * 请求包含映射信息：
 * <ul>
 * <li>应用ID</li>
 * <li>子集群ID</li>
 * </ul>
 */
@Private
@Unstable
public abstract class UpdateApplicationHomeSubClusterRequest {

  /**
   * 创建新的更新应用主子集群请求实例。
   * 
   * @param applicationHomeSubCluster 应用与主子集群的映射信息
   * @return 封装好的请求实例
   */
  @Private
  @Unstable
  public static UpdateApplicationHomeSubClusterRequest newInstance(
      ApplicationHomeSubCluster applicationHomeSubCluster) {
    UpdateApplicationHomeSubClusterRequest updateApplicationRequest =
        Records.newRecord(UpdateApplicationHomeSubClusterRequest.class);
    updateApplicationRequest
        .setApplicationHomeSubCluster(applicationHomeSubCluster);
    return updateApplicationRequest;
  }

  /**
   * 获取应用与主子集群的映射信息。
   *
   * @return 应用与主子集群的映射
   */
  @Public
  @Unstable
  public abstract ApplicationHomeSubCluster getApplicationHomeSubCluster();

  /**
   * 设置应用与主子集群的映射信息。
   *
   * @param applicationHomeSubCluster 应用与主子集群的映射
   */
  @Private
  @Unstable
  public abstract void setApplicationHomeSubCluster(
      ApplicationHomeSubCluster applicationHomeSubCluster);
}