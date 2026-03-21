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
 * 请求类，Router向联邦状态存储写入新提交应用的归属子集群映射关系时使用。
 *
 * <p>
 * 请求包含的映射信息：
 * <ul>
 * <li>{@code ApplicationId} 应用编号</li>
 * <li>{@code SubClusterId} 归属子集群编号</li>
 * </ul>
 */
@Private
@Unstable
public abstract class AddApplicationHomeSubClusterRequest {

  /**
   * 创建添加应用归属子集群映射的请求实例。
   * @param applicationHomeSubCluster 应用归属子集群映射信息
   * @return 构建完成的请求对象
   */
  @Private
  @Unstable
  public static AddApplicationHomeSubClusterRequest newInstance(
      ApplicationHomeSubCluster applicationHomeSubCluster) {
    AddApplicationHomeSubClusterRequest mapRequest =
        Records.newRecord(AddApplicationHomeSubClusterRequest.class);
    mapRequest.setApplicationHomeSubCluster(applicationHomeSubCluster);
    return mapRequest;
  }

  /**
   * 获取应用归属子集群映射信息。
   *
   * @return 应用与归属子集群的映射关系
   */
  @Public
  @Unstable
  public abstract ApplicationHomeSubCluster getApplicationHomeSubCluster();

  /**
   * 设置应用归属子集群映射信息。
   *
   * @param applicationHomeSubCluster 应用与归属子集群的映射关系
   */
  @Private
  @Unstable
  public abstract void setApplicationHomeSubCluster(
      ApplicationHomeSubCluster applicationHomeSubCluster);
}