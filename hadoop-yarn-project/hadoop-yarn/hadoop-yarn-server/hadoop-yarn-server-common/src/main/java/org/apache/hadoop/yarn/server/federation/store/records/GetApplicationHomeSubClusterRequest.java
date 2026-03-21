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
import org.apache.hadoop.yarn.util.Records;

/**
 * YARN联邦状态存储查询应用所属主子集群的请求类，用于根据指定ApplicationId获取对应主子集群信息。
 */
@Private
@Unstable
public abstract class GetApplicationHomeSubClusterRequest {

  /**
   * 创建不包含应用提交上下文的查询请求实例。
   * @param appId 待查询的应用ID
   * @return 查询请求实例
   */
  @Private
  @Unstable
  public static GetApplicationHomeSubClusterRequest newInstance(
      ApplicationId appId) {
    GetApplicationHomeSubClusterRequest appMapping =
        Records.newRecord(GetApplicationHomeSubClusterRequest.class);
    appMapping.setApplicationId(appId);
    return appMapping;
  }

  /**
   * 创建可指定是否包含应用提交上下文的查询请求实例。
   * @param appId 待查询的应用ID
   * @param containsAppSubmissionContext 是否需要返回应用提交上下文
   * @return 查询请求实例
   */
  @Private
  @Unstable
  public static GetApplicationHomeSubClusterRequest newInstance(
      ApplicationId appId, boolean containsAppSubmissionContext) {
    GetApplicationHomeSubClusterRequest appMapping =
         Records.newRecord(GetApplicationHomeSubClusterRequest.class);
    appMapping.setApplicationId(appId);
    appMapping.setContainsAppSubmissionContext(containsAppSubmissionContext);
    return appMapping;
  }

  /**
   * 获取待查询的应用唯一标识。
   *
   * @return 应用ID
   */
  @Public
  @Unstable
  public abstract ApplicationId getApplicationId();

  /**
   * 设置待查询的应用唯一标识。
   *
   * @param applicationId 应用ID
   */
  @Private
  @Unstable
  public abstract void setApplicationId(ApplicationId applicationId);


  /**
   * 获取是否需要返回应用提交上下文的标志。
   * 添加该标志是因为应用提交上下文不常用且数据体积较大，可按需获取减少传输量。
   *
   * @return 是否需要返回应用提交上下文
   */
  @Public
  @Unstable
  public abstract boolean getContainsAppSubmissionContext();

  /**
   * 设置是否需要返回应用提交上下文的标志。
   *
   * @param containsAppSubmissionContext 是否需要返回应用提交上下文
   */
  @Public
  @Unstable
  public abstract void setContainsAppSubmissionContext(
      boolean containsAppSubmissionContext);
}