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
 * 向联邦状态存储请求删除已提交应用的归属子集群映射记录
 */
@Private
@Unstable
public abstract class DeleteApplicationHomeSubClusterRequest {

  /**
   * 创建删除应用归属子集群映射的请求实例
   * @param applicationId 待删除映射的应用ID
   * @return 删除请求实例
   */
  @Private
  @Unstable
  public static DeleteApplicationHomeSubClusterRequest newInstance(
      ApplicationId applicationId) {
    DeleteApplicationHomeSubClusterRequest deleteApplicationRequest =
        Records.newRecord(DeleteApplicationHomeSubClusterRequest.class);
    deleteApplicationRequest.setApplicationId(applicationId);
    return deleteApplicationRequest;
  }

  /**
   * 获取待从联邦状态存储中删除映射的应用ID
   *
   * @return 待删除映射的应用ID
   */
  @Public
  @Unstable
  public abstract ApplicationId getApplicationId();

  /**
   * 设置待从联邦状态存储中删除映射的应用ID
   *
   * @param applicationId 待删除映射的应用ID
   */
  @Private
  @Unstable
  public abstract void setApplicationId(ApplicationId applicationId);
}