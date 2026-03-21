// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.federation.store.records;

import java.util.List;
import java.util.Collection;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.util.Records;

/**
 * YARN联邦存储层查询子集群信息的响应封装，包含所有当前加入联邦的子集群信息列表。
 */
@Private
@Unstable
public abstract class GetSubClustersInfoResponse {

  /**
   * 创建新的GetSubClustersInfoResponse实例，设置子集群信息列表。
   * @param subClusters 子集群信息集合
   * @return 初始化完成的响应对象
   */
  @Public
  @Unstable
  public static GetSubClustersInfoResponse newInstance(
      Collection<SubClusterInfo> subClusters) {
    GetSubClustersInfoResponse subClusterInfos =
        Records.newRecord(GetSubClustersInfoResponse.class);
    subClusterInfos.setSubClusters(subClusters);
    return subClusterInfos;
  }

  /**
   * 获取所有当前加入联邦的子集群信息列表。
   *
   * @return 子集群信息列表
   */
  @Public
  @Unstable
  public abstract List<SubClusterInfo> getSubClusters();

  /**
   * 设置所有当前加入联邦的子集群信息列表。
   *
   * @param subClusters 子集群信息集合
   */
  @Private
  @Unstable
  public abstract void setSubClusters(Collection<SubClusterInfo> subClusters);
}