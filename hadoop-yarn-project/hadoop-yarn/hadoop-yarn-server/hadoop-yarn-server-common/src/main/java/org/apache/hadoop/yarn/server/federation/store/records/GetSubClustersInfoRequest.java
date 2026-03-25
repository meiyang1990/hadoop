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

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.util.Records;

/**
 * 获取联邦中所有参与子集群信息的请求类。
 *
 * 如果filterInactiveSubClusters设为true，仅返回活跃子集群；否则返回所有子集群，忽略其状态。
 * 默认情况下filterInactiveSubClusters为true。
 */
@Private
@Unstable
public abstract class GetSubClustersInfoRequest {

  /**
   * 创建新的获取子集群信息请求实例。
   * @param filterInactiveSubClusters 是否过滤不活跃子集群
   * @return 新建的请求实例
   */
  @Public
  @Unstable
  public static GetSubClustersInfoRequest newInstance(
      boolean filterInactiveSubClusters) {
    GetSubClustersInfoRequest request =
        Records.newRecord(GetSubClustersInfoRequest.class);
    request.setFilterInactiveSubClusters(filterInactiveSubClusters);
    return request;
  }

  /**
   * 获取是否仅返回活跃子集群的过滤标记。
   *
   * @return 是否过滤不活跃子集群
   */
  @Public
  @Unstable
  public abstract boolean getFilterInactiveSubClusters();

  /**
   * 设置是否仅返回活跃子集群的过滤标记。
   *
   * @param filterInactiveSubClusters 是否过滤不活跃子集群
   */
  @Public
  @Unstable
  public abstract void setFilterInactiveSubClusters(
      boolean filterInactiveSubClusters);

}