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
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.util.Records;

/**
 * 获取子集群策略配置请求类，用于向联邦策略存储服务查询所有子集群路由策略配置。
 * 该请求在YARN联邦架构中用于从策略存储拉取全量策略配置信息。
 */
@Private
@Unstable
public abstract class GetSubClusterPoliciesConfigurationsRequest {
  /**
   * 创建一个新的获取子集群策略配置请求实例。
   * @return 新的请求实例
   */
  public static GetSubClusterPoliciesConfigurationsRequest newInstance() {
    return Records.newRecord(GetSubClusterPoliciesConfigurationsRequest.class);
  }
}