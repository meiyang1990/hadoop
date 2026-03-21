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
 * YARN联邦存储层获取子集群信息查询的响应封装，携带查询到的子集群信息。
 */
@Private
@Unstable
public abstract class GetSubClusterInfoResponse {

  /**
   * 创建新的获取子集群信息响应实例。
   * @param subClusterInfo 子集群信息
   * @return 响应实例
   */
  @Private
  @Unstable
  public static GetSubClusterInfoResponse newInstance(
      SubClusterInfo subClusterInfo) {
    GetSubClusterInfoResponse registerSubClusterRequest =
        Records.newRecord(GetSubClusterInfoResponse.class);
    registerSubClusterRequest.setSubClusterInfo(subClusterInfo);
    return registerSubClusterRequest;
  }

  /**
   * 获取查询到的子集群详细信息。
   *
   * @return 子集群完整信息
   */
  @Public
  @Unstable
  public abstract SubClusterInfo getSubClusterInfo();

  /**
   * 设置查询结果的子集群信息。
   *
   * @param subClusterInfo 子集群完整信息
   */
  @Private
  @Unstable
  public abstract void setSubClusterInfo(SubClusterInfo subClusterInfo);

}