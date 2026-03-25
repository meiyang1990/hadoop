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
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.util.Records;

/**
 * 添加应用归属子集群到联邦状态存储的响应类。
 * 封装FederationApplicationHomeSubClusterStore对添加应用归属子集群请求的返回结果，
 * 如果应用已经存在映射关系，返回存储中已有的归属子集群信息，可能和请求中的不一致。
 */
@Private
@Unstable
public abstract class AddApplicationHomeSubClusterResponse {

  /**
   * 创建添加应用归属子集群响应的新实例。
   * @param homeSubCluster 应用归属子集群ID
   * @return 初始化完成的响应对象
   */
  @Private
  @Unstable
  public static AddApplicationHomeSubClusterResponse newInstance(
      SubClusterId homeSubCluster) {
    AddApplicationHomeSubClusterResponse response =
        Records.newRecord(AddApplicationHomeSubClusterResponse.class);
    response.setHomeSubCluster(homeSubCluster);
    return response;
  }

  /**
   * 设置应用分配的归属子集群ID。
   *
   * @param homeSubCluster 应用归属子集群的{@link SubClusterId}
   */
  public abstract void setHomeSubCluster(SubClusterId homeSubCluster);

  /**
   * 获取应用分配的归属子集群ID。
   * 如果请求的应用已经存在映射关系，返回存储中已有的子集群ID，可能和请求中的不一致。
   *
   * @return 应用归属子集群的{@link SubClusterId}
   */
  public abstract SubClusterId getHomeSubCluster();
}