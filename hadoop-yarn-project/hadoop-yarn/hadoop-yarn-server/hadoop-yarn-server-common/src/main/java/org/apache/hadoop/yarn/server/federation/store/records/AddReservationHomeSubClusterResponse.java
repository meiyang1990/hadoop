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
 * 添加预约归属子集群响应类，封装联邦集群预约存储层对添加预约归属子集群请求的返回结果。
 * YARN联邦集群中，每个预约需要存储其归属的子集群信息，该类封装存储操作的返回结果。
 * 如果预约已存在，返回存储中已有的归属子集群信息，可能与请求中的信息不同。
 */
@Private
@Unstable
public abstract class AddReservationHomeSubClusterResponse {

  /**
   * 创建添加预约归属子集群响应的新实例。
   *
   * @param homeSubCluster 预约归属的子集群ID
   * @return 构造完成的响应实例
   */
  @Private
  @Unstable
  public static AddReservationHomeSubClusterResponse newInstance(
      SubClusterId homeSubCluster) {
    AddReservationHomeSubClusterResponse response =
        Records.newRecord(AddReservationHomeSubClusterResponse.class);
    response.setHomeSubCluster(homeSubCluster);
    return response;
  }

  /**
   * 设置预约分配到的归属子集群ID。
   *
   * @param homeSubCluster 预约所属子集群的ID
   */
  public abstract void setHomeSubCluster(SubClusterId homeSubCluster);

  /**
   * 获取预约分配到的归属子集群ID。
   * 如果请求添加的预约已存在，返回存储中已有的映射，可能与请求中的不同。
   *
   * @return 预约所属子集群的ID
   */
  public abstract SubClusterId getHomeSubCluster();
}