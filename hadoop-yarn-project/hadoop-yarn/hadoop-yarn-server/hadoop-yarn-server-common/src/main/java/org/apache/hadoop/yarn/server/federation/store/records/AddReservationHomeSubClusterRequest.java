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
 * 路由器向联邦状态存储发送的请求，用于为新提交的预留建立家子集群映射关系。
 *
 * <p>
 * 请求包含的映射信息:
 * <ul>
 * <li>{@code 预留ID}</li>
 * <li>{@code 子集群ID}</li>
 * </ul>
 */
@Private
@Unstable
public abstract class AddReservationHomeSubClusterRequest {

  /**
   * 创建新增预留家子集群映射请求实例。
   * @param reservationHomeSubCluster 预留与家子集群的映射信息
   * @return 新增预留家子集群映射请求实例
   */
  @Private
  @Unstable
  public static AddReservationHomeSubClusterRequest newInstance(
      ReservationHomeSubCluster reservationHomeSubCluster) {
    AddReservationHomeSubClusterRequest mapRequest =
        Records.newRecord(AddReservationHomeSubClusterRequest.class);
    mapRequest.setReservationHomeSubCluster(reservationHomeSubCluster);
    return mapRequest;
  }

  /**
   * 获取预留与家子集群的映射信息。
   *
   * @return 预留与家子集群的映射信息
   */
  @Public
  @Unstable
  public abstract ReservationHomeSubCluster getReservationHomeSubCluster();

  /**
   * 设置预留与家子集群的映射信息。
   *
   * @param reservationHomeSubCluster 预留与家子集群的映射信息
   */
  @Private
  @Unstable
  public abstract void setReservationHomeSubCluster(
      ReservationHomeSubCluster reservationHomeSubCluster);
}