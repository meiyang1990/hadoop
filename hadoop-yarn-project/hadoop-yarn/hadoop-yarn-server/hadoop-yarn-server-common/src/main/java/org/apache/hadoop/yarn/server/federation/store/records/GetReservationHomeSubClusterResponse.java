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
 * YARN联邦状态存储查询预约归属子集群的响应类
 * <p>
 * 该响应由联邦状态存储返回，用于响应新提交预约查询归属子集群的请求。
 * 包含预约ID与对应归属子集群ID的映射信息。
 * </p>
 */
@Private
@Unstable
public abstract class GetReservationHomeSubClusterResponse {

  /**
   * 创建预约归属子集群查询响应实例。
   * 
   * @param reservationHomeSubCluster 预约-子集群映射信息
   * @return 初始化完成的响应对象
   */
  @Private
  @Unstable
  public static GetReservationHomeSubClusterResponse newInstance(
      ReservationHomeSubCluster reservationHomeSubCluster) {
    GetReservationHomeSubClusterResponse mapResponse =
        Records.newRecord(GetReservationHomeSubClusterResponse.class);
    mapResponse.setReservationHomeSubCluster(reservationHomeSubCluster);
    return mapResponse;
  }

  /**
   * 获取预约到归属子集群的映射信息。
   *
   * @return 预约与归属子集群的映射
   */
  @Public
  @Unstable
  public abstract ReservationHomeSubCluster getReservationHomeSubCluster();

  /**
   * 设置预约到归属子集群的映射信息。
   *
   * @param reservationHomeSubCluster 预约与归属子集群的映射
   */
  @Private
  @Unstable
  public abstract void setReservationHomeSubCluster(
      ReservationHomeSubCluster reservationHomeSubCluster);
}