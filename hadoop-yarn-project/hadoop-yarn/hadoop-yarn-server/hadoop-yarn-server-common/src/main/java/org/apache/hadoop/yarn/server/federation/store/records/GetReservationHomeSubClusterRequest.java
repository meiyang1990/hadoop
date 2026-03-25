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
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.util.Records;

/**
 * YARN联邦存储层请求类，用于查询指定预留资源所在的归属子集群信息。
 */
@Private
@Unstable
public abstract class GetReservationHomeSubClusterRequest {

  /**
   * 创建新的获取预留资源归属子集群请求实例。
   * @param reservationId 目标预留资源ID
   * @return 封装好的请求实例
   */
  @Private
  @Unstable
  public static GetReservationHomeSubClusterRequest newInstance(
      ReservationId reservationId) {
    GetReservationHomeSubClusterRequest appMapping =
        Records.newRecord(GetReservationHomeSubClusterRequest.class);
    appMapping.setReservationId(reservationId);
    return appMapping;
  }

  /**
   * 获取请求查询的预留资源ID。
   *
   * @return 预留资源唯一标识符
   */
  @Public
  @Unstable
  public abstract ReservationId getReservationId();

  /**
   * 设置请求查询的预留资源ID。
   *
   * @param reservationId 预留资源唯一标识符
   */
  @Private
  @Unstable
  public abstract void setReservationId(ReservationId reservationId);

}