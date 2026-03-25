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
 * 联邦状态存储删除预约归属子集群映射的请求类，用于从联邦状态存储中删除已提交预约的归属子集群映射关系。
 */
@Private
@Unstable
public abstract class DeleteReservationHomeSubClusterRequest {

  /**
   * 创建删除预约归属子集群映射请求实例。
   * @param reservationId 待删除映射关系的预约ID
   * @return 配置完成的删除请求实例
   */
  @Private
  @Unstable
  public static DeleteReservationHomeSubClusterRequest newInstance(
      ReservationId reservationId) {
    DeleteReservationHomeSubClusterRequest deleteReservationRequest =
        Records.newRecord(DeleteReservationHomeSubClusterRequest.class);
    deleteReservationRequest.setReservationId(reservationId);
    return deleteReservationRequest;
  }

  /**
   * 获取待删除映射关系的预约ID。
   *
   * @return 待从联邦状态存储中删除映射关系的预约ID
   */
  @Public
  @Unstable
  public abstract ReservationId getReservationId();

  /**
   * 设置待删除映射关系的预约ID。
   *
   * @param reservationId 待从联邦状态存储中删除映射关系的预约ID
   */
  @Private
  @Unstable
  public abstract void setReservationId(ReservationId reservationId);
}