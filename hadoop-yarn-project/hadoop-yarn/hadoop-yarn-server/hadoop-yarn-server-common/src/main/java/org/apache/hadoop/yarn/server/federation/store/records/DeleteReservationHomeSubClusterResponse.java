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
 * 删除预约归属子集群映射操作的响应类，用于联邦存储层返回删除预约归属子集群映射请求的处理结果。
 * 操作成功时响应为空，失败则通过异常抛出具体失败原因。
 * 对应请求为{@link DeleteReservationHomeSubClusterRequest}，存储操作由{@code FederationReservationHomeSubClusterStore}执行。
 */
@Private
@Unstable
public abstract class DeleteReservationHomeSubClusterResponse {

  /**
   * 创建一个新的删除预约归属子集群响应实例。
   * @return 新建的空响应实例
   */
  @Private
  @Unstable
  public static DeleteReservationHomeSubClusterResponse newInstance() {
    DeleteReservationHomeSubClusterResponse response =
        Records.newRecord(DeleteReservationHomeSubClusterResponse.class);
    return response;
  }
}