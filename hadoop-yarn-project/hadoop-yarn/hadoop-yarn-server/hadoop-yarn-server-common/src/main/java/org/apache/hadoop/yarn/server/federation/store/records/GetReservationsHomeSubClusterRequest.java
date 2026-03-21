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
 * 获取所有活跃预约对应的归属子集群映射关系的请求类，用于YARN联邦存储层查询。
 */
@Private
@Unstable
public abstract class GetReservationsHomeSubClusterRequest {

  /**
   * 创建一个新的GetReservationsHomeSubClusterRequest实例。
   * @return 创建好的请求实例
   */
  @Private
  @Unstable
  public static GetReservationsHomeSubClusterRequest newInstance() {
    GetReservationsHomeSubClusterRequest request =
        Records.newRecord(GetReservationsHomeSubClusterRequest.class);
    return request;
  }
}