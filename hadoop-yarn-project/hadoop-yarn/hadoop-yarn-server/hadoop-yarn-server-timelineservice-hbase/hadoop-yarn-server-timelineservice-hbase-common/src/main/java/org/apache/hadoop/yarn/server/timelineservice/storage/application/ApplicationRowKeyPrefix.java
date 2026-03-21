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
package org.apache.hadoop.yarn.server.timelineservice.storage.application;

import org.apache.hadoop.yarn.server.timelineservice.storage.common.RowKeyPrefix;

/**
 * 应用表HBase行键前缀实现，用于应用范围扫描，表示不完整的行键（缺少应用ID部分）
 * 可基于集群、用户、流名称或流运行ID构造扫描前缀，查询对应范围的应用
 */
public class ApplicationRowKeyPrefix extends ApplicationRowKey implements
    RowKeyPrefix<ApplicationRowKey> {

  /**
   * 构造应用表行键前缀，格式为 clusterId!userName!flowName!，用于查询指定流下的所有应用
   *
   * @param clusterId 应用运行所在集群ID
   * @param userId 提交应用的用户ID
   * @param flowName 用户集群上运行的流名称
   */
  public ApplicationRowKeyPrefix(String clusterId, String userId,
      String flowName) {
    super(clusterId, userId, flowName, null, null);
  }

  /**
   * 构造应用表行键前缀，格式为 clusterId!userName!flowName!flowRunId!，用于查询指定流运行实例下的所有应用
   *
   * @param clusterId 集群标识
   * @param userId 用户标识
   * @param flowName 流标识
   * @param flowRunId 流运行实例标识
   */
  public ApplicationRowKeyPrefix(String clusterId, String userId,
      String flowName, Long flowRunId) {
    super(clusterId, userId, flowName, flowRunId, null);
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.hadoop.yarn.server.timelineservice.storage.application.
   * RowKeyPrefix#getRowKeyPrefix()
   */
  @Override
  public byte[] getRowKeyPrefix() {
    return super.getRowKey();
  }

}