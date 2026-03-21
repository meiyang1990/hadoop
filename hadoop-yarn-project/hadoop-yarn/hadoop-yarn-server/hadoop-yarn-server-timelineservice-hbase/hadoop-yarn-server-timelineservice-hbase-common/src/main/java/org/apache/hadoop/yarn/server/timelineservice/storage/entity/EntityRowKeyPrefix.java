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
package org.apache.hadoop.yarn.server.timelineservice.storage.entity;

import org.apache.hadoop.yarn.server.timelineservice.storage.common.RowKeyPrefix;

/**
 * 实体表HBase行键前缀，代表不包含entityId或同时不包含entityType和entityId的不完整行键
 * 用于HBase前缀扫描，实现根据前缀范围查询实体数据
 */
public class EntityRowKeyPrefix extends EntityRowKey implements
    RowKeyPrefix<EntityRowKey> {

  /**
   * 构造包含到实体类型为止/带实体ID前缀的行键前缀，格式为：userName!clusterId!flowName!flowRunId!AppId!entityType!
   * @param clusterId 集群标识
   * @param userId 用户标识
   * @param flowName 流名称
   * @param flowRunId 流运行实例ID
   * @param appId 应用标识
   * @param entityType 实体类型
   * @param entityIdPrefix 实体ID前缀
   * @param entityId 完整实体ID
   */
  public EntityRowKeyPrefix(String clusterId, String userId, String flowName,
      Long flowRunId, String appId, String entityType, Long entityIdPrefix,
      String entityId) {
    super(clusterId, userId, flowName, flowRunId, appId, entityType,
        entityIdPrefix, entityId);
  }

  /**
   * 构造包含到应用为止的行键前缀，格式为：userName!clusterId!flowName!flowRunId!AppId!
   * @param clusterId 集群标识
   * @param userId 用户标识
   * @param flowName 流名称
   * @param flowRunId 流运行实例ID
   * @param appId 应用标识
   */
  public EntityRowKeyPrefix(String clusterId, String userId, String flowName,
      Long flowRunId, String appId) {
    this(clusterId, userId, flowName, flowRunId, appId, null, null, null);
  }

  /**
   * 获取序列化后的行键前缀字节数组
   * @return 行键前缀的字节表示
   */
  public byte[] getRowKeyPrefix() {
    return super.getRowKey();
  }

}