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
package org.apache.hadoop.yarn.server.timelineservice.storage.subapplication;

import org.apache.hadoop.yarn.server.timelineservice.storage.common.RowKeyPrefix;

/**
 * 子应用表HBase行键前缀，代表不包含完整entityId（或同时不包含entityType和entityId）的不完整行键
 * 用于子应用表的范围扫描查询
 */
public class SubApplicationRowKeyPrefix extends SubApplicationRowKey
    implements RowKeyPrefix<SubApplicationRowKey> {

  /**
   * 构造子应用表行键前缀，生成的行键前缀格式为：
   * {@code subAppUserId!clusterId!entityType!entityPrefix!userId}
   *
   * @param subAppUserId 子应用用户标识，通常为doAsUser代理用户
   * @param clusterId 集群标识
   * @param entityType 实体类型
   * @param entityIdPrefix 实体ID前缀
   * @param entityId 实体ID
   * @param userId 运行AM的YARN实际用户
   */
  public SubApplicationRowKeyPrefix(String subAppUserId, String clusterId,
      String entityType, Long entityIdPrefix, String entityId,
      String userId) {
    super(subAppUserId, clusterId, entityType, entityIdPrefix, entityId,
        userId);
  }

  /**
   * 获取行键前缀字节数组
   * @return 行键前缀的二进制表示
   */
  public byte[] getRowKeyPrefix() {
    return super.getRowKey();
  }

}