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
package org.apache.hadoop.yarn.server.timelineservice.storage.flow;

import org.apache.hadoop.yarn.server.timelineservice.storage.common.RowKeyPrefix;

/**
 * 流活动HBase行键前缀实现，用于范围扫描查询流活动数据
 */
public class FlowActivityRowKeyPrefix extends FlowActivityRowKey implements
    RowKeyPrefix<FlowActivityRowKey> {

  /**
   * 构造基于集群ID和日期的流活动行键前缀，格式为 clusterId!dayTimestamp!
   * 用于按日期范围扫描特定集群的流活动
   *
   * @param clusterId 集群ID
   * @param dayTs 当日起始时间戳
   */
  public FlowActivityRowKeyPrefix(String clusterId, Long dayTs) {
    super(clusterId, dayTs, null, null, false);
  }

  /**
   * 构造基于集群ID的流活动行键前缀，格式为 clusterId!
   * 用于扫描特定集群下的所有流活动
   *
   * @param clusterId 集群标识
   */
  public FlowActivityRowKeyPrefix(String clusterId) {
    super(clusterId, null, null, null, false);
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