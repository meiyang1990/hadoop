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
 * 流运行记录表的前缀行键（不包含流运行ID），用于HBase范围查询
 * 表示不完整的流运行行键，仅包含前缀部分，用于根据集群、用户、流名称进行范围扫描
 */
public class FlowRunRowKeyPrefix extends FlowRunRowKey implements
    RowKeyPrefix<FlowRunRowKey> {

  /**
   * 构造流运行表前缀行键，格式为：{@code clusterId!userId!flowName!}
   *
   * @param clusterId 集群标识
   * @param userId 用户标识
   * @param flowName 流名称
   */
  public FlowRunRowKeyPrefix(String clusterId, String userId,
      String flowName) {
    super(clusterId, userId, flowName, null);
  }

  /**
   * 获取序列化后的前缀行键字节数组
   * @return 前缀行键字节数组
   */
  public byte[] getRowKeyPrefix() {
    // 由于当前实例流运行ID为null，父类生成的行键正好就是前缀，直接委托父类实现即可
    return super.getRowKey();
  }

}