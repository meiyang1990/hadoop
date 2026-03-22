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
package org.apache.hadoop.hdfs.server.protocol;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * 日志隔离请求的响应封装类，用于HDFS高可用场景下JournalNode对NameNode隔离请求的回复
 * 对应 {@link JournalProtocol#fence} 方法的返回结果
 */
@InterfaceAudience.Private
public class FenceResponse {
  private final long previousEpoch;
  private final long lastTransactionId;
  private final boolean isInSync;
  
  /**
   * 构造日志隔离响应对象
   * @param previousEpoch 隔离前的世代编号
   * @param lastTransId 最后一条已写入事务ID
   * @param inSync 是否与最新状态同步
   */
  public FenceResponse(long previousEpoch, long lastTransId, boolean inSync) {
    this.previousEpoch = previousEpoch;
    this.lastTransactionId = lastTransId;
    this.isInSync = inSync;
  }

  /**
   * 获取JournalNode是否与最新事务状态同步
   * @return 是否同步
   */
  public boolean isInSync() {
    return isInSync;
  }

  /**
   * 获取JournalNode上最后一条已写入事务的ID
   * @return 最后事务ID
   */
  public long getLastTransactionId() {
    return lastTransactionId;
  }

  /**
   * 获取隔离前的上一个世代编号
   * @return 上一代世代编号
   */
  public long getPreviousEpoch() {
    return previousEpoch;
  }
}