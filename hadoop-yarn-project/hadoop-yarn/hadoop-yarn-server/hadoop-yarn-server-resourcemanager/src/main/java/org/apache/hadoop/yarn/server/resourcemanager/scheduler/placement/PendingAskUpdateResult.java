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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.PendingAsk;

/**
 * 待分配资源请求更新结果，用于位置放置计算后更新应用和队列的指标统计。
 * 该结果对应单个调度键，用于AppSchedulingInfo更新队列和应用的总待分配资源指标。
 * 当指定调度键的待分配请求发生变化时，会记录更新前后的待分配请求和节点分区信息：
 * - lastPendingAsk: 更新前的待分配资源请求
 * - newPendingAsk: 更新后的待分配资源请求
 * - lastNodePartition: 更新前的节点分区
 * - newNodePartition: 更新后的节点分区
 */
public class PendingAskUpdateResult {
  // 更新前的待分配资源请求
  private final PendingAsk lastPendingAsk;
  // 更新前的节点分区
  private final String lastNodePartition;
  // 更新后的待分配资源请求
  private final PendingAsk newPendingAsk;
  // 更新后的节点分区
  private final String newNodePartition;

  /**
   * 构造待分配请求更新结果，保存更新前后的信息。
   * @param lastPendingAsk 更新前的待分配资源请求
   * @param newPendingAsk 更新后的待分配资源请求
   * @param lastNodePartition 更新前的节点分区
   * @param newNodePartition 更新后的节点分区
   */
  public PendingAskUpdateResult(PendingAsk lastPendingAsk,
      PendingAsk newPendingAsk, String lastNodePartition,
      String newNodePartition) {
    this.lastPendingAsk = lastPendingAsk;
    this.newPendingAsk = newPendingAsk;
    this.lastNodePartition = lastNodePartition;
    this.newNodePartition = newNodePartition;
  }

  /** 获取更新前的待分配资源请求 */
  public PendingAsk getLastPendingAsk() {
    return lastPendingAsk;
  }

  /** 获取更新后的待分配资源请求 */
  public PendingAsk getNewPendingAsk() {
    return newPendingAsk;
  }

  /** 获取更新前的节点分区 */
  public String getLastNodePartition() {
    return lastNodePartition;
  }

  /** 获取更新后的节点分区 */
  public String getNewNodePartition() {
    return newNodePartition;
  }

  @Override
  public String toString() {
    return "PendingAskUpdateResult{" + "lastPendingAsk=" + lastPendingAsk
        + ", lastNodePartition='" + lastNodePartition + '\''
        + ", newPendingAsk=" + newPendingAsk + ", newNodePartition='"
        + newNodePartition + '\'' + '}';
  }
}