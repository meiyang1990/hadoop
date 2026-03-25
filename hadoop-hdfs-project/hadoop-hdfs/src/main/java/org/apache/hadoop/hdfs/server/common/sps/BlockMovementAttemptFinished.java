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

package org.apache.hadoop.hdfs.server.common.sps;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;

/**
 * 存储块移动任务完成后的结果信息，包含移动任务的执行状态、源节点、目标节点等核心信息，
 * 用于在存储策略满足性调度（SPS）流程中传递任务执行结果。
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class BlockMovementAttemptFinished {
  private final Block block;
  private final DatanodeInfo src;
  private final DatanodeInfo target;
  private final StorageType targetType;
  private final BlockMovementStatus status;

  /**
   * 构造块移动任务完成结果对象。
   *
   * @param block 待移动的块信息
   * @param src 源数据节点
   * @param target 目标数据节点
   * @param targetType 目标存储类型
   * @param status 移动任务执行状态
   */
  public BlockMovementAttemptFinished(Block block, DatanodeInfo src,
      DatanodeInfo target, StorageType targetType, BlockMovementStatus status) {
    this.block = block;
    this.src = src;
    this.target = target;
    this.targetType = targetType;
    this.status = status;
  }

  /**
   * 获取本次移动的块信息。
   * @return 待移动块对象
   */
  public Block getBlock() {
    return block;
  }

  /**
   * 获取本次移动的目标数据节点。
   * @return 目标数据节点信息
   */
  public DatanodeInfo getTargetDatanode() {
    return target;
  }

  /**
   * 获取本次移动的目标存储类型。
   * @return 目标存储类型
   */
  public StorageType getTargetType() {
    return targetType;
  }

  /**
   * 获取本次块移动任务的执行状态。
   * @return 块移动状态枚举
   */
  public BlockMovementStatus getStatus() {
    return status;
  }

  @Override
  public String toString() {
    return new StringBuilder().append("Block movement attempt finished(\n  ")
        .append(" block : ").append(block).append(" src node: ").append(src)
        .append(" target node: ").append(target).append(" target type: ")
        .append(targetType).append(" movement status: ")
        .append(status).append(")").toString();
  }
}