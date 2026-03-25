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

import java.util.Arrays;

import org.apache.hadoop.hdfs.protocol.Block;

/**
 * HDFS数据节点存储块移动尝试完成消息
 * 表示数据节点完成一批块存储移动尝试后，向NameNode上报的结果信息
 * 包含所有已尝试移动的块（无论移动成功还是失败）
 */
public class BlocksStorageMoveAttemptFinished {

  /** 已完成移动尝试的块数组 */
  private final Block[] movementFinishedBlocks;

  /**
   * 构造块移动尝试完成消息
   * @param moveAttemptFinishedBlocks 已完成移动尝试的块数组
   */
  public BlocksStorageMoveAttemptFinished(Block[] moveAttemptFinishedBlocks) {
    this.movementFinishedBlocks = moveAttemptFinishedBlocks;
  }

  /**
   * 获取已完成移动尝试的所有块
   * @return 已完成移动尝试的块数组
   */
  public Block[] getBlocks() {
    return movementFinishedBlocks;
  }

  @Override
  public String toString() {
    return new StringBuilder().append("BlocksStorageMovementFinished(\n  ")
        .append("  blockID: ").append(Arrays.toString(movementFinishedBlocks))
        .append(")").toString();
  }
}