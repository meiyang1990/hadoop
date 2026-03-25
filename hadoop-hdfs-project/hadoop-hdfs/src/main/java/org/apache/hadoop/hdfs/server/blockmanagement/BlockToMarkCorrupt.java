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
package org.apache.hadoop.hdfs.server.blockmanagement;

import static org.apache.hadoop.hdfs.server.blockmanagement.CorruptReplicasMap.Reason;

import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.hdfs.protocol.Block;

/**
 * 数据节点块上报过程中，用于记录需要标记为损坏的块信息，构建待处理损坏块列表
 * 存储损坏块本身信息，以及BlockManager中对应的存储块信息和损坏原因
 */
class BlockToMarkCorrupt {
  /** 数据节点上报的损坏块 */
  private final Block corrupted;
  /** BlockManager中存储的对应块信息 */
  private final BlockInfo stored;
  /** 标记为损坏的文本描述原因 */
  private final String reason;
  /** 标记为损坏的枚举原因码，用于存储 */
  private final CorruptReplicasMap.Reason reasonCode;

  /**
   * 构造需要标记为损坏的块信息对象
   * @param corrupted 数据节点侧损坏块对象
   * @param stored BlockManager中存储的对应块信息
   * @param reason 损坏原因文本描述
   * @param reasonCode 损坏原因枚举码
   */
  BlockToMarkCorrupt(Block corrupted, BlockInfo stored, String reason,
      CorruptReplicasMap.Reason reasonCode) {
    Preconditions.checkNotNull(corrupted, "corrupted is null");
    Preconditions.checkNotNull(stored, "stored is null");

    this.corrupted = corrupted;
    this.stored = stored;
    this.reason = reason;
    this.reasonCode = reasonCode;
  }

  /**
   * 构造需要标记为损坏的块信息对象，指定数据节点侧块的生成 stamps
   * @param corrupted 数据节点侧损坏块对象
   * @param stored BlockManager中存储的对应块信息
   * @param gs 数据节点侧块的生成戳
   * @param reason 损坏原因文本描述
   * @param reasonCode 损坏原因枚举码
   */
  BlockToMarkCorrupt(Block corrupted, BlockInfo stored, long gs, String reason,
      CorruptReplicasMap.Reason reasonCode) {
    this(corrupted, stored, reason, reasonCode);
    // 数据节点上损坏块的生成戳与存储版本不一致，更新到对象中
    this.corrupted.setGenerationStamp(gs);
  }

  /**
   * 判断块是否是写入过程中发生损坏
   * @return true表示存储端生成戳大于损坏块生成戳，写入过程中损坏
   */
  public boolean isCorruptedDuringWrite() {
    return stored.getGenerationStamp() > corrupted.getGenerationStamp();
  }

  /**
   * 获取损坏块对象
   * @return 损坏块对象
   */
  public Block getCorrupted() {
    return corrupted;
  }

  /**
   * 获取BlockManager中存储的对应块信息
   * @return 存储的块信息对象
   */
  public BlockInfo getStored() {
    return stored;
  }

  /**
   * 获取损坏原因文本描述
   * @return 损坏原因文本
   */
  public String getReason() {
    return reason;
  }

  /**
   * 获取损坏原因枚举码
   * @return 损坏原因枚举码
   */
  public Reason getReasonCode() {
    return reasonCode;
  }

  @Override
  public String toString() {
    return corrupted + "("
        + (corrupted == stored ? "same as stored": "stored=" + stored) + ")";
  }
}