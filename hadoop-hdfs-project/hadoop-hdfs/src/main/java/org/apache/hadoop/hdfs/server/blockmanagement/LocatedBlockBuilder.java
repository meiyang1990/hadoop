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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * LocatedBlocks构建器，用于流式构造文件块位置信息列表
 * 支持Builder模式，通过链式调用逐步设置各个属性，最后构建出完整的LocatedBlocks对象
 * 用于HDFS NameNode向客户端返回文件块位置信息的场景
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
class LocatedBlockBuilder {

  protected long flen;
  protected List<LocatedBlock> blocks = Collections.<LocatedBlock>emptyList();
  protected boolean isUC;
  protected LocatedBlock last;
  protected boolean lastComplete;
  protected FileEncryptionInfo feInfo;
  private final int maxBlocks;
  protected ErasureCodingPolicy ecPolicy;

  /**
   * 构造函数，初始化构建器，设置最大可容纳块数量
   * @param maxBlocks 允许添加的最大块数量
   */
  LocatedBlockBuilder(int maxBlocks) {
    this.maxBlocks = maxBlocks;
  }

  /**
   * 检查是否已达到允许添加的最大块数量
   * @return true 表示已达到或超过最大块数量，false表示还可继续添加
   */
  boolean isBlockMax() {
    return blocks.size() >= maxBlocks;
  }

  /**
   * 设置文件总长度
   * @param fileLength 文件总长度（字节）
   * @return 当前构建器实例
   */
  LocatedBlockBuilder fileLength(long fileLength) {
    flen = fileLength;
    return this;
  }

  /**
   * 添加一个已构造好的块位置信息到列表
   * @param block 块位置信息对象
   * @return 当前构建器实例
   */
  LocatedBlockBuilder addBlock(LocatedBlock block) {
    if (blocks.isEmpty()) {
      blocks = new ArrayList<>();
    }
    blocks.add(block);
    return this;
  }

  /**
   * 创建新的LocatedBlock实例，方便后续设置访问令牌
   * @param eb 扩展块信息，包含块ID和存储池ID
   * @param storage 存储该块的数据节点存储信息数组
   * @param pos 块在文件中的起始偏移量
   * @param isCorrupt 块是否损坏
   * @return 新构造的LocatedBlock实例
   */
  // return new block so tokens can be set
  LocatedBlock newLocatedBlock(ExtendedBlock eb,
      DatanodeStorageInfo[] storage,
      long pos, boolean isCorrupt) {
    LocatedBlock blk =
        BlockManager.newLocatedBlock(eb, storage, pos, isCorrupt);
    return blk;
  }

  /**
   * 设置文件是否处于构建中（未完成写入）状态
   * @param underConstruction true表示文件未构建完成，false表示已完成
   * @return 当前构建器实例
   */
  LocatedBlockBuilder lastUC(boolean underConstruction) {
    isUC = underConstruction;
    return this;
  }

  /**
   * 设置文件的最后一个块信息
   * @param block 最后一个块的位置信息
   * @return 当前构建器实例
   */
  LocatedBlockBuilder lastBlock(LocatedBlock block) {
    last = block;
    return this;
  }

  /**
   * 设置最后一个块是否已完成写入
   * @param complete true表示最后一块已完成，false表示未完成
   * @return 当前构建器实例
   */
  LocatedBlockBuilder lastComplete(boolean complete) {
    lastComplete = complete;
    return this;
  }

  /**
   * 设置文件加密信息
   * @param fileEncryptionInfo 文件加密信息对象，为空表示未加密
   * @return 当前构建器实例
   */
  LocatedBlockBuilder encryption(FileEncryptionInfo fileEncryptionInfo) {
    feInfo = fileEncryptionInfo;
    return this;
  }

  /**
   * 设置文件使用的纠删码策略
   * @param codingPolicy 纠删码策略对象
   * @return 当前构建器实例
   */
  LocatedBlockBuilder erasureCoding(ErasureCodingPolicy codingPolicy) {
    ecPolicy = codingPolicy;
    return this;
  }

  /**
   * 构建LocatedBlocks对象，兼容传入客户端节点的调用形式
   * @param client 客户端所在数据节点描述符（用于网络拓扑位置选择）
   * @return 构造完成的LocatedBlocks对象
   */
  LocatedBlocks build(DatanodeDescriptor client) {
    return build();
  }

  /**
   * 根据已设置的所有属性构造完整的LocatedBlocks对象
   * @return 构造完成的LocatedBlocks对象，包含文件所有块位置信息
   */
  LocatedBlocks build() {
    return new LocatedBlocks(flen, isUC, blocks, last,
        lastComplete, feInfo, ecPolicy);
  }

}