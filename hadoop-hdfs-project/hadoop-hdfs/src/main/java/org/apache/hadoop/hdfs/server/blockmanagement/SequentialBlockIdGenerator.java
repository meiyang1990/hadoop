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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.namenode.INodeId;
import org.apache.hadoop.util.SequentialNumber;

/**
 * 顺序式块ID生成器，通过递增当前已分配的最大块ID生成下一个有效块ID，起始值为2^30+1。
 * 历史上HDFS块ID是随机生成的，因此顺序遍历ID空间时可能会遇到冲突。由于ID空间非常稀疏，冲突概率很低，遇到冲突时直接跳过即可。
 * 本类是HDFS NameNode块管理模块的核心组件，负责为新创建的数据块生成全局唯一ID。
 */
@InterfaceAudience.Private
public class SequentialBlockIdGenerator extends SequentialNumber {
  /**
   * 最后一个预留块ID，小于等于该值的ID为系统预留，不用于普通数据块
   */
  public static final long LAST_RESERVED_BLOCK_ID = 1024L * 1024 * 1024;

  private final BlockManager blockManager;

  /**
   * 构造顺序式块ID生成器，从最后一个预留ID之后开始生成新ID
   * @param blockManagerRef 块管理器引用，用于查询块是否已存在
   */
  SequentialBlockIdGenerator(BlockManager blockManagerRef) {
    super(LAST_RESERVED_BLOCK_ID);
    this.blockManager = blockManagerRef;
  }

  /**
   * 生成下一个不冲突的有效块ID
   * @return 新生成的唯一有效块ID
   * @throws IllegalStateException 当所有正块ID耗尽时抛出异常，避免与纠删码块组ID冲突
   */
  @Override // NumberGenerator
  public long nextValue() {
    // 生成下一个顺序ID并构造块对象
    Block b = new Block(super.nextValue());

    // 若该ID已被历史随机生成的块占用，则跳过冲突，继续生成下一个ID
    while(isValidBlock(b)) {
      b.setBlockId(super.nextValue());
    }
    if (b.getBlockId() < 0) {
      throw new IllegalStateException("All positive block IDs are used, " +
          "wrapping to negative IDs, " +
          "which might conflict with erasure coded block groups.");
    }
    return b.getBlockId();
  }

  /**
   * 检查给定块ID是否已经被现有文件使用
   * @param b 待检查的块对象
   * @return true 如果块ID已被使用，false 如果块ID可用
   */
  private boolean isValidBlock(Block b) {
    // 从块管理器获取存储的块信息
    BlockInfo bi = blockManager.getStoredBlock(b);
    // 块存在且关联了有效的文件集合，说明该ID已被使用
    return bi != null && bi.getBlockCollectionId() !=
        INodeId.INVALID_INODE_ID;
  }
}