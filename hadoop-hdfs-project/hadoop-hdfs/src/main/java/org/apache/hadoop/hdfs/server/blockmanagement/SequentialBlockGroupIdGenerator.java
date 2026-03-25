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
import org.apache.hadoop.util.SequentialNumber;

import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BLOCK_GROUP_INDEX_MASK;
import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants.MAX_BLOCKS_IN_GROUP;

/**
 * 纠删码条块组ID顺序生成器，用于生成HDFS纠删码条带化存储的块组ID。
 * 通过顺序递增已分配最大块组ID生成下一个有效ID，保留前2^10个块组ID作为系统预留。
 * 
 * HDFS-EC分层ID编码规则：
 * 普通连续块: {保留ID区 | 类型标记 | 块ID}
 * 条带化块: {保留ID区 | 类型标记 | 块组ID | 组内索引}
 *
 * 保留位之后，第(n+1)位区分块类型：0表示普通连续块，1表示条带化块。
 * 对于条带化块，从(n+2)位到(64-m)位为块组ID，最后m位为组内块索引。
 * m由块组最大块数(MAX_BLOCKS_IN_GROUP)决定。
 * 
 * 注意：nextValue()方法需要外部加锁保证ID分配不冲突。
 */
@InterfaceAudience.Private
public class SequentialBlockGroupIdGenerator extends SequentialNumber {

  private final BlockManager blockManager;

  /**
   * 构造块组ID生成器，绑定当前NameNode的块管理器。
   * @param blockManagerRef 当前块管理器实例
   */
  SequentialBlockGroupIdGenerator(BlockManager blockManagerRef) {
    super(Long.MIN_VALUE);
    this.blockManager = blockManagerRef;
  }

  /**
   * 生成下一个可用的块组起始ID。
   * @return 下一个可用块组起始ID
   */
  @Override // NumberGenerator
  public long nextValue() {
    // 跳转到下一个块组起始位置，跳过当前组内m个索引位
    skipTo((getCurrentValue() & ~BLOCK_GROUP_INDEX_MASK) + MAX_BLOCKS_IN_GROUP);
    // 检查当前范围是否与已存在的随机分配块ID冲突
    final Block b = new Block(getCurrentValue());
    while (hasValidBlockInRange(b)) {
      // 冲突则跳转到下一个块组位置继续检查
      skipTo(getCurrentValue() + MAX_BLOCKS_IN_GROUP);
      b.setBlockId(getCurrentValue());
    }
    // 所有负ID空间耗尽，进入正ID可能和普通块冲突，抛出异常提示
    if (b.getBlockId() >= 0) {
      throw new IllegalStateException("All negative block group IDs are used, "
          + "growing into positive IDs, "
          + "which might conflict with non-erasure coded blocks.");
    }
    return getCurrentValue();
  }

  /**
   * 检查当前块组ID范围内是否存在已存储的块，用于冲突检测。
   * @param b 块对象，其blockId为待检查范围的起始ID
   * @return true如果范围内存在任意已存储块，false表示整个范围可用
   */
  private boolean hasValidBlockInRange(Block b) {
    final long id = b.getBlockId();
    // 遍历块组内所有可能的索引位置
    for (int i = 0; i < MAX_BLOCKS_IN_GROUP; i++) {
      b.setBlockId(id + i);
      // 只要有一个位置已被占用，说明当前块组冲突
      if (blockManager.getStoredBlock(b) != null) {
        return true;
      }
    }
    return false;
  }
}