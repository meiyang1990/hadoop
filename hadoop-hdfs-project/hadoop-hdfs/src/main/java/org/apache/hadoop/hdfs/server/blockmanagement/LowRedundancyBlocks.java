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
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.LongAdder;

import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.util.LightWeightLinkedSet;

/**
 * 低冗余数据块优先级队列管理类。
 * 该类维护了按优先级分层的低冗余块队列，让BlockManager可以优先复制风险最高、最重要的数据块，
 * 从而最大化数据可用性，降低数据丢失风险。优先级从高到低分为5个队列，分别对应不同的风险等级。
 * 支持连续块和EC纠删码块两种类型的优先级计算，提供添加、删除、更新优先级、获取待复制块等操作。
 */
class LowRedundancyBlocks implements Iterable<BlockInfo> {
  /** 优先级队列总数量 */
  static final int LEVEL = 5;
  /** 最高优先级队列编号：风险最高，需要立即复制 */
  static final int QUEUE_HIGHEST_PRIORITY = 0;
  /** 极低冗余队列编号：冗余度远低于预期 */
  static final int QUEUE_VERY_LOW_REDUNDANCY = 1;
  /**
   * 普通低冗余队列编号：冗余度不足但未达到极低标准。
   */
  static final int QUEUE_LOW_REDUNDANCY = 2;
  /** 分布不佳队列编号：副本数量足够但机架分布不合理，存在整机架丢失风险。
   */
  static final int QUEUE_REPLICAS_BADLY_DISTRIBUTED = 3;
  /** 损坏块队列编号：当前无完好可用副本，仅存在损坏副本 */
  static final int QUEUE_WITH_CORRUPT_BLOCKS = 4;
  /** 按优先级存储的队列列表，索引即为优先级 */
  private final List<LightWeightLinkedSet<BlockInfo>> priorityQueues
      = new ArrayList<>(LEVEL);


  /** 低冗余连续块总数统计 */
  private final LongAdder lowRedundancyBlocks = new LongAdder();
  /** 损坏连续块总数统计 */
  private final LongAdder corruptBlocks = new LongAdder();
  /** 副本系数为1的损坏块总数统计 */
  private final LongAdder corruptReplicationOneBlocks = new LongAdder();
  /** 低冗余EC块组总数统计 */
  private final LongAdder lowRedundancyECBlockGroups = new LongAdder();
  /** 损坏EC块组总数统计 */
  private final LongAdder corruptECBlockGroups = new LongAdder();
  /** 分布不佳块总数统计 */
  private final LongAdder badlyDistributedBlocks = new LongAdder();
  /** 最高优先级低冗余连续块总数统计 */
  private final LongAdder highestPriorityLowRedundancyReplicatedBlocks
      = new LongAdder();
  /** 最高优先级低冗余EC块总数统计 */
  private final LongAdder highestPriorityLowRedundancyECBlocks
      = new LongAdder();

  /** 构造函数，初始化所有优先级队列 */
  LowRedundancyBlocks() {
    for (int i = 0; i < LEVEL; i++) {
      priorityQueues.add(new LightWeightLinkedSet<BlockInfo>());
    }
  }

  /**
   * 清空所有队列和统计信息。
   */
  synchronized void clear() {
    for (int i = 0; i < LEVEL; i++) {
      priorityQueues.get(i).clear();
    }
    lowRedundancyBlocks.reset();
    corruptBlocks.reset();
    corruptReplicationOneBlocks.reset();
    lowRedundancyECBlockGroups.reset();
    corruptECBlockGroups.reset();
    highestPriorityLowRedundancyReplicatedBlocks.reset();
    highestPriorityLowRedundancyECBlocks.reset();
  }

  /**
   * 获取所有低冗余块的总数量。
   * @return 所有队列块总数
   */
  synchronized int size() {
    int size = 0;
    for (int i = 0; i < LEVEL; i++) {
      size += priorityQueues.get(i).size();
    }
    return size;
  }

  /**
   * 获取低冗余块数量（排除损坏块队列）。
   * @return 排除损坏块后的低冗余块总数
   */
  synchronized int getLowRedundancyBlockCount() {
    int size = 0;
    for (int i = 0; i < LEVEL; i++) {
      if (i != QUEUE_WITH_CORRUPT_BLOCKS) {
        size += priorityQueues.get(i).size();
      }
    }
    return size;
  }

  /**
   * 获取损坏块数量。
   * @return 损坏块队列中块的数量
   */
  synchronized int getCorruptBlockSize() {
    return priorityQueues.get(QUEUE_WITH_CORRUPT_BLOCKS).size();
  }

  /**
   * 获取副本系数为1的损坏块数量。
   * @return 副本系数为1的损坏块总数
   */
  long getCorruptReplicationOneBlockSize() {
    return getCorruptReplicationOneBlocks();
  }

  /**
   * 获取低冗余连续块数量（减去损坏块）。
   * @return 有效低冗余连续块总数
   */
  long getLowRedundancyBlocks() {
    return lowRedundancyBlocks.longValue() - getCorruptBlocks();
  }

  long getCorruptBlocks() {
    return corruptBlocks.longValue();
  }

  long getCorruptReplicationOneBlocks() {
    return corruptReplicationOneBlocks.longValue();
  }

  /**
   * 获取分布不佳块的总数。
   * @return 分布不佳块总数量
   */
  long getBadlyDistributedBlocks() {
    return badlyDistributedBlocks.longValue();
  }

  /**
   * 获取最高优先级待恢复连续块数量。
   * @return 最高优先级连续块总数
   */
  long getHighestPriorityReplicatedBlockCount() {
    return highestPriorityLowRedundancyReplicatedBlocks.longValue();
  }

  /**
   * 获取最高优先级待恢复EC块数量。
   * @return 最高优先级EC块总数
   */
  long getHighestPriorityECBlockCount() {
    return highestPriorityLowRedundancyECBlocks.longValue();
  }

  /**
   * 获取低冗余EC块组数量（减去损坏EC块组）。
   * @return 有效低冗余EC块组总数
   */
  long getLowRedundancyECBlockGroups() {
    return lowRedundancyECBlockGroups.longValue() -
        getCorruptECBlockGroups();
  }

  long getCorruptECBlockGroups() {
    return corruptECBlockGroups.longValue();
  }

  /**
   * 检查块是否存在于任何低冗余队列中。
   * @param block 待检查的数据块
   * @return true如果块在任一队列中，否则返回false
   */
  synchronized boolean contains(BlockInfo block) {
    for(LightWeightLinkedSet<BlockInfo> set : priorityQueues) {
      if (set.contains(block)) {
        return true;
      }
    }
    return false;
  }

  /**
   * 根据块当前状态计算其优先级。
   * @param block 待计算优先级的数据块
   * @param curReplicas 当前活副本数量
   * @param readOnlyReplicas 只读副本数量
   * @param outOfServiceReplicas 停用副本（退役/维护中节点上）数量
   * @param expectedReplicas 期望副本数量
   * @return 计算得到的优先级，范围0到LEVEL-1
   */
  private int getPriority(BlockInfo block,
                          int curReplicas,
                          int readOnlyReplicas,
                          int outOfServiceReplicas,
                          int expectedReplicas) {
    assert curReplicas >= 0 : "Negative replicas!";
    if (curReplicas >= expectedReplicas) {
      // 副本总数足够，但可能分布不佳
      return QUEUE_REPLICAS_BADLY_DISTRIBUTED;
    }
    if (block.isStriped()) {
      // 处理EC纠删码块
      BlockInfoStriped sblk = (BlockInfoStriped) block;
      return getPriorityStriped(curReplicas, outOfServiceReplicas,
          sblk.getRealDataBlockNum(), sblk.getParityBlockNum());
    } else {
      // 处理普通连续块
      return getPriorityContiguous(curReplicas, readOnlyReplicas,
          outOfServiceReplicas, expectedReplicas);
    }
  }

  /**
   * 计算普通连续块的优先级。
   * @param curReplicas 当前活副本数量
   * @param readOnlyReplicas 只读副本数量
   * @param outOfServiceReplicas 停用副本数量
   * @param expectedReplicas 期望副本数量
   * @return 计算得到的优先级
   */
  private int getPriorityContiguous(int curReplicas, int readOnlyReplicas,
      int outOfServiceReplicas, int expectedReplicas) {
    if (curReplicas == 0) {
      // 没有活副本，但存在停用副本，需要最高优先级恢复
      if (outOfServiceReplicas > 0) {
        return QUEUE_HIGHEST_PRIORITY;
      }
      if (readOnlyReplicas > 0) {
        // 仅存只读副本，存在集体下线风险，最高优先级
        return QUEUE_HIGHEST_PRIORITY;
      }
      // 仅存损坏副本，放入损坏块队列
      return QUEUE_WITH_CORRUPT_BLOCKS;
    } else if (curReplicas == 1) {
      // 仅存一个活副本，丢失风险最高，最高优先级
      return QUEUE_HIGHEST_PRIORITY;
    } else if ((curReplicas * 3) < expectedReplicas) {
      // 活副本不足期望的1/3，判定为极低冗余
      return QUEUE_VERY_LOW_REDUNDANCY;
    } else {
      // 其余低冗余情况，放入普通低冗余队列
      return QUEUE_LOW_REDUNDANCY;
    }
  }

  /**
   * 计算EC纠删码块的优先级。
   * @param curReplicas 当前活副本数量
   * @param outOfServiceReplicas 停用副本数量
   * @param dataBlkNum 数据块数量
   * @param parityBlkNum 校验块数量
   * @return 计算得到的优先级
   */
  private int getPriorityStriped(int curReplicas, int outOfServiceReplicas,
      short dataBlkNum, short parityBlkNum) {
    if (curReplicas < dataBlkNum) {
      // 活数据块不足，但加上停用块仍满足数据块数量，最高优先级恢复
      if (curReplicas + outOfServiceReplicas >= dataBlkNum) {
        return QUEUE_HIGHEST_PRIORITY;
      }
      // 数据块数量不足，已经无法恢复，放入损坏队列
      return QUEUE_WITH_CORRUPT_BLOCKS;
    } else if (curReplicas == dataBlkNum) {
      // 刚好满足数据块数量，无冗余校验块，丢失风险最高，最高优先级
      return QUEUE_HIGHEST_PRIORITY;
    } else if ((curReplicas - dataBlkNum) * 3 < parityBlkNum + 1) {
      // 剩余冗余校验块不足1/3，判定为极低冗余
      return QUEUE_VERY_LOW_REDUNDANCY;
    } else {
      // 其余低冗余情况，放入普通低冗余队列
      return QUEUE_LOW_REDUNDANCY;
    }
  }

  /**
   * 根据块当前状态计算优先级并添加到对应队列。
   *
   * @param block 低冗余数据块
   * @param curReplicas 当前活副本数量
   * @param readOnlyReplicas 只读副本数量
   * @param outOfServiceReplicas 停用副本数量
   * @param expectedReplicas 期望副本数量
   * @return true如果块成功添加到队列，false如果已经存在
   */
  synchronized boolean add(BlockInfo block,
      int curReplicas, int readOnlyReplicas,
      int outOfServiceReplicas, int expectedReplicas) {
    final int priLevel = getPriority(block, curReplicas, readOnlyReplicas,
        outOfServiceReplicas, expectedReplicas);
    if(add(block, priLevel, expectedReplicas)) {
      NameNode.blockStateChangeLog.debug(
          "BLOCK* NameSystem.LowRedundancyBlock.add: {}"
              + " has only {} replicas and need {} replicas so is added to"
              + " neededReconstructions at priority level {}",
          block, curReplicas, expectedReplicas, priLevel);

      return true;
    }
    return false;
  }

  /**
   * 将块添加到指定优先级队列，更新对应统计。
   * @param blockInfo 待添加块
   * @param priLevel 优先级
   * @param expectedReplicas 期望副本数
   * @return true成功添加，false块已存在
   */
  private boolean add(BlockInfo blockInfo, int priLevel, int expectedReplicas) {
    if (priorityQueues.get(priLevel).add(blockInfo)) {
      incrementBlockStat(blockInfo, priLevel, expectedReplicas);
      return true;
    }
    return false;
  }

  /**
   * 根据块类型和优先级增加对应统计计数器。
   * @param blockInfo 数据块
   * @param priLevel 优先级
   * @param expectedReplicas 期望副本数
   */
  private void incrementBlockStat(BlockInfo blockInfo, int priLevel,
      int expectedReplicas) {
    if (blockInfo.isStriped()) {
      lowRedundancyECBlockGroups.increment();
      if (priLevel == QUEUE_WITH_CORRUPT_BLOCKS) {
        corruptECBlockGroups.increment();
      }
      if (priLevel == QUEUE_HIGHEST_PRIORITY) {
        highestPriorityLowRedundancyECBlocks.increment();
      }
      if (priLevel == QUEUE_REPLICAS_BADLY_DISTRIBUTED) {
        badlyDistributedBlocks.increment();
      }
    } else {
      lowRedundancyBlocks.increment();
      if (priLevel == QUEUE_WITH_CORRUPT_BLOCKS) {
        corruptBlocks.increment();
        if (expectedReplicas == 1) {
          corruptReplicationOneBlocks.increment();
        }
      }
      if (priLevel == QUEUE_HIGHEST_PRIORITY) {
        highestPriorityLowRedundancyReplicatedBlocks.increment();
      }
      if (priLevel == QUEUE_REPLICAS_BADLY_DISTRIBUTED) {
        badlyDistributedBlocks.increment();
      }
    }
  }

  /**
   * 根据块旧状态计算优先级并从对应队列移除。
   * @param block 待移除块
   * @param oldReplicas 旧活副本数量
   * @param oldReadOnlyReplicas 旧只读副本数量
   * @param outOfServiceReplicas 旧停用副本数量
   * @param oldExpectedReplicas 旧期望副本数量
   * @return true成功移除，false未找到块
   */
  synchronized boolean remove(BlockInfo block,
      int oldReplicas, int oldReadOnlyReplicas,
      int outOfServiceReplicas, int oldExpectedReplicas) {
    final int priLevel = getPriority(block, oldReplicas, oldReadOnlyReplicas,
        outOfServiceReplicas, oldExpectedReplicas);
    boolean removedBlock = remove(block, priLevel, oldExpectedReplicas);
    if (priLevel == QUEUE_WITH_CORRUPT_BLOCKS &&
        oldExpectedReplicas == 1 &&
        removedBlock) {
      assert corruptReplicationOneBlocks.longValue() >= 0 :
          "Number of corrupt blocks with replication factor 1 " +
              "should be non-negative";
    }
    return removedBlock;
  }

  /**
   * 从低冗余队列移除块，优先从指定优先级队列查找移除。
   * 如果指定优先级队列找不到，则遍历所有队列查找移除。
   * @param block 待移除块
   * @param priLevel 预期优先级，用于