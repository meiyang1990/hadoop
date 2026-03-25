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

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockType;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.GenerationStamp;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.FSEditLog;

import java.io.IOException;

import static org.apache.hadoop.hdfs.protocol.BlockType.STRIPED;

/**
 * 文件: org.apache.hadoop.hdfs.server.blockmanagement.BlockIdManager.java
 * 所属模块: HDFS 服务端核心模块
 * 核心职责: 统一管理HDFS集群中块ID和生成戳(Generation Stamp)的分配，区分传统随机块ID和新式顺序块ID，支持纠删码条带化块，
 *          同时为HA架构下的Standby NameNode提供生成戳同步能力，保证故障转移后不会重用生成戳。
 * 
 * {@link FSNamesystem}负责将分配结果持久化到{@link FSEditLog}中。
 */
public class BlockIdManager {
  /**
   * The global generation stamp for legacy blocks with randomly
   * generated block IDs.
   */
  private final GenerationStamp legacyGenerationStamp = new GenerationStamp();
  /**
   * The global generation stamp for this file system.
   */
  private final GenerationStamp generationStamp = new GenerationStamp();
  /**
   * Most recent global generation stamp as seen on Active NameNode.
   * Used by StandbyNode only.<p/>
   * StandbyNode does not update its global {@link #generationStamp} during
   * edits tailing. The global generation stamp on StandbyNode is updated
   * <ol><li>when the block with the next generation stamp is actually
   * received</li>
   * <li>during fail-over it is bumped to the last value received from the
   * Active NN through edits and stored as
   * {@link #impendingGenerationStamp}</li></ol>
   * The former helps to avoid a race condition with IBRs during edits tailing.
   * The latter guarantees that generation stamps are never reused by new
   * Active after fail-over.
   * <p/> See HDFS-14941 for more details.
   */
  private final GenerationStamp impendingGenerationStamp
      = new GenerationStamp();
  /**
   * The value of the generation stamp when the first switch to sequential
   * block IDs was made. Blocks with generation stamps below this value
   * have randomly allocated block IDs. Blocks with generation stamps above
   * this value had sequentially allocated block IDs. Read from the fsImage
   * (or initialized as an offset from the V1 (legacy) generation stamp on
   * upgrade).
   */
  private long legacyGenerationStampLimit;
  /**
   * The global block ID space for this file system.
   */
  private final SequentialBlockIdGenerator blockIdGenerator;
  private final SequentialBlockGroupIdGenerator blockGroupIdGenerator;

  /**
   * 构造方法，初始化BlockIdManager
   * @param blockManager 块管理器实例
   */
  public BlockIdManager(BlockManager blockManager) {
    this.legacyGenerationStampLimit =
        HdfsConstants.GRANDFATHER_GENERATION_STAMP;
    this.blockIdGenerator = new SequentialBlockIdGenerator(blockManager);
    this.blockGroupIdGenerator = new SequentialBlockGroupIdGenerator(blockManager);
  }

  /**
   * 升级传统生成戳，为现有块预留足够范围，仅在首次升级到顺序块ID时调用
   * @return 升级后的生成戳当前值
   */
  public long upgradeLegacyGenerationStamp() {
    Preconditions.checkState(generationStamp.getCurrentValue() ==
      GenerationStamp.LAST_RESERVED_STAMP);
    generationStamp.skipTo(legacyGenerationStamp.getCurrentValue() +
      HdfsServerConstants.RESERVED_LEGACY_GENERATION_STAMPS);

    legacyGenerationStampLimit = generationStamp.getCurrentValue();
    return generationStamp.getCurrentValue();
  }

  /**
   * 设置区分随机分配和顺序分配块ID的生成戳边界
   *
   * @param stamp 边界值
   */
  public void setLegacyGenerationStampLimit(long stamp) {
    Preconditions.checkState(legacyGenerationStampLimit ==
        HdfsConstants.GRANDFATHER_GENERATION_STAMP);
    legacyGenerationStampLimit = stamp;
  }

  /**
   * 获取随机分配和顺序分配块ID分界处的生成戳值
   * @return 分界生成戳值
   */
  public long getGenerationStampAtblockIdSwitch() {
    return legacyGenerationStampLimit;
  }

  @VisibleForTesting
  SequentialBlockIdGenerator getBlockIdGenerator() {
    return blockIdGenerator;
  }

  /**
   * 设置文件系统已分配的最大连续块ID，作为分配新块ID的基础
   * @param blockId 最大已分配连续块ID
   */
  public void setLastAllocatedContiguousBlockId(long blockId) {
    blockIdGenerator.skipTo(blockId);
  }

  /**
   * 获取文件系统已顺序分配的最大连续块ID
   * @return 最大连续块ID
   */
  public long getLastAllocatedContiguousBlockId() {
    return blockIdGenerator.getCurrentValue();
  }

  /**
   * 设置文件系统已分配的最大条带化块ID，作为分配新块ID的基础
   * @param blockId 最大已分配条带化块ID
   */
  public void setLastAllocatedStripedBlockId(long blockId) {
    blockGroupIdGenerator.skipTo(blockId);
  }

  /**
   * 获取文件系统已顺序分配的最大条带化块ID
   * @return 最大条带化块ID
   */
  public long getLastAllocatedStripedBlockId() {
    return blockGroupIdGenerator.getCurrentValue();
  }

  /**
   * 设置传统块的当前生成戳
   * @param stamp 生成戳值
   */
  public void setLegacyGenerationStamp(long stamp) {
    legacyGenerationStamp.setCurrentValue(stamp);
  }

  /**
   * 获取传统块的当前生成戳
   * @return 当前生成戳值
   */
  public long getLegacyGenerationStamp() {
    return legacyGenerationStamp.getCurrentValue();
  }

  /**
   * 设置文件系统的当前生成戳
   * @param stamp 生成戳值
   */
  public void setGenerationStamp(long stamp) {
    generationStamp.setCurrentValue(stamp);
  }

  /**
   * 设置从Active NameNode获取的最新生成戳，仅Standby NameNode使用
   * @param stamp 新的待应用生成戳
   */
  public void setImpendingGenerationStamp(long stamp) {
    impendingGenerationStamp.setIfGreater(stamp);
  }

  /**
   * 将待应用生成戳更新为当前全局生成戳，用于Standby切换为Active时
   */
  public void applyImpendingGenerationStamp() {
    setGenerationStampIfGreater(impendingGenerationStamp.getCurrentValue());
  }

  @VisibleForTesting
  public long getImpendingGenerationStamp() {
    return impendingGenerationStamp.getCurrentValue();
  }

  /**
   * 仅当传入值大于当前值时更新生成戳
   * @param stamp 待更新生成戳值
   */
  public void setGenerationStampIfGreater(long stamp) {
    generationStamp.setIfGreater(stamp);
  }

  /**
   * 获取当前全局生成戳
   * @return 当前生成戳值
   */
  public long getGenerationStamp() {
    return generationStamp.getCurrentValue();
  }

  /**
   * 获取下一个生成戳，根据是否为传统块选择不同分配逻辑
   * @param legacyBlock 是否为传统块
   * @return 下一个生成戳
   * @throws IOException 传统生成戳耗尽时抛出异常
   */
  long nextGenerationStamp(boolean legacyBlock) throws IOException {
    return legacyBlock ? getNextLegacyGenerationStamp() :
        getNextGenerationStamp();
  }

  @VisibleForTesting
  long getNextLegacyGenerationStamp() throws IOException {
    long legacyGenStamp = legacyGenerationStamp.nextValue();

    if (legacyGenStamp >= legacyGenerationStampLimit) {
      // 传统块生成戳耗尽，实际生产中几乎不会发生，因为预留了足够大的范围
      // 耗尽后将无法追加升级前创建的传统块
      throw new OutOfLegacyGenerationStampsException();
    }

    return legacyGenStamp;
  }

  @VisibleForTesting
  long getNextGenerationStamp() {
    return generationStamp.nextValue();
  }

  /**
   * 获取传统块生成戳边界值
   * @return 边界值
   */
  public long getLegacyGenerationStampLimit() {
    return legacyGenerationStampLimit;
  }

  /**
   * 判断块是否为传统随机生成ID的块，基于生成戳边界判断
   *
   * @param block 待判断块
   * @return true为传统随机块，false为新式顺序块
   */
  boolean isLegacyBlock(Block block) {
    return block.getGenerationStamp() < getLegacyGenerationStampLimit();
  }

  /**
   * 获取下一个块ID，根据块类型分配不同ID
   * @param blockType 块类型
   * @return 下一个块ID
   */
  long nextBlockId(BlockType blockType) {
    switch(blockType) {
    case CONTIGUOUS: return blockIdGenerator.nextValue();
    case STRIPED: return blockGroupIdGenerator.nextValue();
    default:
      throw new IllegalArgumentException(
          "nextBlockId called with an unsupported BlockType");
    }
  }

  /**
   * 判断块的生成戳是否大于当前分配的最大生成戳
   * @param block 待检查块
   * @return true表示生成戳来自未来，false表示正常
   */
  boolean isGenStampInFuture(Block block) {
    if (isLegacyBlock(block)) {
      return block.getGenerationStamp() > getLegacyGenerationStamp();
    } else {
      return block.getGenerationStamp() > getGenerationStamp();
    }
  }

  /**
   * 重置所有生成戳和块ID分配器，清空状态到初始值
   */
  void clear() {
    legacyGenerationStamp.setCurrentValue(GenerationStamp.LAST_RESERVED_STAMP);
    generationStamp.setCurrentValue(GenerationStamp.LAST_RESERVED_STAMP);
    getBlockIdGenerator().setCurrentValue(SequentialBlockIdGenerator
      .LAST_RESERVED_BLOCK_ID);
    getBlockGroupIdGenerator().setCurrentValue(Long.MIN_VALUE);
    legacyGenerationStampLimit = HdfsConstants.GRANDFATHER_GENERATION_STAMP;
  }

  /**
   * 判断块是否为纠删码条带化块，需要同时满足块ID特征和非传统块
   *
   * 因为传统随机块ID也可能出现负数值，不能仅通过块ID判断，需要结合生成戳排除传统块
   *
   * @param block 待判断块
   * @return true为条带化块，false为普通块
   * @see #isLegacyBlock(Block)
   */
  public boolean isStripedBlock(Block block) {
    return isStripedBlockID(block.getBlockId()) && !isLegacyBlock(block);
  }

  /**
   * 仅通过块ID判断是否为条带化块ID，不能单独使用该方法判断块类型，需要结合isLegacyBlock排除传统块
   * @param id 块ID
   * @return true为条带化块ID格式
   * @see #isStripedBlock(Block)
   */
  public static boolean isStripedBlockID(long id) {
    return BlockType.fromBlockId(id) == STRIPED;
  }

  /**
   * 将条带块ID转换为块组ID，利用位运算取出高60位作为块组ID
   * HdfsConstants.BLOCK_GROUP_INDEX_MASK的低4位为1111，取反后低4位为0000，其余60位为1
   * 同一个条带块组内所有数据块和校验块的块组ID相同
   * @param id 条带块ID
   * @return 块组ID
   */
  static long convertToStripedID(long id) {
    return id & (~HdfsServerConstants.BLOCK_GROUP_INDEX_MASK);
  }

  /**
   * 获取块在条带组内的索引，从块ID低4位取出
   * @param reportedBlock 上报的块
   * @return 块在组内的索引
   */
  public static byte getBlockIndex(Block reportedBlock) {
    return (byte) (reportedBlock.getBlockId() &
        HdfsServerConstants.BLOCK_GROUP_INDEX_MASK);
  }

  /**
   * 获取条带块组ID生成器
   * @return 条带块组ID生成器实例
   */
  SequentialBlockGroupIdGenerator getBlockGroupIdGenerator() {
    return blockGroupIdGenerator;
  }
}