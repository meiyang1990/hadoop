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

import java.util.Collection;
import java.util.ArrayList;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;

import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;

/**
 * 文件级注释：HDFS Namenode向Datanode发送的块恢复命令，用于触发指定数据块的恢复流程
 * <p>
 * BlockRecoveryCommand is an instruction to a data-node to recover
 * the specified blocks.
 *
 * The data-node that receives this command treats itself as a primary
 * data-node in the recover process.
 *
 * Block recovery is identified by a recoveryId, which is also the new
 * generation stamp, which the block will have after the recovery succeeds.
 * <p>
 * 核心功能：NameNode将接收该命令的Datanode指定为恢复流程的主节点，由它协调所有持有该块的Datanode完成块恢复
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class BlockRecoveryCommand extends DatanodeCommand {
  final Collection<RecoveringBlock> recoveringBlocks;

  /**
   * 待恢复块信息封装类，保存待恢复块的位置信息和恢复完成后的新世代戳
   * This is a block with locations from which it should be recovered
   * and the new generation stamp, which the block will have after 
   * successful recovery.
   * 
   * The new generation stamp of the block, also plays role of the recovery id.
   */
  @InterfaceAudience.Private
  @InterfaceStability.Evolving
  public static class RecoveringBlock extends LocatedBlock {
    private final long newGenerationStamp;
    private final Block recoveryBlock;

    /**
     * 构造待恢复块对象
     * @param b 待恢复的扩展块
     * @param locs 持有该块的Datanode位置列表
     * @param newGS 恢复完成后的新世代戳，同时作为恢复ID
     */
    public RecoveringBlock(ExtendedBlock b, DatanodeInfo[] locs, long newGS) {
      super(b, locs); // startOffset is unknown
      this.newGenerationStamp = newGS;
      this.recoveryBlock = null;
    }

    /**
     * 构造支持截断复制场景的待恢复块对象
     * @param b 待恢复的扩展块
     * @param locs 持有该块的Datanode位置列表
     * @param recoveryBlock 包含新世代戳的恢复后块信息
     */
    public RecoveringBlock(ExtendedBlock b, DatanodeInfo[] locs,
        Block recoveryBlock) {
      super(b, locs); // startOffset is unknown
      this.newGenerationStamp = recoveryBlock.getGenerationStamp();
      this.recoveryBlock = recoveryBlock;
    }

    /**
     * 拷贝构造方法，从已有RecoveringBlock创建新对象
     * @param rBlock 已有待恢复块对象
     */
    public RecoveringBlock(RecoveringBlock rBlock) {
      super(rBlock.getBlock(), rBlock.getLocations(), rBlock.getStorageIDs(),
          rBlock.getStorageTypes());
      this.newGenerationStamp = rBlock.newGenerationStamp;
      this.recoveryBlock = rBlock.recoveryBlock;
    }

    /**
     * 获取恢复完成后的新世代戳，该值同时作为恢复ID
     * @return 新世代戳
     */
    public long getNewGenerationStamp() {
      return newGenerationStamp;
    }

    /**
     * 获取恢复完成后的新块对象
     * @return 新块对象，截断复制场景下有效
     */
    public Block getNewBlock() {
      return recoveryBlock;
    }
  }

  /**
   * 纠删码条带化块的恢复信息类，继承普通待恢复块，添加纠擦码相关信息
   */
  public static class RecoveringStripedBlock extends RecoveringBlock {
    private final byte[] blockIndices;
    private final ErasureCodingPolicy ecPolicy;

    /**
     * 构造条带化待恢复块对象
     * @param rBlock 基础待恢复块信息
     * @param blockIndices 需要恢复的块索引列表（在纠删码组内索引
     * @param ecPolicy 当前使用的纠删码策略
     */
    public RecoveringStripedBlock(RecoveringBlock rBlock, byte[] blockIndices,
        ErasureCodingPolicy ecPolicy) {
      super(rBlock);
      this.blockIndices = blockIndices == null ? new byte[]{} : blockIndices;
      this.ecPolicy = ecPolicy;
    }

    /**
     * 获取需要恢复的条带块索引数组
     * @return 块索引数组
     */
    public byte[] getBlockIndices() {
      return blockIndices;
    }

    /**
     * 获取当前条带块使用的纠删码策略
     * @return 纠删码策略
     */
    public ErasureCodingPolicy getErasureCodingPolicy() {
      return ecPolicy;
    }

    @Override
    public boolean isStriped() {
      return true;
    }
  }

  /**
   * 构造空的块恢复命令对象
   */
  public BlockRecoveryCommand() {
    this(0);
  }

  /**
   * 构造指定初始容量的块恢复命令对象
   * @param capacity 预期待恢复块数量，用于初始化集合容量
   */
  public BlockRecoveryCommand(int capacity) {
    this(new ArrayList<RecoveringBlock>(capacity));
  }
  
  /**
   * 构造块恢复命令，使用给定的待恢复块集合
   * @param blocks 待恢复块集合
   */
  public BlockRecoveryCommand(Collection<RecoveringBlock> blocks) {
    super(DatanodeProtocol.DNA_RECOVERBLOCK);
    recoveringBlocks = blocks;
  }

  /**
   * 获取命令中所有待恢复块的集合
   * @return 待恢复块集合
   */
  public Collection<RecoveringBlock> getRecoveringBlocks() {
    return recoveringBlocks;
  }

  /**
   * 向命令中添加一个待恢复块
   * @param block 待添加的待恢复块
   */
  public void add(RecoveringBlock block) {
    recoveringBlocks.add(block);
  }
  
  @Override
  public String toString() {
    // 拼接命令字符串，包含所有待恢复块信息
    StringBuilder sb = new StringBuilder();
    sb.append("BlockRecoveryCommand(\n  ");
    Joiner.on("\n  ").appendTo(sb, recoveringBlocks);
    sb.append("\n)");
    return sb.toString();
  }
}