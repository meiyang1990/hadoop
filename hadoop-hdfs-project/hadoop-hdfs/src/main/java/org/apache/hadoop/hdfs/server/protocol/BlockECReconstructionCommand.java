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

import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeStorageInfo;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;

import java.util.Arrays;
import java.util.Collection;

/**
 * 文件级注释：HDFS纠删码块重构命令，用于NameNode向DataNode下发缺失条带块组的重构任务
 *
 * A BlockECReconstructionCommand is an instruction to a DataNode to
 * reconstruct a striped block group with missing blocks.
 *
 * Upon receiving this command, the DataNode pulls data from other DataNodes
 * hosting blocks in this group and reconstructs the lost blocks through codec
 * calculation.
 *
 * After the reconstruction, the DataNode pushes the reconstructed blocks to
 * their final destinations if necessary (e.g., the destination is different
 * from the reconstruction node, or multiple blocks in a group are to be
 * reconstructed).
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class BlockECReconstructionCommand extends DatanodeCommand {
  private final Collection<BlockECReconstructionInfo> ecTasks;

  /**
   * 构造纠删码块重构命令，从任务信息集合创建命令
   * @param action 命令类型
   * @param blockECReconstructionInfoList 所有需要执行的重构任务列表
   */
  public BlockECReconstructionCommand(int action,
      Collection<BlockECReconstructionInfo> blockECReconstructionInfoList) {
    super(action);
    this.ecTasks = blockECReconstructionInfoList;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("BlockECReconstructionCommand(\n  ");
    Joiner.on("\n  ").appendTo(sb, ecTasks);
    sb.append("\n)");
    return sb.toString();
  }

  /**
   * 类级注释：存储单个纠删码块重构任务的所有信息，包含待重构块、源节点、目标节点等信息
   * Block and targets pair
   */
  @InterfaceAudience.Private
  @InterfaceStability.Evolving
  public static class BlockECReconstructionInfo {
    private final ExtendedBlock block;
    private final DatanodeInfo[] sources;
    private DatanodeInfo[] targets;
    private String[] targetStorageIDs;
    private StorageType[] targetStorageTypes;
    private final byte[] liveBlockIndices;
    private final byte[] excludeReconstructedIndices;
    private final ErasureCodingPolicy ecPolicy;

    /**
     * 构造单个纠删码重构任务信息（从DatanodeStorageInfo转换目标信息）
     * @param block 待重构的扩展块
     * @param sources 提供源数据的DataNode数组
     * @param targetDnStorageInfo 目标存储信息数组
     * @param liveBlockIndices 当前可用块在条带组中的索引
     * @param excludeReconstructedIndices 需要排除的已重构块索引
     * @param ecPolicy 纠删码编码策略
     */
    public BlockECReconstructionInfo(ExtendedBlock block,
        DatanodeInfo[] sources, DatanodeStorageInfo[] targetDnStorageInfo,
        byte[] liveBlockIndices, byte[] excludeReconstructedIndices, ErasureCodingPolicy ecPolicy) {
      this(block, sources, DatanodeStorageInfo
          .toDatanodeInfos(targetDnStorageInfo), DatanodeStorageInfo
          .toStorageIDs(targetDnStorageInfo), DatanodeStorageInfo
          .toStorageTypes(targetDnStorageInfo), liveBlockIndices,
          excludeReconstructedIndices, ecPolicy);
    }

    /**
     * 构造单个纠删码重构任务信息（直接传入转换完成的目标信息）
     * @param block 待重构的扩展块
     * @param sources 提供源数据的DataNode数组
     * @param targets 存储重构结果的目标DataNode数组
     * @param targetStorageIDs 目标存储ID数组
     * @param targetStorageTypes 目标存储类型数组
     * @param liveBlockIndices 当前可用块在条带组中的索引
     * @param excludeReconstructedIndices 需要排除的已重构块索引
     * @param ecPolicy 纠删码编码策略
     */
    public BlockECReconstructionInfo(ExtendedBlock block,
        DatanodeInfo[] sources, DatanodeInfo[] targets,
        String[] targetStorageIDs, StorageType[] targetStorageTypes,
        byte[] liveBlockIndices, byte[] excludeReconstructedIndices, ErasureCodingPolicy ecPolicy) {
      this.block = block;
      this.sources = sources;
      this.targets = targets;
      this.targetStorageIDs = targetStorageIDs;
      this.targetStorageTypes = targetStorageTypes;
      // 处理空可用索引数组，创建空数组避免空指针
      this.liveBlockIndices = liveBlockIndices == null ?
          new byte[]{} : liveBlockIndices;
      this.excludeReconstructedIndices = excludeReconstructedIndices;
      this.ecPolicy = ecPolicy;
    }

    /**
     * 获取待重构的扩展块
     * @return 待重构的扩展块
     */
    public ExtendedBlock getExtendedBlock() {
      return block;
    }

    /**
     * 获取源数据DataNode列表
     * @return 提供源数据的DataNode数组
     */
    public DatanodeInfo[] getSourceDnInfos() {
      return sources;
    }

    /**
     * 获取目标存储DataNode列表
     * @return 存储重构结果的DataNode数组
     */
    public DatanodeInfo[] getTargetDnInfos() {
      return targets;
    }

    /**
     * 获取目标存储ID列表
     * @return 目标存储ID数组
     */
    public String[] getTargetStorageIDs() {
      return targetStorageIDs;
    }

    /**
     * 获取目标存储类型列表
     * @return 目标存储类型数组
     */
    public StorageType[] getTargetStorageTypes() {
      return targetStorageTypes;
    }

    /**
     * 获取可用块在条带组中的索引列表
     * @return 可用块索引数组
     */
    public byte[] getLiveBlockIndices() {
      return liveBlockIndices;
    }

    /**
     * 获取需要排除的已重构块索引列表
     * @return 需要排除的索引数组
     */
    public byte[] getExcludeReconstructedIndices() {
      return excludeReconstructedIndices;
    }

    /**
     * 获取纠删码编码策略
     * @return 当前任务使用的纠删码策略
     */
    public ErasureCodingPolicy getErasureCodingPolicy() {
      return ecPolicy;
    }

    @Override
    public String toString() {
      return new StringBuilder().append("BlockECReconstructionInfo(\n  ")
          .append("Recovering ").append(block).append(" From: ")
          .append(Arrays.asList(sources)).append(" To: [")
          .append(Arrays.asList(targets)).append(")\n")
          .append(" Block Indices: ").append(Arrays.toString(liveBlockIndices))
          .toString();
    }
  }

  /**
   * 获取所有纠删码重构任务列表
   * @return 当前命令包含的所有重构任务集合
   */
  public Collection<BlockECReconstructionInfo> getECTasks() {
    return this.ecTasks;
  }
}