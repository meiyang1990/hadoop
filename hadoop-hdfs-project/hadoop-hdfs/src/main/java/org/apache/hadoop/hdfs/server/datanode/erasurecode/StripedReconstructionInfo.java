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
package org.apache.hadoop.hdfs.server.datanode.erasurecode;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;

/**
 * 存储纠删码条带化块重建所需的全部信息，用于在数据节点执行丢失块的恢复计算。
 * 包含源数据块位置、目标重建位置、纠删码策略等重建流程需要的所有参数。
 */
@InterfaceAudience.Private
public class StripedReconstructionInfo {

  private final ExtendedBlock blockGroup;
  private final ErasureCodingPolicy ecPolicy;

  // 源数据相关信息：可用的数据块索引和对应的DataNode节点
  private final byte[] liveIndices;
  private final DatanodeInfo[] sources;

  // 目标重建相关信息：需要重建的块索引、目标节点、存储信息
  private final byte[] targetIndices;
  private final DatanodeInfo[] targets;
  private final StorageType[] targetStorageTypes;
  private final String[] targetStorageIds;
  private final byte[] excludeReconstructedIndices;

  /**
   * 构造条带化重建信息，用于仅指定重建索引，目标节点后续补充的场景。
   * @param blockGroup 待重建的条带块组
   * @param ecPolicy 纠删码编码策略
   * @param liveIndices 可用的存活数据块索引列表
   * @param sources 提供源数据的DataNode节点列表
   * @param targetIndices 需要重建的目标块索引列表
   */
  public StripedReconstructionInfo(ExtendedBlock blockGroup,
      ErasureCodingPolicy ecPolicy, byte[] liveIndices, DatanodeInfo[] sources,
      byte[] targetIndices) {
    this(blockGroup, ecPolicy, liveIndices, sources, targetIndices, null,
        null, null, new byte[0]);
  }

  /**
   * 构造条带化重建信息，指定全部源和目标节点信息。
   * @param blockGroup 待重建的条带块组
   * @param ecPolicy 纠删码编码策略
   * @param liveIndices 可用的存活数据块索引列表
   * @param sources 提供源数据的DataNode节点列表
   * @param targets 存放重建结果的目标DataNode节点列表
   * @param targetStorageTypes 目标存储类型列表
   * @param targetStorageIds 目标存储ID列表
   * @param excludeReconstructedIndices 需要排除的已重建块索引列表
   */
  StripedReconstructionInfo(ExtendedBlock blockGroup,
      ErasureCodingPolicy ecPolicy, byte[] liveIndices, DatanodeInfo[] sources,
      DatanodeInfo[] targets, StorageType[] targetStorageTypes,
      String[] targetStorageIds, byte[] excludeReconstructedIndices) {
    this(blockGroup, ecPolicy, liveIndices, sources, null, targets,
        targetStorageTypes, targetStorageIds, excludeReconstructedIndices);
  }

  /**
   * 私有全参数构造方法，由各个重载构造方法调用完成对象初始化。
   * @param blockGroup 待重建的条带块组
   * @param ecPolicy 纠删码编码策略
   * @param liveIndices 可用的存活数据块索引列表
   * @param sources 提供源数据的DataNode节点列表
   * @param targetIndices 需要重建的目标块索引列表
   * @param targets 存放重建结果的目标DataNode节点列表
   * @param targetStorageTypes 目标存储类型列表
   * @param targetStorageIds 目标存储ID列表
   * @param excludeReconstructedIndices 需要排除的已重建块索引列表
   */
  private StripedReconstructionInfo(ExtendedBlock blockGroup,
      ErasureCodingPolicy ecPolicy, byte[] liveIndices, DatanodeInfo[] sources,
      byte[] targetIndices, DatanodeInfo[] targets,
      StorageType[] targetStorageTypes, String[] targetStorageIds,
      byte[] excludeReconstructedIndices) {

    this.blockGroup = blockGroup;
    this.ecPolicy = ecPolicy;
    this.liveIndices = liveIndices;
    this.sources = sources;
    this.targetIndices = targetIndices;
    this.targets = targets;
    this.targetStorageTypes = targetStorageTypes;
    this.targetStorageIds = targetStorageIds;
    this.excludeReconstructedIndices = excludeReconstructedIndices;
  }

  /**
   * 获取待重建的条带块组。
   * @return 条带化块组对象
   */
  ExtendedBlock getBlockGroup() {
    return blockGroup;
  }

  /**
   * 获取当前条带使用的纠删码编码策略。
   * @return 纠删码编码策略对象
   */
  ErasureCodingPolicy getEcPolicy() {
    return ecPolicy;
  }

  /**
   * 获取当前可用的存活数据块索引列表。
   * @return 存活块索引数组
   */
  byte[] getLiveIndices() {
    return liveIndices;
  }

  /**
   * 获取提供源数据的DataNode节点列表。
   * @return 源DataNode信息数组
   */
  DatanodeInfo[] getSources() {
    return sources;
  }

  /**
   * 获取需要重建的目标块索引列表。
   * @return 目标块索引数组
   */
  byte[] getTargetIndices() {
    return targetIndices;
  }

  /**
   * 获取存放重建结果的目标DataNode节点列表。
   * @return 目标DataNode信息数组
   */
  DatanodeInfo[] getTargets() {
    return targets;
  }

  /**
   * 获取目标节点的存储类型列表。
   * @return 目标存储类型数组
   */
  StorageType[] getTargetStorageTypes() {
    return targetStorageTypes;
  }

  /**
   * 获取目标节点的存储ID列表。
   * @return 目标存储ID数组
   */
  String[] getTargetStorageIds() {
    return targetStorageIds;
  }

  /**
   * 获取需要排除的已完成重建的块索引列表。
   * @return 排除块索引数组
   */
  byte[] getExcludeReconstructedIndices() {
    return excludeReconstructedIndices;
  }

}