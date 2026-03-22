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

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.util.StripedBlockUtil;
import org.apache.hadoop.net.Node;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 纠删码块重建任务类，继承自BlockReconstructionWork，负责处理纠删码条带中丢失块的重建工作。
 * 核心职责包括：选择重建目标节点、处理不同场景下的块恢复（包括数据节点退役、机架数量不足等场景）、
 * 区分全重建和简单复制场景，将任务下发到目标数据节点执行重建。
 */
class ErasureCodingWork extends BlockReconstructionWork {
  // 可用存活块在条带中的索引数组
  private final byte[] liveBlockIndices;
  // 繁忙的存活块在条带中的索引数组
  private final byte[] liveBusyBlockIndices;
  // 需要排除的已重建块索引数组
  private final byte[] excludeReconstructedIndices;
  // 块池ID
  private final String blockPoolId;

  /**
   * 构造纠删码块重建任务。
   * @param blockPoolId 块池ID
   * @param block 待重建的条带块信息
   * @param bc 块所属文件集合
   * @param srcNodes 源数据节点数组
   * @param containingNodes 包含该块的数据节点列表
   * @param liveReplicaStorages 存活副本存储列表
   * @param additionalReplRequired 需要新增的副本数量
   * @param priority 任务优先级
   * @param liveBlockIndices 可用存活块索引数组
   * @param liveBusyBlockIndices 繁忙存活块索引数组
   * @param excludeReconstrutedIndices 需要排除的已重建块索引数组
   */
  public ErasureCodingWork(String blockPoolId, BlockInfo block,
      BlockCollection bc,
      DatanodeDescriptor[] srcNodes,
      List<DatanodeDescriptor> containingNodes,
      List<DatanodeStorageInfo> liveReplicaStorages,
      int additionalReplRequired, int priority,
      byte[] liveBlockIndices, byte[] liveBusyBlockIndices,
      byte[] excludeReconstrutedIndices) {
    super(block, bc, srcNodes, containingNodes,
        liveReplicaStorages, additionalReplRequired, priority);
    this.blockPoolId = blockPoolId;
    this.liveBlockIndices = liveBlockIndices;
    this.liveBusyBlockIndices = liveBusyBlockIndices;
    this.excludeReconstructedIndices = excludeReconstrutedIndices;
    LOG.debug("Creating an ErasureCodingWork to {} reconstruct ",
        block);
  }

  /**
   * 获取可用存活块索引数组。
   * @return 可用存活块索引数组
   */
  byte[] getLiveBlockIndices() {
    return liveBlockIndices;
  }

  /**
   * 为纠删码块重建选择目标存储节点。
   * @param blockplacement 块放置策略
   * @param storagePolicySuite 存储策略集合
   * @param excludedNodes 需要排除的节点集合
   */
  @Override
  void chooseTargets(BlockPlacementPolicy blockplacement,
      BlockStoragePolicySuite storagePolicySuite,
      Set<Node> excludedNodes) {
    // TODO: new placement policy for EC considering multiple writers
    DatanodeStorageInfo[] chosenTargets = null;
    // 若块已被删除，则无需执行重建，跳过目标选择
    if (!getBlock().isDeleted()) {
      chosenTargets = blockplacement.chooseTarget(
          getSrcPath(), getAdditionalReplRequired(), getSrcNodes()[0],
          getLiveReplicaStorages(), false, excludedNodes, getBlockSize(),
          storagePolicySuite.getPolicy(getStoragePolicyID()), null);
    } else {
      LOG.warn("ErasureCodingWork could not need choose targets for {}", getBlock());
    }
    setTargets(chosenTargets);
  }

  /**
   * 检查当前是否已经拥有所有内部块（数据块+校验块）的存活副本，仅缺少足够机架分布。
   * @return true表示所有内部块都已有存活副本，仅需要补充机架分布
   */
  private boolean hasAllInternalBlocks() {
    final BlockInfoStriped block = (BlockInfoStriped) getBlock();
    // 存活块总数小于实际需要的总块数，直接返回false
    if (liveBlockIndices.length
        + liveBusyBlockIndices.length < block.getRealTotalBlockNum()) {
      return false;
    }
    BitSet bitSet = new BitSet(block.getTotalBlockNum());
    // 标记所有存活块索引
    for (byte index : liveBlockIndices) {
      bitSet.set(index);
    }
    for (byte busyIndex: liveBusyBlockIndices) {
      bitSet.set(busyIndex);
    }
    // 检查所有数据块是否都存在
    for (int i = 0; i < block.getRealDataBlockNum(); i++) {
      if (!bitSet.get(i)) {
        return false;
      }
    }
    // 检查所有校验块是否都存在
    for (int i = block.getDataBlockNum(); i < block.getTotalBlockNum(); i++) {
      if (!bitSet.get(i)) {
        return false;
      }
    }
    return true;
  }

  /**
   * 当所有内部块都已存在但机架数量不足时，选择源数据节点进行简单复制（无需解码重建）。
   * 算法选择拥有最多块副本的机架，从中选择第一个源节点。
   * @return 源数据节点在源数组中的索引
   */
  private int chooseSource4SimpleReplication() {
    // 按机架分组存储源节点索引
    Map<String, List<Integer>> map = new HashMap<>();
    for (int i = 0; i < getSrcNodes().length; i++) {
      final String rack = getSrcNodes()[i].getNetworkLocation();
      List<Integer> dnList = map.get(rack);
      if (dnList == null) {
        dnList = new ArrayList<>();
        map.put(rack, dnList);
      }
      dnList.add(i);
    }
    // 找到拥有最多节点数量的机架
    List<Integer> max = null;
    for (Map.Entry<String, List<Integer>> entry : map.entrySet()) {
      if (max == null || entry.getValue().size() > max.size()) {
        max = entry.getValue();
      }
    }
    assert max != null;
    // 返回该机架中第一个源节点索引
    return max.get(0);
  }

  /**
   * 将纠删码重建任务添加到目标数据节点执行。
   * 根据不同场景选择不同处理方式：机架不足场景做简单复制、节点退役场景迁移丢失块、常规场景执行完整解码重建。
   * @param numberReplicas 各类副本数量统计信息
   * @return 是否成功添加任务
   */
  @Override
  boolean addTaskToDatanode(NumberReplicas numberReplicas) {
    final DatanodeStorageInfo[] targets = getTargets();
    assert targets.length > 0;
    BlockInfoStriped stripedBlk = (BlockInfoStriped) getBlock();
    boolean flag = true;
    if (hasNotEnoughRack()) {
      // 已有所有内部块，但机架分布不足，只需复制一个块到新机架即可
      int sourceIndex = chooseSource4SimpleReplication();
      createReplicationWork(sourceIndex, targets[0]);
    } else if ((numberReplicas.decommissioning() > 0 ||
        numberReplicas.liveEnteringMaintenanceReplicas() > 0) &&
        hasAllInternalBlocks()) {
      // 存在节点退役/进入维护，且所有块都已在正常节点存在，只需迁移即将下线节点上的块
      List<Integer> leavingServiceSources = findLeavingServiceSources();
      final int num = Math.min(leavingServiceSources.size(), targets.length);
      if (num == 0) {
        flag = false;
      }
      // 为每个需要迁移的块创建复制任务
      for (int i = 0; i < num; i++) {
        createReplicationWork(leavingServiceSources.get(i), targets[i]);
      }
    } else {
      // 常规场景：下发完整纠删码重建任务到目标数据节点执行解码重建
      targets[0].getDatanodeDescriptor().addBlockToBeErasureCoded(
          new ExtendedBlock(blockPoolId, stripedBlk), getSrcNodes(), targets,
          liveBlockIndices, excludeReconstructedIndices, stripedBlk.getErasureCodingPolicy());
    }
    return flag;
  }

  /**
   * 创建单个内部块的简单复制任务，添加到源数据节点。
   * @param sourceIndex 源块在当前任务源数组中的索引
   * @param target 目标存储信息
   */
  private void createReplicationWork(int sourceIndex,
      DatanodeStorageInfo target) {
    BlockInfoStriped stripedBlk = (BlockInfoStriped) getBlock();
    final byte blockIndex = liveBlockIndices[sourceIndex];
    final DatanodeDescriptor source = getSrcNodes()[sourceIndex];
    // 计算该内部块的实际长度
    final long internBlkLen = StripedBlockUtil.getInternalBlockLength(
        stripedBlk.getNumBytes(), stripedBlk.getCellSize(),
        stripedBlk.getDataBlockNum(), blockIndex);
    // 构造目标块对象
    final Block targetBlk = new Block(stripedBlk.getBlockId() + blockIndex,
        internBlkLen, stripedBlk.getGenerationStamp());
    // 添加复制任务到源节点
    source.addECBlockToBeReplicated(targetBlk,
        new DatanodeStorageInfo[] {target});
    LOG.debug("Add replication task from source {} to "
        + "target {} for EC block {}", source, target, targetBlk);
  }

  /**
   * 查找需要下线节点上、且不存在于其他正常节点的块对应的源节点索引。
   * 这些块需要在节点退役过程中迁移出去。
   * @return 需要迁移的源节点索引列表
   */
  private List<Integer> findLeavingServiceSources() {
    BlockInfoStriped block = (BlockInfoStriped)getBlock();
    BitSet bitSet = new BitSet(block.getRealTotalBlockNum());
    // 标记正常在线节点上存在的块索引
    for (int i = 0; i < getSrcNodes().length; i++) {
      if (getSrcNodes()[i].isInService()) {
        bitSet.set(liveBlockIndices[i]);
      }
    }
    // 收集处于退役/维护中、且该块不存在于其他正常节点的源节点
    List<Integer> srcIndices = new ArrayList<>();
    for (int i = 0; i < getSrcNodes().length; i++) {
      if ((getSrcNodes()[i].isDecommissionInProgress() ||
          (getSrcNodes()[i].isEnteringMaintenance() &&
          getSrcNodes()[i].isAlive())) &&
          !bitSet.get(liveBlockIndices[i])) {
        srcIndices.add(i);
      }
    }
    return srcIndices;
  }
}