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
import org.apache.hadoop.hdfs.protocol.BlockType;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.namenode.NameNode;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState.COMPLETE;

/**
 * 文件构建中正在构造块的特征类，存储未完成块的构造状态和副本信息。
 * 通常用于保存正在写入或追加的文件的最后一个块，存储分配给该块的相关元数据。
 */
public class BlockUnderConstructionFeature {
  private BlockUCState blockUCState;
  private static final ReplicaUnderConstruction[] NO_REPLICAS =
      new ReplicaUnderConstruction[0];

  /**
   * 块分配时确定的预期副本列表 */
  private ReplicaUnderConstruction[] replicas = NO_REPLICAS;

  /**
   * 块恢复过程中主DataNode的索引，用于日志追踪 */
  private int primaryNodeIndex = -1;

  /**
   * 块恢复成功后新块的生成时间戳，同时作为恢复ID标识，用于识别过期恢复 */
  private long blockRecoveryId = 0;

  /**
   * 写时复制截断场景下使用的源块信息 */
  private BlockInfo truncateBlock;

  /**
   * 构造正在构造块特征对象，初始化块状态和预期副本位置
   * @param blk 正在构造的块
   * @param state 块构造状态
   * @param targets 分配的目标DataNode存储位置
   * @param blockType 块类型（普通/纠删码条纹块
   */
  public BlockUnderConstructionFeature(Block blk,
      BlockUCState state, DatanodeStorageInfo[] targets, BlockType blockType) {
    assert getBlockUCState() != COMPLETE :
        "BlockUnderConstructionFeature cannot be in COMPLETE state";
    this.blockUCState = state;
    setExpectedLocations(blk, targets, blockType);
  }

  /** 设置预期副本位置 */
  public void setExpectedLocations(Block block, DatanodeStorageInfo[] targets,
      BlockType blockType) {
    if (targets == null) {
      return;
    }
    // 统计非空目标位置数量
    int numLocations = 0;
    for (DatanodeStorageInfo target : targets) {
      if (target != null) {
        numLocations++;
      }
    }

    this.replicas = new ReplicaUnderConstruction[numLocations];
    int offset = 0;
    for(int i = 0; i < targets.length; i++) {
      if (targets[i] != null) {
        // 条纹块为每个存储分配唯一块ID，普通块复用原块ID
        Block replicaBlock = blockType == BlockType.STRIPED ?
            new Block(block.getBlockId() + i, 0, block.getGenerationStamp()) :
            block;
        replicas[offset++] = new ReplicaUnderConstruction(replicaBlock,
            targets[i], ReplicaState.RBW);
      }
    }
  }

  /**
   * 获取分配给该块的所有预期存储位置数组，由chooseTargets分配
   * @return 预期存储位置数组
   */
  public DatanodeStorageInfo[] getExpectedStorageLocations() {
    int numLocations = getNumExpectedLocations();
    DatanodeStorageInfo[] storages = new DatanodeStorageInfo[numLocations];
    for (int i = 0; i < numLocations; i++) {
      storages[i] = replicas[i].getExpectedStorageLocation();
    }
    return storages;
  }

  /**
   * 获取预期存储位置迭代器，不保证线程安全，依赖外部FSNamesystem锁保护
   * @return 预期存储位置迭代器
   */
  public Iterator<DatanodeStorageInfo> getExpectedStorageLocationsIterator() {
    return new Iterator<DatanodeStorageInfo>() {
      private int index = 0;

      @Override
      public boolean hasNext() {
        return index <  replicas.length;
      }

      @Override
      public DatanodeStorageInfo next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        return replicas[index++].getExpectedStorageLocation();
      }
    };
  }

  /**
   * 获取每个存储上对应的块索引数组，仅用于纠删码条纹块
   * @return 块索引数组
   */
  public byte[] getBlockIndices() {
    int numLocations = getNumExpectedLocations();
    byte[] indices = new byte[numLocations];
    for (int i = 0; i < numLocations; i++) {
      indices[i] = BlockIdManager.getBlockIndex(replicas[i]);
    }
    return indices;
  }

  /**
   * 根据指定存储索引列表，获取对应块索引数组
   * @param storageIdx 指定存储索引列表
   * @return 对应块索引数组
   */
  public byte[] getBlockIndicesForSpecifiedStorages(List<Integer> storageIdx) {
    byte[] indices = new byte[storageIdx.size()];
    for (int i = 0; i < indices.length; i++) {
      indices[i] = BlockIdManager.getBlockIndex(replicas[storageIdx.get(i)]);
    }
    return indices;
  }

  public int getNumExpectedLocations() {
    return replicas.length;
  }

  /**
   * 提交小于一个条纹大小的纠删码块时，更新未使用存储的调度块计数
   * 减少未存储实际数据块的DataNode上的已调度块计数，修正资源统计
   * @param storedBlock 已存储的条纹块信息
   */
  void updateStorageScheduledSize(BlockInfoStriped storedBlock) {
    assert storedBlock.getUnderConstructionFeature() == this;
    if (replicas.length == 0) {
      return;
    }
    final int dataBlockNum = storedBlock.getDataBlockNum();
    final int realDataBlockNum = storedBlock.getRealDataBlockNum();
    // 实际数据块少于总块数，需要清理多余存储
    if (realDataBlockNum < dataBlockNum) {
      for (ReplicaUnderConstruction replica : replicas) {
      // 索引超出实际数据块范围的存储，需要减少已调度计数
        int index = BlockIdManager.getBlockIndex(replica);
        if (index >= realDataBlockNum && index < dataBlockNum) {
          final DatanodeStorageInfo storage =
              replica.getExpectedStorageLocation();
          storage.getDatanodeDescriptor()
              .decrementBlocksScheduled(storage.getStorageType());
        }
      }
    }
  }

  /**
   * 获取该块当前构造状态
   * @return 块构造状态枚举
   */
  public BlockUCState getBlockUCState() {
    return blockUCState;
  }

  void setBlockUCState(BlockUCState s) {
    blockUCState = s;
  }

  public long getBlockRecoveryId() {
    return blockRecoveryId;
  }

  /** 获取截断场景使用的源块 */
  public BlockInfo getTruncateBlock() {
    return truncateBlock;
  }

  public void setTruncateBlock(BlockInfo recoveryBlock) {
    this.truncateBlock = recoveryBlock;
  }

  /**
   * 将块构造状态设置为已提交 */
  void commit() {
    blockUCState = BlockUCState.COMMITTED;
  }

  /**
   * 获取所有生成时间戳不匹配的过时副本列表
   * @param genStamp 当前正确的生成时间戳
   * @return 过时副本列表
   */
  List<ReplicaUnderConstruction> getStaleReplicas(long genStamp) {
    List<ReplicaUnderConstruction> staleReplicas = new ArrayList<>();
    // 遍历副本收集生成时间戳不匹配的副本
    for (ReplicaUnderConstruction r : replicas) {
      if (genStamp != r.getGenerationStamp()) {
        staleReplicas.add(r);
      }
    }
    return staleReplicas;
  }

  /**
   * 初始化该块的租约恢复流程，选择最新上线的DataNode作为主恢复节点
   * @param blockInfo 需要恢复的块信息
   * @param recoveryId 恢复ID（新的生成时间戳
   * @param startRecovery 是否需要向DataNode下发恢复命令
   */
  public void initializeBlockRecovery(BlockInfo blockInfo, long recoveryId,
      boolean startRecovery) {
    setBlockUCState(BlockUCState.UNDER_RECOVERY);
    blockRecoveryId = recoveryId;
    if (!startRecovery) {
      return;
    }
    if (replicas.length == 0) {
      NameNode.blockStateChangeLog.warn("BLOCK*" +
          " BlockUnderConstructionFeature.initializeBlockRecovery:" +
          " No blocks found, lease removed.");
      // 设置主节点索引为-1并返回
      primaryNodeIndex = -1;
      return;
    }
    boolean allLiveReplicasTriedAsPrimary = true;
    // 检查所有存活副本是否都已经被选过为主节点
    for (ReplicaUnderConstruction replica : replicas) {
      if (replica.isAlive()) {
        allLiveReplicasTriedAsPrimary = allLiveReplicasTriedAsPrimary
            && replica.getChosenAsPrimary();
      }
    }
    // 所有存活节点都尝试过一遍后，重置选择标记
    if (allLiveReplicasTriedAsPrimary) {
      for (ReplicaUnderConstruction replica : replicas) {
        replica.setChosenAsPrimary(false);
      }
    }
    long mostRecentLastUpdate = 0;
    ReplicaUnderConstruction primary = null;
    primaryNodeIndex = -1;
    // 找到最近一次心跳更新存活且未被选过的节点，选择最新的作为主节点
    for (int i = 0; i < replicas.length; i++) {
      if (!(replicas[i].isAlive() && !replicas[i].getChosenAsPrimary())) {
        continue;
      }
      final ReplicaUnderConstruction ruc = replicas[i];
      final long lastUpdate = ruc.getExpectedStorageLocation()
          .getDatanodeDescriptor().getLastUpdateMonotonic();
      if (lastUpdate > mostRecentLastUpdate) {
        primaryNodeIndex = i;
        primary = ruc;
        mostRecentLastUpdate = lastUpdate;
      }
    }
    // 选中主节点后，将块添加到恢复队列，标记为主节点
    if (primary != null) {
      primary.getExpectedStorageLocation().getDatanodeDescriptor()
          .addBlockToBeRecovered(blockInfo);
      primary.setChosenAsPrimary(true);
      NameNode.blockStateChangeLog.debug(
          "BLOCK* {} recovery started, primary={}", this, primary);
    }
  }

  /**
   * 如果报告的副本不在预期列表中，则添加进去，处理同一节点存储变更
   * 如果同一DataNode不同存储的情况，更新存储信息
   * @param storage 报告副本的存储位置
   * @param reportedBlock 报告的块信息
   * @param rState 副本状态
   */
  void addReplicaIfNotPresent(DatanodeStorageInfo storage,
      Block reportedBlock, ReplicaState rState) {
    // 当前副本为空，初始化第一个副本
    if (replicas.length == 0) {
      replicas = new ReplicaUnderConstruction[1];
      replicas[0] = new ReplicaUnderConstruction(reportedBlock, storage,
          rState);
    } else {
      // 遍历现有副本查找是否已经存在
      for (int i = 0; i < replicas.length; i++) {
        DatanodeStorageInfo expected =
            replicas[i].getExpectedStorageLocation();
        // 同一存储已经存在，更新生成时间戳
        if (expected == storage) {
          replicas[i].setGenerationStamp(reportedBlock.getGenerationStamp());
          return;
        } else if (expected != null && expected.getDatanodeDescriptor() ==
            storage.getDatanodeDescriptor()) {
          // 同一DataNode不同存储，允许DataNode选择目标存储，更新存储信息
          replicas[i] = new ReplicaUnderConstruction(reportedBlock, storage,
              rState);
          return;
        }
      }
      // 不存在则扩容副本数组，添加新副本
      ReplicaUnderConstruction[] newReplicas =
          new ReplicaUnderConstruction[replicas.length + 1];
      System.arraycopy(replicas, 0, newReplicas, 0, replicas.length);
      newReplicas[newReplicas.length - 1] = new ReplicaUnderConstruction(
          reportedBlock, storage, rState);
      replicas = newReplicas;
    }
  }

  @Override
  public String toString() {
    final StringBuilder b = new StringBuilder(100);
    appendUCParts(b);
    return b.toString();
  }

  private void appendUCParts(StringBuilder sb) {
    sb.append("{UCState=").append(blockUCState)
      .append(", truncateBlock=").append(truncateBlock)
      .append(", primaryNodeIndex=").append(primaryNodeIndex)
      .append(", replicas=[");
    int i = 0;
    for (ReplicaUnderConstruction r : replicas) {
      r.appendStringTo(sb);
      if (++i < replicas.length) {
        sb.append(", ");
      }
    }
    sb.append("]}");
  }
  
  /**
   * 拼接简洁格式的构造块副本信息到字符串生成器，用于日志输出
   * @param sb 字符串生成器
   */
  public void appendUCPartsConcise(StringBuilder sb) {
    sb.append("replicas=");
    int i = 0;
    for (ReplicaUnderConstruction r : replicas) {
      sb.append(r.getExpectedStorageLocation().getDatanodeDescriptor());
      if (++i < replicas.length) {
        sb.append(", ");
      }
    }
  }
}