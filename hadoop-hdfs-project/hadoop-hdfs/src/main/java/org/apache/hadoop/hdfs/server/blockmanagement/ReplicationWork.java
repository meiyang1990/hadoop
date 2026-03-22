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

import org.apache.hadoop.net.Node;

import java.util.List;
import java.util.Set;

/**
 * 数据块副本复制任务，负责将已有数据块复制到新节点，用于补足副本数量不足的场景。
 * 继承BlockReconstructionWork，实现了块重建任务中复制副本的具体逻辑。
 */
class ReplicationWork extends BlockReconstructionWork {
  /**
   * 构造副本复制任务
   * @param block 需要复制的数据块
   * @param bc 数据块所属的块集合（文件/目录）
   * @param srcNodes 源数据节点，这里必须只有一个源节点
   * @param containingNodes 当前持有该块的数据节点列表
   * @param liveReplicaStorages 当前存活的副本存储信息列表
   * @param additionalReplRequired 需要新增的副本数量
   * @param priority 任务优先级
   */
  public ReplicationWork(BlockInfo block, BlockCollection bc,
      DatanodeDescriptor[] srcNodes, List<DatanodeDescriptor> containingNodes,
      List<DatanodeStorageInfo> liveReplicaStorages, int additionalReplRequired,
      int priority) {
    super(block, bc, srcNodes, containingNodes,
        liveReplicaStorages, additionalReplRequired, priority);
    assert getSrcNodes().length == 1 :
        "There should be exactly 1 source node that have been selected";
    getSrcNodes()[0].incrementPendingReplicationWithoutTargets();
    LOG.debug("Creating a ReplicationWork to reconstruct " + block);
  }

  /**
   * 为副本复制选择目标数据节点
   * @param blockplacement 块放置策略，用于选择合适的目标节点
   * @param storagePolicySuite 存储策略套件，获取当前块对应的存储策略
   * @param excludedNodes 需要排除的节点集合
   */
  @Override
  void chooseTargets(BlockPlacementPolicy blockplacement,
      BlockStoragePolicySuite storagePolicySuite,
      Set<Node> excludedNodes) {
    assert getSrcNodes().length > 0
        : "At least 1 source node should have been selected";
    try {
      DatanodeStorageInfo[] chosenTargets = null;
      // HDFS-14720 如果块已经被删除，不需要进行复制
      if (!getBlock().isDeleted()) {
        // 根据块放置策略选择指定数量的目标节点
        chosenTargets = blockplacement.chooseTarget(getSrcPath(),
            getAdditionalReplRequired(), getSrcNodes()[0],
            getLiveReplicaStorages(), false, excludedNodes, getBlockSize(),
            storagePolicySuite.getPolicy(getStoragePolicyID()), null);
      } else {
        LOG.warn("ReplicationWork could not need choose targets for {}", getBlock());
      }
      // 保存选择好的目标节点
      setTargets(chosenTargets);
    } finally {
      // 无论是否选择成功，都递减源节点的无目标待复制计数
      getSrcNodes()[0].decrementPendingReplicationWithoutTargets();
    }
  }

  /**
   * 将复制任务添加到源数据节点，等待源节点执行块复制
   * @param numberReplicas 副本数量统计信息
   * @return 始终返回true，表示添加成功
   */
  @Override
  boolean addTaskToDatanode(NumberReplicas numberReplicas) {
    getSrcNodes()[0].addBlockToBeReplicated(getBlock(), getTargets());
    return true;
  }
}