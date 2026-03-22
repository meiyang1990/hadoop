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
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;

import java.util.*;

/**
 * @file BlockPlacementPolicyRackFaultTolerant.java
 * @brief 机架容错感知的块放置策略实现，核心目标是将副本尽可能分布到更多不同的机架上，提升故障容错能力
 * 
 * 该策略继承默认块放置策略，通过优先将副本分散到不同机架来最大化提升机架级故障容错能力，
 * 当机架数量足够时，每个机架只放一个副本；当副本数量超过机架数量时，会尽可能均匀分布副本。
 */
@InterfaceAudience.Private
public class BlockPlacementPolicyRackFaultTolerant extends BlockPlacementPolicyDefault {

  /**
   * 计算每个机架最多可放置的副本数量，用于指导块放置
   * @param numOfChosen 已选择的数据节点数量
   * @param numOfReplicas 还需要选择的副本数量
   * @return 长度为2的数组，第一个元素是剩余需要选择的副本数，第二个元素是每个机架允许的最大节点数
   */
  @Override
  protected int[] getMaxNodesPerRack(int numOfChosen, int numOfReplicas) {
    // 获取集群总数据节点数量
    int clusterSize = clusterMap.getNumOfLeaves();
    int totalNumOfReplicas = numOfChosen + numOfReplicas;
    // 如果总副本数超过集群节点总数，调整为最大可用节点数
    if (totalNumOfReplicas > clusterSize) {
      numOfReplicas -= (totalNumOfReplicas-clusterSize);
      totalNumOfReplicas = clusterSize;
    }
    // 获取集群中非空机架数量
    int numOfRacks = clusterMap.getNumOfNonEmptyRacks();
    // 当机架数量<=1或只需要选1个节点时，使用默认配置，避免算术异常
    if (numOfRacks <= 1 || totalNumOfReplicas <= 1) {
      return new int[] {numOfReplicas, totalNumOfReplicas};
    }
    // 如果机架数量比总副本数多，每个机架放1个副本
    if (totalNumOfReplicas < numOfRacks) {
      return new int[] {numOfReplicas, 1};
    }
    // 如果副本数多于机架数，向上取整计算每个机架最多放置的副本数
    int maxNodesPerRack = (totalNumOfReplicas - 1) / numOfRacks + 1;
    return new int[] {numOfReplicas, maxNodesPerRack};
  }

  /**
   * 按优先级顺序选择数据节点放置副本，尽可能将副本均匀分布到不同机架
   * @param numOfReplicas 需要选择的副本数量
   * @param writer 写入节点（客户端所在节点）
   * @param excludedNodes 需要排除的节点列表
   * @param blocksize 块大小
   * @param maxNodesPerRack 每个机架允许的最大节点数
   * @param results 存储选择结果的列表
   * @param avoidStaleNodes 是否避免选择 stale 节点
   * @param newBlock 是否是新建块
   * @param storageTypes 存储类型需求
   * @return 写入节点本地节点
   * @throws NotEnoughReplicasException 当无法选到足够节点时抛出异常
   */
  @Override
  protected Node chooseTargetInOrder(int numOfReplicas,
                                 Node writer,
                                 final Set<Node> excludedNodes,
                                 final long blocksize,
                                 final int maxNodesPerRack,
                                 final List<DatanodeStorageInfo> results,
                                 final boolean avoidStaleNodes,
                                 final boolean newBlock,
                                 EnumMap<StorageType, Integer> storageTypes)
                                 throws NotEnoughReplicasException {
    int totalReplicaExpected = results.size() + numOfReplicas;
    int numOfRacks = clusterMap.getNumOfNonEmptyRacks();

    try {
      // 当总期望副本数小于机架数，或刚好被机架数整除时，直接一次选择完成
      if (totalReplicaExpected < numOfRacks ||
          totalReplicaExpected % numOfRacks == 0) {
        writer = chooseOnce(numOfReplicas, writer, excludedNodes, blocksize,
            maxNodesPerRack, results, avoidStaleNodes, storageTypes);
        return writer;
      }

      assert totalReplicaExpected > (maxNodesPerRack -1) * numOfRacks;

      // 统计每个已选副本所在机架的节点计数
      HashMap<String, Integer> rackCounts = new HashMap<>();
      for (DatanodeStorageInfo dsInfo : results) {
        String rack = dsInfo.getDatanodeDescriptor().getNetworkLocation();
        Integer count = rackCounts.get(rack);
        if (count != null) {
          rackCounts.put(rack, count + 1);
        } else {
          rackCounts.put(rack, 1);
        }
      }
      // 计算已选结果中超过(maxNodesPerRack-1)限制的超额节点总数
      int excess = 0;
      for (int count : rackCounts.values()) {
        if (count > maxNodesPerRack -1) {
          excess += count - (maxNodesPerRack -1);
        }
      }
      // 计算本轮需要选择的副本数，保证每个机架最多放(maxNodesPerRack-1)个
      numOfReplicas = Math.min(totalReplicaExpected - results.size(),
          (maxNodesPerRack -1) * numOfRacks - (results.size() - excess));

      // 第一阶段：选择节点，每个机架最多放(maxNodesPerRack-1)个
      writer = chooseOnce(numOfReplicas, writer, new HashSet<>(excludedNodes),
          blocksize, maxNodesPerRack - 1, results, avoidStaleNodes,
          storageTypes);

      // 将已选节点加入排除列表，避免重复选择
      for (DatanodeStorageInfo resultStorage : results) {
        addToExcludedNodes(resultStorage.getDatanodeDescriptor(),
            excludedNodes);
      }
      LOG.trace("Chosen nodes: {}", results);
      LOG.trace("Excluded nodes: {}", excludedNodes);

      // 第二阶段：选择剩余需要的副本，每个机架可以放最多maxNodesPerRack个
      numOfReplicas = totalReplicaExpected - results.size();
      chooseOnce(numOfReplicas, writer, excludedNodes, blocksize,
          maxNodesPerRack, results, avoidStaleNodes, storageTypes);
    } catch (NotEnoughReplicasException e) {
      // 均匀放置失败，降级到从剩余机架尽力而为放置
      LOG.warn("Only able to place {} of total expected {}"
              + " (maxNodesPerRack={}, numOfReplicas={}) nodes "
              + "evenly across racks, falling back to evenly place on the "
              + "remaining racks. This may not guarantee rack-level fault "
              + "tolerance. Please check if the racks are configured properly.",
          results.size(), totalReplicaExpected, maxNodesPerRack, numOfReplicas);
      LOG.debug("Caught exception was:", e);
      chooseEvenlyFromRemainingRacks(writer, excludedNodes, blocksize,
          maxNodesPerRack, results, avoidStaleNodes, storageTypes,
          totalReplicaExpected, e);

    }

    return writer;
  }

  /**
   * 当均匀放置失败时，从剩余可用机架尽力而为均匀选择节点
   * @param writer 写入节点
   * @param excludedNodes 需要排除的节点列表
   * @param blocksize 块大小
   * @param maxNodesPerRack 每个机架允许的最大节点数
   * @param results 存储选择结果的列表
   * @param avoidStaleNodes 是否避免选择 stale 节点
   * @param storageTypes 存储类型需求
   * @param totalReplicaExpected 总共需要的副本数
   * @param e 原始异常
   * @throws NotEnoughReplicasException 尽力后仍无法满足需求时抛出异常
   */
  private void chooseEvenlyFromRemainingRacks(Node writer,
      Set<Node> excludedNodes, long blocksize, int maxNodesPerRack,
      List<DatanodeStorageInfo> results, boolean avoidStaleNodes,
      EnumMap<StorageType, Integer> storageTypes, int totalReplicaExpected,
      NotEnoughReplicasException e) throws NotEnoughReplicasException {
    int numResultsOflastChoose = 0;
    NotEnoughReplicasException lastException = e;
    int bestEffortMaxNodesPerRack = maxNodesPerRack;
    // 逐步放宽每个机架最大节点数限制，直到选够副本或无法继续
    while (results.size() != totalReplicaExpected &&
        bestEffortMaxNodesPerRack < totalReplicaExpected) {
      // 构造新的排除列表，包含所有已选节点
      final Set<Node> newExcludeNodes = new HashSet<>();
      for (DatanodeStorageInfo resultStorage : results) {
        addToExcludedNodes(resultStorage.getDatanodeDescriptor(),
            newExcludeNodes);
      }

      LOG.trace("Chosen nodes: {}", results);
      LOG.trace("Excluded nodes: {}", excludedNodes);
      LOG.trace("New Excluded nodes: {}", newExcludeNodes);
      final int numOfReplicas = totalReplicaExpected - results.size();
      numResultsOflastChoose = results.size();
      try {
        // 尝试放宽限制后选择节点
        chooseOnce(numOfReplicas, writer, newExcludeNodes, blocksize,
            ++bestEffortMaxNodesPerRack, results, avoidStaleNodes,
            storageTypes);
      } catch (NotEnoughReplicasException nere) {
        lastException = nere;
      } finally {
        excludedNodes.addAll(newExcludeNodes);
      }
      // 如果本轮没有选到新节点，重新计算最大每个机架节点数
      if (numResultsOflastChoose == results.size()) {
        Map<String, Integer> nodesPerRack = new HashMap<>();
        for (DatanodeStorageInfo dsInfo : results) {
          String rackName = dsInfo.getDatanodeDescriptor().getNetworkLocation();
          nodesPerRack.merge(rackName, 1, Integer::sum);
        }
        bestEffortMaxNodesPerRack =
            Math.max(bestEffortMaxNodesPerRack, Collections.max(nodesPerRack.values()));
      }
    }

    // 仍然没选够副本，抛出异常
    if (results.size() != totalReplicaExpected) {
      LOG.debug("Best effort placement failed: expecting {} replicas, only "
          + "chose {}.", totalReplicaExpected, results.size());
      throw lastException;
    }
  }

  /**
   * 单次选择指定数量的副本，第一个副本优先选择本地节点，剩余副本随机选择
   * @param numOfReplicas 需要选择的副本数量
   * @param writer 写入节点
   * @param excludedNodes 需要排除的节点列表
   * @param blocksize 块大小
   * @param maxNodesPerRack 每个机架允许的最大节点数
   * @param results 存储选择结果的列表
   * @param avoidStaleNodes 是否避免选择 stale 节点
   * @param storageTypes 存储类型需求
   * @return 写入节点本地节点
   * @throws NotEnoughReplicasException 无法选到足够节点时抛出异常
   */
  private Node chooseOnce(int numOfReplicas,
                            Node writer,
                            final Set<Node> excludedNodes,
                            final long blocksize,
                            final int maxNodesPerRack,
                            final List<DatanodeStorageInfo> results,
                            final boolean avoidStaleNodes,
                            EnumMap<StorageType, Integer> storageTypes)
                            throws NotEnoughReplicasException {
    if (numOfReplicas == 0) {
      return writer;
    }
    // 优先选择写入节点本地存储放置第一个副本
    writer = chooseLocalStorage(writer, excludedNodes, blocksize,
        maxNodesPerRack, results, avoidStaleNodes, storageTypes, true)
        .getDatanodeDescriptor();
    if (--numOfReplicas == 0) {
      return writer;
    }
    // 剩余副本随机选择
    chooseRandom(numOfReplicas, NodeBase.ROOT, excludedNodes, blocksize,
        maxNodesPerRack, results, avoidStaleNodes, storageTypes);
    return writer;
  }

  /**
   * 验证块放置是否满足当前策略的要求，统计副本分布的机架数量
   * @param locs 已放置的数据节点位置数组
   * @param numberOfReplicas 副本总数
   * @return 块放置状态对象，包含当前分布的机架数等信息
   */
  @Override
  public BlockPlacementStatus verifyBlockPlacement(DatanodeInfo[] locs,
      int numberOfReplicas) {
    if (locs == null)
      locs = DatanodeDescriptor.EMPTY_ARRAY;
    if (!clusterMap.hasClusterEverBeenMultiRack()) {
      // 集群只有一个机架
      return new BlockPlacementStatusDefault(1, 1, 1);
    }
    // 统计不同机架的数量
    Set<String> racks = new HashSet<>();
    for (DatanodeInfo dn : locs) {
      racks.add(dn.getNetworkLocation());
    }
    return new BlockPlacementStatusDefault(racks.size(), numberOfReplicas,
        clusterMap.getNumOfNonEmptyRacks());
  }

  /**
   * 从候选副本集中选择合适的副本集，本策略优先选择超过一个候选的集合
   * @param moreThanOne 每个机架有超过一个节点的候选集合
   * @param exactlyOne 每个机架正好有一个节点的候选集合
   * @param rackMap 按机架分组的候选节点映射
   * @return 选中的候选集合
   */
  @Override
  protected Collection<DatanodeStorageInfo> pickupReplicaSet(
      Collection<DatanodeStorageInfo> moreThanOne,
      Collection<DatanodeStorageInfo> exactlyOne,
      Map<String, List<DatanodeStorageInfo>> rackMap) {
    return moreThanOne.isEmpty() ? exactlyOne : moreThanOne;
  }
}