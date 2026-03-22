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
package org.apache.hadoop.hdfs.net;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Preconditions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.util.ReflectionUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Random;

/**
 * 文件说明：HDFS专属网络拓扑实现类，继承自通用网络拓扑，核心扩展了按存储类型随机选择节点的能力
 * 类说明：HDFS特定的网络拓扑实现，主要扩展支持感知存储类型的节点选择逻辑，其余逻辑与父类保持一致
 * 当前作为存储类型感知节点选择的核心实现，用于数据块放置时按存储类型选择合适的数据节点
 */
public class DFSNetworkTopology extends NetworkTopology {

  private static final Random RANDOM = new Random();

  /**
   * 根据配置创建DFS网络拓扑实例，支持自定义实现类配置
   * @param conf Hadoop配置对象
   * @return 初始化完成的DFSNetworkTopology实例
   */
  public static DFSNetworkTopology getInstance(Configuration conf) {

    DFSNetworkTopology nt = ReflectionUtils.newInstance(conf.getClass(
        DFSConfigKeys.DFS_NET_TOPOLOGY_IMPL_KEY,
        DFSConfigKeys.DFS_NET_TOPOLOGY_IMPL_DEFAULT,
        DFSNetworkTopology.class), conf);
    return (DFSNetworkTopology) nt.init(DFSTopologyNodeImpl.FACTORY);
  }

  /**
   * 根据指定范围和存储类型，随机选择一个符合要求的数据节点
   * 支持排除指定范围节点和排除节点列表，若范围前缀为~表示排除该范围选择
   * @param scope 选择节点的范围，前缀~表示反向选择
   * @param excludedNodes 需要排除的节点列表
   * @param type 要求的存储类型
   * @return 符合要求的随机节点，无符合节点则返回null
   */
  public Node chooseRandomWithStorageType(final String scope,
      final Collection<Node> excludedNodes, StorageType type) {
    netlock.readLock().lock();
    try {
      if (scope.startsWith("~")) {
        return chooseRandomWithStorageType(
            NodeBase.ROOT, scope.substring(1), excludedNodes, type);
      } else {
        return chooseRandomWithStorageType(
            scope, null, excludedNodes, type);
      }
    } finally {
      netlock.readLock().unlock();
    }
  }

  /**
   * 两次尝试按存储类型随机选择节点，兼顾性能和成功率
   * 第一次尝试使用普通随机选择，若命中符合存储类型的节点直接返回；失败则进行第二次精确选择
   * 该设计基于性能考量：普通随机选择更快，多数情况可命中，仅在失败时使用更精确但稍慢的算法
   * @param scope 选择节点的范围，前缀~表示反向选择
   * @param excludedNodes 需要排除的节点列表
   * @param type 要求的存储类型
   * @return 符合要求的随机节点，无符合节点则返回null
   */
  public Node chooseRandomWithStorageTypeTwoTrial(final String scope,
      final Collection<Node> excludedNodes, StorageType type) {
    netlock.readLock().lock();
    try {
      String searchScope;
      String excludedScope;
      if (scope.startsWith("~")) {
        searchScope = NodeBase.ROOT;
        excludedScope = scope.substring(1);
      } else {
        searchScope = scope;
        excludedScope = null;
      }
      // 第一次尝试：调用父类普通随机选择方法
      Node n = chooseRandom(searchScope, excludedScope, excludedNodes);
      if (n == null) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("No node to choose.");
        }
        // 无可用节点，直接返回空
        return null;
      }
      Preconditions.checkArgument(n instanceof DatanodeDescriptor);
      DatanodeDescriptor dnDescriptor = (DatanodeDescriptor)n;

      if (dnDescriptor.hasStorageType(type)) {
        // 第一次尝试命中符合存储类型的节点，直接返回
        return dnDescriptor;
      } else {
        // 第一次尝试失败，调用精确选择方法进行第二次尝试
        LOG.debug("First trial failed, node has no type {}, " +
            "making second trial carrying this type", type);
        return chooseRandomWithStorageType(searchScope, excludedScope,
            excludedNodes, type);
      }
    } finally {
      netlock.readLock().unlock();
    }
  }

  /**
   * 按范围、排除范围、排除节点和存储类型精确随机选择节点
   * 采用加权随机算法：根据子树中符合存储类型的节点数量加权，保证选择均匀性
   * 处理排除逻辑：排除范围从计数中扣除对应存储节点数量，排除节点列表逐个扣除符合条件的计数
   * @param scope 搜索节点的范围
   * @param excludedScope 必须排除的范围
   * @param excludedNodes 必须排除的节点列表
   * @param type 要求的存储类型
   * @return 符合所有条件的随机节点，无符合节点则返回null
   */
  @VisibleForTesting
  Node chooseRandomWithStorageType(final String scope,
      String excludedScope, final Collection<Node> excludedNodes,
      StorageType type) {
    if (excludedScope != null) {
      if (isChildScope(scope, excludedScope)) {
        return null;
      }
      if (!isChildScope(excludedScope, scope)) {
        excludedScope = null;
      }
    }
    Node node = getNode(scope);
    if (node == null) {
      LOG.debug("Invalid scope {}, non-existing node", scope);
      return null;
    }
    if (!(node instanceof DFSTopologyNodeImpl)) {
      // 当前节点已是数据节点，检查是否被排除
      if (excludedNodes != null && excludedNodes.contains(node)) {
        LOG.debug("{} in excludedNodes", node);
        return null;
      }
      // 检查存储类型是否符合要求
      return ((DatanodeDescriptor) node).hasStorageType(type) ? node : null;
    }
    DFSTopologyNodeImpl root = (DFSTopologyNodeImpl)node;
    Node excludeRoot = excludedScope == null ? null : getNode(excludedScope);

    // 计算当前范围中符合要求的可用节点总数
    int availableCount = root.getSubtreeStorageCount(type);
    // 扣除排除范围内的符合节点数量
    if (excludeRoot != null && root.isAncestor(excludeRoot)) {
      if (excludeRoot instanceof DFSTopologyNodeImpl) {
        availableCount -= ((DFSTopologyNodeImpl)excludeRoot)
            .getSubtreeStorageCount(type);
      } else {
        availableCount -= ((DatanodeDescriptor)excludeRoot)
            .hasStorageType(type) ? 1 : 0;
      }
    }
    // 扣除排除节点列表中符合条件的节点数量
    if (excludedNodes != null) {
      for (Node excludedNode : excludedNodes) {
        if ((excludeRoot != null && isNodeInScope(excludedNode, excludedScope)) ||
            !isNodeInScope(excludedNode, scope)) {
          continue;
        }
        if (excludedNode instanceof DatanodeDescriptor) {
          availableCount -= ((DatanodeDescriptor) excludedNode)
              .hasStorageType(type) ? 1 : 0;
        } else if (excludedNode instanceof DFSTopologyNodeImpl) {
          availableCount -= ((DFSTopologyNodeImpl) excludedNode)
              .getSubtreeStorageCount(type);
        } else if (excludedNode instanceof DatanodeInfo) {
          // DatanodeInfo需要从拓扑中获取对应DatanodeDescriptor获取存储类型信息
          // 由于排除节点列表通常很小，该操作性能可接受
          String nodeLocation = excludedNode.getNetworkLocation()
              + "/" + excludedNode.getName();
          DatanodeDescriptor dn = (DatanodeDescriptor)getNode(nodeLocation);
          if (dn == null) {
            continue;
          }
          availableCount -= dn.hasStorageType(type)? 1 : 0;
        } else {
          LOG.error("Unexpected node type: {}.", excludedNode.getClass());
        }
      }
    }
    if (availableCount <= 0) {
      // 无可用节点，返回空
      return null;
    }
    // 递归加权随机选择符合要求的节点
    Node chosen =
        chooseRandomWithStorageTypeAndExcludeRoot(root, excludeRoot, type,
            excludedNodes);
    LOG.debug("chooseRandom returning {}", chosen);
    return chosen;
  }

  /**
   * 在指定根节点下，排除指定子树，按存储类型随机选择叶子数据节点
   * 内部递归实现：若当前是机架节点，则直接从子节点中随机选择符合要求的数据节点；
   * 若当前是内部节点，则按子树符合节点数量加权随机选择下一层级，递归搜索直到叶子节点
   * @param root 当前搜索的根节点
   * @param excludeRoot 需要排除的子树根节点
   * @param type 要求的存储类型
   * @param excludedNodes 需要排除的节点列表
   * @return 符合要求的随机数据节点，无符合则返回null
   */
  private Node chooseRandomWithStorageTypeAndExcludeRoot(
      DFSTopologyNodeImpl root, Node excludeRoot, StorageType type,
      Collection<Node> excludedNodes) {
    Node chosenNode;
    if (root.isRack()) {
      // 当前是机架层，子节点均为数据节点，收集所有符合条件的候选节点
      ArrayList<Node> candidates = new ArrayList<>();
      for (Node node : root.getChildren()) {
        if (node.equals(excludeRoot) || (excludedNodes != null && excludedNodes
            .contains(node))) {
          continue;
        }
        DatanodeDescriptor dnDescriptor = (DatanodeDescriptor)node;
        if (dnDescriptor.hasStorageType(type)) {
          candidates.add(node);
        }
      }
      if (candidates.size() == 0) {
        return null;
      }
      // 从候选节点中随机选择一个
      chosenNode = candidates.get(RANDOM.nextInt(candidates.size()));
    } else {
      // 当前是内部层级，收集所有符合条件的子节点
      ArrayList<DFSTopologyNodeImpl> candidates =
          getEligibleChildren(root, excludeRoot, type, excludedNodes);
      if (candidates.size() == 0) {
        return null;
      }
      // 按子树符合节点数量计算总加权计数
      int totalCounts = 0;
      int[] countArray = new int[candidates.size()];
      for (int i = 0; i < candidates.size(); i++) {
        DFSTopologyNodeImpl innerNode = candidates.get(i);
        int subTreeCount = innerNode.getSubtreeStorageCount(type);
        totalCounts += subTreeCount;
        countArray[i] = subTreeCount;
      }
      // 生成[1, totalCounts]区间的随机数，按加权选择子节点
      int randomCounts = RANDOM.nextInt(totalCounts) + 1;
      int idxChosen = 0;
      for (int i = 0; i < countArray.length; i++) {
        if (randomCounts <= countArray[i]) {
          idxChosen = i;
          break;
        }
        randomCounts -= countArray[i];
      }
      DFSTopologyNodeImpl nextRoot = candidates.get(idxChosen);
      // 递归搜索下一层级
      chosenNode = chooseRandomWithStorageTypeAndExcludeRoot(
          nextRoot, excludeRoot, type, excludedNodes);
    }
    return chosenNode;
  }

  /**
   * 获取当前根节点下所有符合存储类型要求的子节点，处理排除逻辑修正计数
   * @param root 当前检查的子树根节点
   * @param excludeRoot 需要排除的子树根节点
   * @param type 要求的存储类型
   * @param excludedNodes 需要排除的节点列表
   * @return 所有符合条件（可用计数大于0）的子节点列表
   */
  private ArrayList<DFSTopologyNodeImpl> getEligibleChildren(
      DFSTopologyNodeImpl root, Node excludeRoot, StorageType type,
      Collection<Node> excludedNodes) {
    ArrayList<DFSTopologyNodeImpl> candidates = new ArrayList<>();
    int excludeCount = 0;
    if (excludeRoot != null && root.isAncestor(excludeRoot)) {
      // 计算排除子树中符合存储类型的节点总数
      if (excludeRoot instanceof DFSTopologyNodeImpl) {
        excludeCount = ((DFSTopologyNodeImpl) excludeRoot)
            .getSubtreeStorageCount(type);
      } else {
        if (((DatanodeDescriptor) excludeRoot).hasStorageType(type)) {
          excludeCount = 1;
        }
      }
    }
    // 遍历所有子节点检查 eligibility
    for (Node node : root.getChildren()) {
      DFSTopologyNodeImpl dfsNode = (DFSTopologyNodeImpl) node;
      int storageCount = dfsNode.getSubtreeStorageCount(type);
      // 如果当前子节点包含排除根，扣除排除计数
      if (excludeRoot != null && excludeCount != 0 &&
          (dfsNode.isAncestor(excludeRoot) || dfsNode.equals(excludeRoot))) {
        storageCount -= excludeCount;
      }
      // 扣除排除节点列表中在当前子树下的符合节点数量
      if (excludedNodes != null) {
        for (Node excludedNode : excludedNodes) {
          if (excludeRoot != null && isNodeInScope(excludedNode,
              NodeBase.getPath(excludeRoot))) {
            continue;
          }
          if (isNodeInScope(excludedNode, NodeBase.getPath(node))) {
            if (excludedNode instanceof DatanodeDescriptor) {
              storageCount -=
                  ((DatanodeDescriptor) excludedNode).hasStorageType(type) ?
                      1 : 0;
            } else if (excludedNode instanceof DFSTopologyNodeImpl) {
              storageCount -= ((DFSTopologyNodeImpl) excludedNode)
                  .getSubtreeStorageCount(type);
            }
          }
        }
      }
      // 只有可用计数大于0的子节点才加入候选列表
      if (storageCount > 0) {
        candidates.add(dfsNode);
      }
    }
    return candidates;
  }
}