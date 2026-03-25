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

import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.NetworkTopologyWithNodeGroup;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;

/**
 * 带节点组层级的块放置策略实现，用于拥有节点分组（如机架下的可用区/机房分组）的集群环境
 * 副本放置规则调整为：
 * 1. 如果写入者在数据节点上，第一个副本放在本地节点，否则回退到本地节点组、本地机架，最后随机选择
 * 2. 第二个副本放在与第一个副本不同的机架上
 * 3. 第三个副本放在与第二个副本相同机架、但不同节点组上
 * 核心目标是保证在多可用区架构下，副本分散在不同节点组，提升容灾能力
 */
public class BlockPlacementPolicyWithNodeGroup extends BlockPlacementPolicyDefault {

  protected BlockPlacementPolicyWithNodeGroup() {
  }

  /**
   * 初始化带节点组的块放置策略，校验集群拓扑类型必须支持节点组
   */
  @Override
  public void initialize(Configuration conf,  FSClusterStats stats,
          NetworkTopology clusterMap, 
          Host2NodesMap host2datanodeMap) {
    if (!(clusterMap instanceof NetworkTopologyWithNodeGroup)) {
      throw new IllegalArgumentException(
          "Configured cluster topology should be "
              + NetworkTopologyWithNodeGroup.class.getName());
    }
    super.initialize(conf, stats, clusterMap, host2datanodeMap);
  }

  /**
   * 优先选择偏好节点作为块副本放置目标，如果偏好节点不足，则从未选中偏好节点的节点组中选择补充节点
   * @throws NotEnoughReplicasException
   */
  @Override
  protected void chooseFavouredNodes(String src, int numOfReplicas,
      List<DatanodeDescriptor> favoredNodes,
      Set<Node> favoriteAndExcludedNodes, long blocksize,
      int maxNodesPerRack, List<DatanodeStorageInfo> results,
      boolean avoidStaleNodes, EnumMap<StorageType, Integer> storageTypes)
      throws NotEnoughReplicasException {
    super.chooseFavouredNodes(src, numOfReplicas, favoredNodes,
        favoriteAndExcludedNodes, blocksize, maxNodesPerRack, results,
        avoidStaleNodes, storageTypes);
    if (results.size() < numOfReplicas) {
      // 已选节点不足，遍历未选中的偏好节点，从其所在节点组补充选择
      for (int i = 0;
          i < favoredNodes.size() && results.size() < numOfReplicas; i++) {
        DatanodeDescriptor favoredNode = favoredNodes.get(i);
        boolean chosenNode =
            isNodeChosen(results, favoredNode);
        if (chosenNode) {
          continue;
        }
        NetworkTopologyWithNodeGroup clusterMapNodeGroup =
            (NetworkTopologyWithNodeGroup) clusterMap;
        // 尝试从当前偏好节点所在节点组选择一个可用节点
        DatanodeStorageInfo target = null;
        String scope =
            clusterMapNodeGroup.getNodeGroup(favoredNode.getNetworkLocation());
        try {
          target =
              chooseRandom(scope, favoriteAndExcludedNodes, blocksize,
                maxNodesPerRack, results, avoidStaleNodes, storageTypes);
        } catch (NotEnoughReplicasException e) {
          // 当前节点组选不到，继续尝试下一个偏好节点的节点组
          continue;
        }
        if (target == null) {
          LOG.warn("Could not find a target for file "
              + src + " within nodegroup of favored node " + favoredNode);
          continue;
        }
        favoriteAndExcludedNodes.add(target.getDatanodeDescriptor());
      }
    }
  }

  /**
   * 判断指定偏好节点是否已经被选中为放置目标
   */
  private boolean isNodeChosen(
      List<DatanodeStorageInfo> results, DatanodeDescriptor favoredNode) {
    boolean chosenNode = false;
    for (int j = 0; j < results.size(); j++) {
      if (results.get(j).getDatanodeDescriptor().equals(favoredNode)) {
        chosenNode = true;
        break;
      }
    }
    return chosenNode;
  }

  /**
   * 优先选择本地节点存储作为第一个副本放置目标
   * 如果本地节点不可用且开启回退开关，则依次回退到本地节点组、本地机架选择
   * @return 选中的存储信息，选不到返回null
   */
  @Override
  protected DatanodeStorageInfo chooseLocalStorage(Node localMachine,
      Set<Node> excludedNodes, long blocksize, int maxNodesPerRack,
      List<DatanodeStorageInfo> results, boolean avoidStaleNodes,
      EnumMap<StorageType, Integer> storageTypes,
      boolean fallbackToNodeGroupAndLocalRack)
      throws NotEnoughReplicasException {
    DatanodeStorageInfo localStorage = chooseLocalStorage(localMachine,
        excludedNodes, blocksize, maxNodesPerRack, results,
        avoidStaleNodes, storageTypes);
    if (localStorage != null) {
      return localStorage;
    }

    if (!fallbackToNodeGroupAndLocalRack) {
      return null;
    }
    // 尝试选择本地节点组内的节点
    DatanodeStorageInfo chosenStorage = chooseLocalNodeGroup(
        (NetworkTopologyWithNodeGroup)clusterMap, localMachine, excludedNodes, 
        blocksize, maxNodesPerRack, results, avoidStaleNodes, storageTypes);
    if (chosenStorage != null) {
      return chosenStorage;
    }
    // 本地节点组也选不到，尝试选择本地机架内的节点
    return chooseLocalRack(localMachine, excludedNodes, 
        blocksize, maxNodesPerRack, results, avoidStaleNodes, storageTypes);
  }

  /**
   * 从已选结果中获取第二个副本对应的数据节点（排除本地节点）
   * @return 第二个副本节点，找不到返回null
   */
  private static DatanodeDescriptor secondNode(Node localMachine,
      List<DatanodeStorageInfo> results) {
    // 遍历找到非本地的第二个副本
    for(DatanodeStorageInfo nextStorage : results) {
      DatanodeDescriptor nextNode = nextStorage.getDatanodeDescriptor();
      if (nextNode != localMachine) {
        return nextNode;
      }
    }
    return null;
  }

  /**
   * 在本地机架范围内选择一个不在本地节点组的节点作为放置目标
   */
  @Override
  protected DatanodeStorageInfo chooseLocalRack(Node localMachine,
      Set<Node> excludedNodes, long blocksize, int maxNodesPerRack,
      List<DatanodeStorageInfo> results, boolean avoidStaleNodes,
      EnumMap<StorageType, Integer> storageTypes) throws
      NotEnoughReplicasException {
    // 无本地机器信息，全局随机选择
    if (localMachine == null) {
      return chooseRandom(NodeBase.ROOT, excludedNodes, blocksize,
          maxNodesPerRack, results, avoidStaleNodes, storageTypes);
    }

    // 优先在本地机架（剔除本地节点组）范围内随机选择
    try {
      final String scope = NetworkTopology.getFirstHalf(localMachine.getNetworkLocation());
      return chooseRandom(scope, excludedNodes, blocksize, maxNodesPerRack,
          results, avoidStaleNodes, storageTypes);
    } catch (NotEnoughReplicasException e1) {
      // 本地机架找不到，获取已选的第二个副本，尝试在第二个副本的机架选择
      final DatanodeDescriptor newLocal = secondNode(localMachine, results);
      if (newLocal != null) {
        try {
          return chooseRandom(
              clusterMap.getRack(newLocal.getNetworkLocation()), excludedNodes,
              blocksize, maxNodesPerRack, results, avoidStaleNodes,
              storageTypes);
        } catch(NotEnoughReplicasException e2) {
          // 仍然找不到，全局随机选择
          return chooseRandom(NodeBase.ROOT, excludedNodes, blocksize,
              maxNodesPerRack, results, avoidStaleNodes, storageTypes);
        }
      } else {
        // 没有已选第二个副本，全局随机选择
        return chooseRandom(NodeBase.ROOT, excludedNodes, blocksize,
            maxNodesPerRack, results, avoidStaleNodes, storageTypes);
      }
    }
  }

  /**
   * 在远端机架选择指定数量的节点作为副本放置目标
   */
  @Override
  protected void chooseRemoteRack(int numOfReplicas,
      DatanodeDescriptor localMachine, Set<Node> excludedNodes,
      long blocksize, int maxReplicasPerRack, List<DatanodeStorageInfo> results,
      boolean avoidStaleNodes, EnumMap<StorageType, Integer> storageTypes)
      throws NotEnoughReplicasException {
    int oldNumOfReplicas = results.size();

    final String rackLocation = NetworkTopology.getFirstHalf(
        localMachine.getNetworkLocation());
    try {
      // 优先在除本地机架外的远端机架随机选择
      chooseRandom(numOfReplicas, "~" + rackLocation, excludedNodes, blocksize,
          maxReplicasPerRack, results, avoidStaleNodes, storageTypes);
    } catch (NotEnoughReplicasException e) {
      // 远端机架节点不足，回退到本地机架选择剩余需要的节点
      chooseRandom(numOfReplicas - (results.size() - oldNumOfReplicas),
          rackLocation, excludedNodes, blocksize,
          maxReplicasPerRack, results, avoidStaleNodes, storageTypes);
    }
  }

  /**
   * 在本地节点组范围内选择节点，如果本地节点组找不到则尝试第二个副本的节点组
   * @return 选中的存储信息，选不到返回null
   */
  private DatanodeStorageInfo chooseLocalNodeGroup(
      NetworkTopologyWithNodeGroup clusterMap, Node localMachine,
      Set<Node> excludedNodes, long blocksize, int maxNodesPerRack,
      List<DatanodeStorageInfo> results, boolean avoidStaleNodes,
      EnumMap<StorageType, Integer> storageTypes) throws
      NotEnoughReplicasException {
    // 无本地机器信息，全局随机选择
    if (localMachine == null) {
      return chooseRandom(NodeBase.ROOT, excludedNodes, blocksize,
          maxNodesPerRack, results, avoidStaleNodes, storageTypes);
    }

    // 优先在本地节点组随机选择
    try {
      return chooseRandom(
          clusterMap.getNodeGroup(localMachine.getNetworkLocation()),
          excludedNodes, blocksize, maxNodesPerRack, results, avoidStaleNodes,
          storageTypes);
    } catch (NotEnoughReplicasException e1) {
      // 本地节点组找不到，获取已选的第二个副本，尝试在第二个副本的节点组选择
      final DatanodeDescriptor newLocal = secondNode(localMachine, results);
      if (newLocal != null) {
        try {
          return chooseRandom(
              clusterMap.getNodeGroup(newLocal.getNetworkLocation()),
              excludedNodes, blocksize, maxNodesPerRack, results,
              avoidStaleNodes, storageTypes);
        } catch(NotEnoughReplicasException e2) {
          // 仍然找不到，返回null
          return null;
        }
      } else {
        // 没有已选第二个副本，返回null
        return null;
      }
    }
  }

  @Override
  protected String getRack(final DatanodeInfo cur) {
    String nodeGroupString = cur.getNetworkLocation();
    return NetworkTopology.getFirstHalf(nodeGroupString);
  }
  
  /**
   * 将选中节点所在节点组的所有节点都加入排除列表，避免同一个节点组内放置多个副本
   * @return 新加入排除列表的节点数量
   */
  @Override
  protected int addToExcludedNodes(DatanodeDescriptor chosenNode,
      Set<Node> excludedNodes) {
    int countOfExcludedNodes = 0;
    String nodeGroupScope = chosenNode.getNetworkLocation();
    List<Node> leafNodes = clusterMap.getLeaves(nodeGroupScope);
    for (Node leafNode : leafNodes) {
      if (excludedNodes.add(leafNode)) {
        // 节点原来不在排除列表，计数+1
        countOfExcludedNodes++;
      }
    }
    
    countOfExcludedNodes += addDependentNodesToExcludedNodes(
        chosenNode, excludedNodes);
    return countOfExcludedNodes;
  }
  
  /**
   * 将选中节点的所有依赖节点加入排除列表
   * @return 新加入排除列表的节点数量
   */
  private int addDependentNodesToExcludedNodes(DatanodeDescriptor chosenNode,
      Set<Node> excludedNodes) {
    if (this.host2datanodeMap == null) {
      return 0;
    }
    int countOfExcludedNodes = 0;
    for(String hostname : chosenNode.getDependentHostNames()) {
      DatanodeDescriptor node =
          this.host2datanodeMap.getDataNodeByHostName(hostname);
      if(node!=null) {
        if (excludedNodes.add(node)) {
          countOfExcludedNodes++;
        }
      } else {
        LOG.warn("Not able to find datanode " + hostname
            + " which has dependency with datanode "
            + chosenNode.getHostName());
      }
    }
    
    return countOfExcludedNodes;
  }

  /**
   * 从过度复制的副本集合中选择需要删除的副本集合
   * 优先删除同一个节点组内存在多个副本的节点组中的副本，提升副本容灾分布
   * @return 选中的待删除副本集合
   */
  @Override
  public Collection<DatanodeStorageInfo> pickupReplicaSet(
      Collection<DatanodeStorageInfo> first,
      Collection<DatanodeStorageInfo> second,
      Map<String, List<DatanodeStorageInfo>> rackMap) {
    // 同机架没有多余副本，直接返回剩余集合
    if (first.isEmpty()) {
      return second;
    }
    // 按节点组分组，用于判断哪些节点组存在多个副本
    Map<String, List<DatanodeStorageInfo>> nodeGroupMap = new HashMap<>();
    
    for(DatanodeStorageInfo storage : first) {
      final String nodeGroupName = NetworkTopology.getLastHalf(
          storage.getDatanodeDescriptor().getNetworkLocation());
      List<DatanodeStorageInfo> storageList = nodeGroupMap.get(nodeGroupName);
      if (storageList == null) {
        storageList = new ArrayList<>();
        nodeGroupMap.put(nodeGroupName, storageList);
      }
      storageList.add(storage);
    }
    
    final List<DatanodeStorageInfo> moreThanOne = new ArrayList<>();
    final List<DatanodeStorageInfo> exactlyOne = new ArrayList<>();
    // 将节点组分为有多个副本和只有一个副本两类
    for(List<DatanodeStorageInfo> datanodeList : nodeGroupMap.values()) {
      if (datanodeList.size() == 1 ) {
        // 节点组内只有一个副本
        exactlyOne.add(datanodeList.get(0));
      } else {
        // 节点组内有多个副本，优先删除这里的副本
        moreThanOne.addAll(datanodeList);
      }
    }
    
    return moreThanOne.isEmpty()? exactlyOne : moreThanOne;
  }

  /**
   * 检查目标节点是否适合作为块迁移的目标节点
   * 不允许目标节点和已有任意副本（除源节点外）处于同一个节点组
   * @return 如果目标节点与已有副本同节点组则返回false（不可移动），否则返回true
   */
  @Override
  public boolean isMovable(Collection<DatanodeInfo> locs,
      DatanodeInfo source, DatanodeInfo target) {
    for (DatanodeInfo dn : locs) {
      if (dn != source && dn != target &&
          clusterMap.isOnSameNodeGroup(dn, target)) {
        return false;
      }
    }
    return true;
  }


  /**
   * 验证块放置是否满足节点组分散的放置策略，返回放置状态报告
   */
  @Override
  public BlockPlacementStatus verifyBlockPlacement(DatanodeInfo[] locs,
      int numberOfReplicas)