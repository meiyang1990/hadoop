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

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_AVAILABLE_SPACE_BLOCK_PLACEMENT_POLICY_BALANCED_SPACE_PREFERENCE_FRACTION_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_AVAILABLE_SPACE_BLOCK_PLACEMENT_POLICY_BALANCED_SPACE_PREFERENCE_FRACTION_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_AVAILABLE_SPACE_BLOCK_PLACEMENT_POLICY_BALANCED_SPACE_TOLERANCE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_AVAILABLE_SPACE_BLOCK_PLACEMENT_POLICY_BALANCED_SPACE_TOLERANCE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_AVAILABLE_SPACE_BLOCK_PLACEMENT_POLICY_BALANCED_SPACE_TOLERANCE_LIMIT_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_AVAILABLE_SPACE_BLOCK_PLACEMENT_POLICY_BALANCED_SPACE_TOLERANCE_LIMIT_DEFAULT;

import java.util.Collection;
import java.util.EnumMap;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.hadoop.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.net.DFSNetworkTopology;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;

/**
 * 基于可用空间均衡的HDFS块放置策略
 * 相比默认策略更关注数据节点剩余空间均衡，避免部分节点数据过度倾斜
 * 继承默认块放置策略，在保持原有拓扑位置规则的基础上，增加空间均衡选择逻辑
 */
public class AvailableSpaceBlockPlacementPolicy extends
    BlockPlacementPolicyDefault {
  private static final Logger LOG = LoggerFactory
      .getLogger(AvailableSpaceBlockPlacementPolicy.class);
  private static final Random RAND = new Random();
  private int balancedPreference =
      (int) (100 * DFS_NAMENODE_AVAILABLE_SPACE_BLOCK_PLACEMENT_POLICY_BALANCED_SPACE_PREFERENCE_FRACTION_DEFAULT);
  private int balancedSpaceTolerance =
      DFS_NAMENODE_AVAILABLE_SPACE_BLOCK_PLACEMENT_POLICY_BALANCED_SPACE_TOLERANCE_DEFAULT;

  private int balancedSpaceToleranceLimit =
      DFS_NAMENODE_AVAILABLE_SPACE_BLOCK_PLACEMENT_POLICY_BALANCED_SPACE_TOLERANCE_LIMIT_DEFAULT;
  private boolean optimizeLocal;

  /**
   * 初始化块放置策略，从配置加载空间均衡相关参数并做合法性校验
   * @param conf Hadoop配置对象
   * @param stats 集群统计信息
   * @param clusterMap 网络拓扑结构
   * @param host2datanodeMap 主机到数据节点的映射
   */
  @Override
  public void initialize(Configuration conf, FSClusterStats stats,
      NetworkTopology clusterMap, Host2NodesMap host2datanodeMap) {
    super.initialize(conf, stats, clusterMap, host2datanodeMap);
    // 读取空间均衡偏好比例配置
    float balancedPreferencePercent =
        conf.getFloat(
        DFS_NAMENODE_AVAILABLE_SPACE_BLOCK_PLACEMENT_POLICY_BALANCED_SPACE_PREFERENCE_FRACTION_KEY,
        DFS_NAMENODE_AVAILABLE_SPACE_BLOCK_PLACEMENT_POLICY_BALANCED_SPACE_PREFERENCE_FRACTION_DEFAULT);

    LOG.info("Available space block placement policy initialized: "
        + DFS_NAMENODE_AVAILABLE_SPACE_BLOCK_PLACEMENT_POLICY_BALANCED_SPACE_PREFERENCE_FRACTION_KEY
        + " = " + balancedPreferencePercent);

    // 读取空间差异容忍度配置
    balancedSpaceTolerance =
        conf.getInt(
        DFS_NAMENODE_AVAILABLE_SPACE_BLOCK_PLACEMENT_POLICY_BALANCED_SPACE_TOLERANCE_KEY,
        DFS_NAMENODE_AVAILABLE_SPACE_BLOCK_PLACEMENT_POLICY_BALANCED_SPACE_TOLERANCE_DEFAULT);

    // 读取整体使用率上限阈值配置
    balancedSpaceToleranceLimit =
      conf.getInt(
      DFS_NAMENODE_AVAILABLE_SPACE_BLOCK_PLACEMENT_POLICY_BALANCED_SPACE_TOLERANCE_LIMIT_KEY,
      DFS_NAMENODE_AVAILABLE_SPACE_BLOCK_PLACEMENT_POLICY_BALANCED_SPACE_TOLERANCE_LIMIT_DEFAULT);

    // 读取是否开启本节点空间均衡优化配置
    optimizeLocal = conf.getBoolean(
        DFSConfigKeys.DFS_NAMENODE_AVAILABLE_SPACE_BLOCK_PLACEMENT_POLICY_BALANCE_LOCAL_NODE_KEY,
        DFSConfigKeys.DFS_NAMENODE_AVAILABLE_SPACE_BLOCK_PLACEMENT_POLICY_BALANCE_LOCAL_NODE_DEFAULT);

    // 偏好比例配置合法性检查和警告
    if (balancedPreferencePercent > 1.0) {
      LOG.warn("The value of "
          + DFS_NAMENODE_AVAILABLE_SPACE_BLOCK_PLACEMENT_POLICY_BALANCED_SPACE_PREFERENCE_FRACTION_KEY
          + " is greater than 1.0 but should be in the range 0.0 - 1.0");
    }
    if (balancedPreferencePercent < 0.5) {
      LOG.warn("The value of "
          + DFS_NAMENODE_AVAILABLE_SPACE_BLOCK_PLACEMENT_POLICY_BALANCED_SPACE_PREFERENCE_FRACTION_KEY
          + " is less than 0.5 so datanodes with more used percent will"
          + " receive  more block allocations.");
    }

    // 整体使用率上限阈值合法性检查，非法则使用默认值
    if (balancedSpaceToleranceLimit > 100 || balancedSpaceToleranceLimit < 0) {
      LOG.warn("The value of "
          + DFS_NAMENODE_AVAILABLE_SPACE_BLOCK_PLACEMENT_POLICY_BALANCED_SPACE_TOLERANCE_LIMIT_KEY
          + " is invalid, Current value is " + balancedSpaceToleranceLimit + ", Default value "
          + DFS_NAMENODE_AVAILABLE_SPACE_BLOCK_PLACEMENT_POLICY_BALANCED_SPACE_TOLERANCE_LIMIT_DEFAULT
          + " will be used instead.");

      balancedSpaceToleranceLimit =
          DFS_NAMENODE_AVAILABLE_SPACE_BLOCK_PLACEMENT_POLICY_BALANCED_SPACE_TOLERANCE_LIMIT_DEFAULT;
    }

    // 空间差异容忍度合法性检查，非法则使用默认值
    if (balancedSpaceTolerance > 20 || balancedSpaceTolerance < 0) {
      LOG.warn("The value of "
          + DFS_NAMENODE_AVAILABLE_SPACE_BLOCK_PLACEMENT_POLICY_BALANCED_SPACE_TOLERANCE_KEY
          + " is invalid, Current value is " + balancedSpaceTolerance + ", Default value " +
            DFS_NAMENODE_AVAILABLE_SPACE_BLOCK_PLACEMENT_POLICY_BALANCED_SPACE_TOLERANCE_DEFAULT
          + " will be used instead.");
      balancedSpaceTolerance =
              DFS_NAMENODE_AVAILABLE_SPACE_BLOCK_PLACEMENT_POLICY_BALANCED_SPACE_TOLERANCE_DEFAULT;
    }
    // 将比例转换为百分比整数存储，用于随机判断
    balancedPreference = (int) (100 * balancedPreferencePercent);
  }

  /**
   * 根据指定存储类型选择数据节点，在随机选择的两个节点中按空间均衡规则选择
   * @param scope 网络拓扑范围
   * @param excludedNode 需要排除的节点集合
   * @param type 存储类型
   * @return 选中的数据节点描述符
   */
  @Override
  protected DatanodeDescriptor chooseDataNode(final String scope,
      final Collection<Node> excludedNode, StorageType type) {
    // 仅DFSNetworkTopology会进入该流程，做合法性检查
    Preconditions.checkArgument(clusterMap instanceof DFSNetworkTopology);
    DFSNetworkTopology dfsClusterMap = (DFSNetworkTopology)clusterMap;
    // 两次随机选择符合存储类型的节点
    DatanodeDescriptor a = (DatanodeDescriptor) dfsClusterMap
        .chooseRandomWithStorageTypeTwoTrial(scope, excludedNode, type);
    DatanodeDescriptor b = (DatanodeDescriptor) dfsClusterMap
        .chooseRandomWithStorageTypeTwoTrial(scope, excludedNode, type);
    // 按空间均衡规则从两个节点中选择一个
    return select(a, b, false);
  }

  /**
   * 选择本地节点存储，开启优化后会比较本节点和同机架节点空间，选择更均衡的节点
   * @param localMachine 本地机器节点
   * @param excludedNodes 需要排除的节点集合
   * @param blocksize 块大小
   * @param maxNodesPerRack 每个机架最大节点数
   * @param results 已选择的存储结果列表
   * @param avoidStaleNodes 是否避免过期节点
   * @param storageTypes 所需存储类型计数
   * @param fallbackToLocalRack 本节点无可用存储时是否回退到同机架
   * @return 选中的存储信息，无可用返回null
   * @throws NotEnoughReplicasException 当没有足够副本可用时抛出
   */
  @Override
  protected DatanodeStorageInfo chooseLocalStorage(Node localMachine,
      Set<Node> excludedNodes, long blocksize, int maxNodesPerRack,
      List<DatanodeStorageInfo> results, boolean avoidStaleNodes,
      EnumMap<StorageType, Integer> storageTypes, boolean fallbackToLocalRack)
      throws NotEnoughReplicasException {
    // 未开启本地优化则使用父类默认逻辑
    if (!optimizeLocal) {
      return super.chooseLocalStorage(localMachine, excludedNodes, blocksize,
          maxNodesPerRack, results, avoidStaleNodes, storageTypes,
          fallbackToLocalRack);
    }
    // 分别保存尝试本节点和同机架前的存储类型状态
    final EnumMap<StorageType, Integer> initialStorageTypesLocal =
        storageTypes.clone();
    final EnumMap<StorageType, Integer> initialStorageTypesLocalRack =
        storageTypes.clone();
    // 尝试选择本节点存储
    DatanodeStorageInfo local =
        chooseLocalStorage(localMachine, excludedNodes, blocksize,
            maxNodesPerRack, results, avoidStaleNodes,
            initialStorageTypesLocal);
    // 不需要回退则直接返回本节点结果
    if (!fallbackToLocalRack) {
      return local;
    }
    // 移除本节点临时结果，准备和同机架节点比较
    if (local != null) {
      results.remove(local);
    }
    // 尝试选择同机架存储
    DatanodeStorageInfo localRack =
        chooseLocalRack(localMachine, excludedNodes, blocksize, maxNodesPerRack,
            results, avoidStaleNodes, initialStorageTypesLocalRack);
    // 两个节点都可用，比较后选择更均衡的节点
    if (local != null && localRack != null) {
      if (select(local.getDatanodeDescriptor(),
          localRack.getDatanodeDescriptor(), true) == local
          .getDatanodeDescriptor()) {
        // 选择本节点，回滚存储类型计数，添加本节点到结果
        results.remove(localRack);
        results.add(local);
        swapStorageTypes(initialStorageTypesLocal, storageTypes);
        excludedNodes.remove(localRack.getDatanodeDescriptor());
        return local;
      } else {
        // 选择同机架节点，回滚存储类型计数，添加同机架节点到结果
        swapStorageTypes(initialStorageTypesLocalRack, storageTypes);
        excludedNodes.remove(local.getDatanodeDescriptor());
        return localRack;
      }
    } else if (localRack == null && local != null) {
      // 只有本节点可用，添加本节点到结果返回
      results.add(local);
      swapStorageTypes(initialStorageTypesLocal, storageTypes);
      return local;
    } else {
      // 只有同机架节点可用，直接返回同机架结果
      swapStorageTypes(initialStorageTypesLocalRack, storageTypes);
      return localRack;
    }
  }

  /**
   * 将源存储类型计数覆盖到目标对象，用于回滚计数状态
   * @param fromStorageTypes 源存储类型计数
   * @param toStorageTypes 目标存储类型计数
   */
  private void swapStorageTypes(EnumMap<StorageType, Integer> fromStorageTypes,
      EnumMap<StorageType, Integer> toStorageTypes) {
    toStorageTypes.clear();
    toStorageTypes.putAll(fromStorageTypes);
  }

  /**
   * 不指定存储类型选择数据节点，在随机选择的两个节点中按空间均衡规则选择
   * @param scope 网络拓扑范围
   * @param excludedNode 需要排除的节点集合
   * @return 选中的数据节点描述符
   */
  @Override
  protected DatanodeDescriptor chooseDataNode(final String scope,
      final Collection<Node> excludedNode) {
    // 两次随机选择节点
    DatanodeDescriptor a =
        (DatanodeDescriptor) clusterMap.chooseRandom(scope, excludedNode);
    DatanodeDescriptor b =
        (DatanodeDescriptor) clusterMap.chooseRandom(scope, excludedNode);
    // 按空间均衡规则选择
    return select(a, b, false);
  }

  /**
   * 从两个候选数据节点中按空间均衡规则选择一个节点
   * @param a 第一个候选节点
   * @param b 第二个候选节点
   * @param isBalanceLocal 是否是本节点均衡场景
   * @return 选中的数据节点描述符
   */
  private DatanodeDescriptor select(DatanodeDescriptor a, DatanodeDescriptor b,
      boolean isBalanceLocal) {
    // 两个节点都可用，比较后按结果选择
    if (a != null && b != null){
      int ret = compareDataNode(a, b, isBalanceLocal);
      if (ret == 0) {
        return a;
      } else if (ret < 0) {
        // a使用率更低，按偏好概率选择a，否则选择b
        return (RAND.nextInt(100) < balancedPreference) ? a : b;
      } else {
        // b使用率更低，按偏好概率选择b，否则选择a
        return (RAND.nextInt(100) < balancedPreference) ? b : a;
      }
    } else {
      // 只有一个节点可用，返回非空节点
      return a == null ? b : a;
    }
  }

  /**
   * 比较两个数据节点的磁盘使用率，按空间均衡规则判断哪个更适合放置块
   * @param a 第一个数据节点
   * @param b 第二个数据节点
   * @param isBalanceLocal 是否是本节点均衡场景
   * @return 负数表示a更优，正数表示b更优，0表示无差异
   */
  protected int compareDataNode(final DatanodeDescriptor a,
      final DatanodeDescriptor b, boolean isBalanceLocal) {

    // 判断两个节点最高使用率是否都低于整体阈值，低于则整体空间充足
    boolean toleranceLimit = Math.max(a.getDfsUsedPercent(), b.getDfsUsedPercent())
        < balancedSpaceToleranceLimit;
    // 满足任意条件视为两个节点无差异：节点相同、整体空间充足且差异小于容忍度、本节点场景且a使用率低于50%
    if (a.equals(b)
        || (toleranceLimit && Math.abs(a.getDfsUsedPercent() - b.getDfsUsedPercent())
            < balancedSpaceTolerance) || ((
        isBalanceLocal && a.getDfsUsedPercent() < 50))) {
      return 0;
    }
    // 返回比较结果，使用率低的更优
    return a.getDfsUsedPercent() < b.getDfsUsedPercent() ? -1 : 1;
  }
}