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
import org.apache.hadoop.classification.InterfaceStability;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.net.NetworkTopology;

/**
 * @file org/apache/hadoop/hdfs/server/blockmanagement/BlockPlacementPolicyWithUpgradeDomain.java
 * @description 支持升级域约束的块副本放置策略，保障升级过程中数据可用性，要求同一数据块的多个副本分布在不同升级域中
 * 
 * 该类是默认块放置策略的扩展，在原有机架感知放置规则的基础上，增加了升级域唯一性约束：
 * 1. 若写入端在DataNode上，第一个副本放在本地节点，否则随机选择节点
 * 2. 第二个副本放在与第一个不同的机架
 * 3. 第三个副本放在与第二个不同机架的节点
 * 4. 所有三个副本必须分配到不同的升级域，以便在滚动升级重启节点时不会导致数据不可用
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class BlockPlacementPolicyWithUpgradeDomain extends
    BlockPlacementPolicyDefault {

  /** 升级域因子，控制副本需要分布到不同升级域的数量阈值 */
  private int upgradeDomainFactor;

  /**
   * 初始化升级域感知块放置策略，读取配置参数
   * @param conf 配置对象
   * @param stats 集群统计信息
   * @param clusterMap 网络拓扑结构
   * @param host2datanodeMap 主机到DataNode映射
   */
  @Override
  public void initialize(Configuration conf,  FSClusterStats stats,
      NetworkTopology clusterMap, Host2NodesMap host2datanodeMap) {
    super.initialize(conf, stats, clusterMap, host2datanodeMap);
    // 从配置读取升级域因子，使用默认值作为兜底
    upgradeDomainFactor = conf.getInt(
        DFSConfigKeys.DFS_UPGRADE_DOMAIN_FACTOR,
        DFSConfigKeys.DFS_UPGRADE_DOMAIN_FACTOR_DEFAULT);
  }

  /**
   * 检查候选DataNode是否满足放置要求，除原有检查外额外增加升级域唯一性约束
   * @param node 候选DataNode
   * @param maxTargetPerRack 每个机架最大节点数
   * @param considerLoad 是否考虑节点负载
   * @param results 已选存储列表
   * @param avoidStaleNodes 是否规避 stale 节点
   * @return 是否合格
   */
  @Override
  protected boolean isGoodDatanode(DatanodeDescriptor node,
      int maxTargetPerRack, boolean considerLoad,
      List<DatanodeStorageInfo> results, boolean avoidStaleNodes) {
    // 先执行默认策略的检查
    boolean isGoodTarget = super.isGoodDatanode(node,
        maxTargetPerRack, considerLoad, results, avoidStaleNodes);
    if (isGoodTarget) {
      // 已选副本数大于0且小于升级域因子时，需要检查升级域唯一性
      if (results.size() > 0 && results.size() < upgradeDomainFactor) {
        // 获取已选副本的升级域集合
        Set<String> upgradeDomains = getUpgradeDomains(results);
        // 如果候选节点升级域已经存在，则拒绝该节点
        if (upgradeDomains.contains(node.getUpgradeDomain())) {
          isGoodTarget = false;
          logNodeIsNotChosen(node, NodeNotChosenReason.NODE_NOT_CONFORM_TO_UD,
              "(The node's upgrade domain: " + node.getUpgradeDomain() +
                  " is already chosen)");
        }
      }
    }
    return isGoodTarget;
  }

  /**
   * 获取DataNode的升级域，如果未配置则使用传输地址作为兜底值
   * 该兜底用于测试场景：未定义升级域但启用了升级域放置策略的情况
   * @param datanodeInfo DataNode信息
   * @return 升级域名称
   */
  public String getUpgradeDomainWithDefaultValue(DatanodeInfo datanodeInfo) {
    String upgradeDomain = datanodeInfo.getUpgradeDomain();
    if (upgradeDomain == null) {
      LOG.warn("Upgrade domain isn't defined for " + datanodeInfo);
      upgradeDomain = datanodeInfo.getXferAddr();
    }
    return upgradeDomain;
  }

  /**
   * 获取指定存储对应DataNode的升级域
   * @param storage 存储信息
   * @return 升级域名称
   */
  private String getUpgradeDomain(DatanodeStorageInfo storage) {
    return getUpgradeDomainWithDefaultValue(storage.getDatanodeDescriptor());
  }

  /**
   * 从已选存储列表提取所有升级域
   * @param results 已选存储列表
   * @return 升级域集合
   */
  private Set<String> getUpgradeDomains(List<DatanodeStorageInfo> results) {
    Set<String> upgradeDomains = new HashSet<>();
    if (results == null) {
      return upgradeDomains;
    }
    for(DatanodeStorageInfo storageInfo : results) {
      upgradeDomains.add(getUpgradeDomain(storageInfo));
    }
    return upgradeDomains;
  }

  /**
   * 从DataNode数组提取所有升级域
   * @param nodes DataNode数组
   * @return 升级域集合
   */
  private Set<String> getUpgradeDomainsFromNodes(DatanodeInfo[] nodes) {
    Set<String> upgradeDomains = new HashSet<>();
    if (nodes == null) {
      return upgradeDomains;
    }
    for(DatanodeInfo node : nodes) {
      upgradeDomains.add(getUpgradeDomainWithDefaultValue(node));
    }
    return upgradeDomains;
  }

  /**
   * 按升级域对存储或DataNode分组
   * @param storagesOrDataNodes 存储或DataNode集合
   * @return 升级域到对应元素列表的映射
   */
  private <T> Map<String, List<T>> getUpgradeDomainMap(
      Collection<T> storagesOrDataNodes) {
    Map<String, List<T>> upgradeDomainMap = new HashMap<>();
    for(T storage : storagesOrDataNodes) {
      String upgradeDomain = getUpgradeDomainWithDefaultValue(
          getDatanodeInfo(storage));
      List<T> storages = upgradeDomainMap.get(upgradeDomain);
      if (storages == null) {
        storages = new ArrayList<>();
        upgradeDomainMap.put(upgradeDomain, storages);
      }
      storages.add(storage);
    }
    return upgradeDomainMap;
  }

  /**
   * 验证块放置是否满足升级域约束
   * @param locs 副本所在DataNode数组
   * @param numberOfReplicas 期望副本数
   * @return 块放置状态对象，包含升级域检查结果
   */
  @Override
  public BlockPlacementStatus verifyBlockPlacement(DatanodeInfo[] locs,
      int numberOfReplicas) {
    BlockPlacementStatus defaultStatus = super.verifyBlockPlacement(locs,
        numberOfReplicas);
    BlockPlacementStatusWithUpgradeDomain upgradeDomainStatus =
        new BlockPlacementStatusWithUpgradeDomain(defaultStatus,
            getUpgradeDomainsFromNodes(locs),
                numberOfReplicas, upgradeDomainFactor);
    return upgradeDomainStatus;
  }

  /**
   * 获取共享同一个升级域的所有元素列表
   * @param upgradeDomains 按升级域分组后的映射
   * @return 所有存在多个副本的升级域中的元素列表
   */
  private <T> List<T> getShareUDSet(
      Map<String, List<T>> upgradeDomains) {
    List<T> getShareUDSet = new ArrayList<>();
    for (Map.Entry<String, List<T>> e : upgradeDomains.entrySet()) {
      if (e.getValue().size() > 1) {
        getShareUDSet.addAll(e.getValue());
      }
    }
    return getShareUDSet;
  }

  /**
   * 合并两个副本集合
   * @param moreThanOne 同一机架存在多个副本的集合
   * @param exactlyOne 同一机架仅一个副本的集合
   * @return 合并后的完整集合
   */
  private Collection<DatanodeStorageInfo> combine(
      Collection<DatanodeStorageInfo> moreThanOne,
      Collection<DatanodeStorageInfo> exactlyOne) {
    List<DatanodeStorageInfo> all = new ArrayList<>();
    if (moreThanOne != null) {
      all.addAll(moreThanOne);
    }
    if (exactlyOne != null) {
      all.addAll(exactlyOne);
    }
    return all;
  }

  /**
   * 选择需要删除的过度副本集合，优先删除同时共享机架和升级域的副本，保障数据可用性
   * 算法逻辑：将副本按是否共享机架、是否共享升级域分为四组，按优先级选择删除集合：
   * 1. 同时共享机架和升级域的副本 -> 2. 仅共享升级域的副本 -> 
   * 3. 仅共享机架的副本 -> 4. 都不共享的副本，该优先级保证删除后不会降低数据高可用性
   */
  @Override
  protected Collection<DatanodeStorageInfo> pickupReplicaSet(
      Collection<DatanodeStorageInfo> moreThanOne,
      Collection<DatanodeStorageInfo> exactlyOne,
      Map<String, List<DatanodeStorageInfo>> rackMap) {
    // 合并所有副本
    Collection<DatanodeStorageInfo> all = combine(moreThanOne, exactlyOne);
    // 获取所有共享升级域的副本
    List<DatanodeStorageInfo> shareUDSet = getShareUDSet(
        getUpgradeDomainMap(all));
    // 存储同时共享机架和升级域的副本
    List<DatanodeStorageInfo> shareRackAndUDSet = new ArrayList<>();
    if (shareUDSet.size() == 0) {
      // 所有升级域唯一，使用默认策略选择删除集合
      return super.pickupReplicaSet(moreThanOne, exactlyOne, rackMap);
    } else if (moreThanOne != null) {
      // 从共享升级域副本中筛选出同时共享机架的副本
      for (DatanodeStorageInfo storage : shareUDSet) {
        if (moreThanOne.contains(storage)) {
          shareRackAndUDSet.add(storage);
        }
      }
    }
    // 优先返回同时共享机架和升级域的集合，否则返回仅共享升级域的集合
    return (shareRackAndUDSet.size() > 0) ? shareRackAndUDSet : shareUDSet;
  }

  /**
   * 检查是否可以使用指定删除提示移动副本，增加升级域约束检查
   * @param delHint 待删除的源存储
   * @param added 新增的目标存储
   * @param moreThanOne 同一机架多个副本的集合
   * @param exactlyOne 同一机架一个副本的集合
   * @param excessTypes 过量存储类型列表
   * @return 是否允许移动
   */
  @Override
  boolean useDelHint(DatanodeStorageInfo delHint,
      DatanodeStorageInfo added, List<DatanodeStorageInfo> moreThanOne,
      Collection<DatanodeStorageInfo> exactlyOne,
      List<StorageType> excessTypes) {
    // 默认策略不允许则直接返回false
    if (!super.useDelHint(delHint, added, moreThanOne, exactlyOne,
        excessTypes)) {
      return false;
    }
    // 额外检查移动后是否保持升级域约束
    return isMovableBasedOnUpgradeDomain(combine(moreThanOne, exactlyOne),
        delHint, added);
  }

  /**
   * 检查从源移动到目标后是否保持升级域约束
   * @param all 所有副本集合
   * @param source 源待删除节点/存储
   * @param target 目标新增节点/存储
   * @return 是否满足升级域约束允许移动
   */
  private <T> boolean isMovableBasedOnUpgradeDomain(Collection<T> all,
      T source, T target) {
    // 按升级域分组
    Map<String, List<T>> udMap = getUpgradeDomainMap(all);
    // 获取共享升级域的列表
    List<T> shareUDSet = getShareUDSet(udMap);
    // 检查删除源添加目标后是否减少升级域数量
    if (notReduceNumOfGroups(shareUDSet, source, target)) {
      return true;
    } else if (udMap.size() > upgradeDomainFactor) {
      // 当前升级域数量已经超过阈值，允许移动
      return true;
    } else {
      // 移动会减少升级域数量，不允许
      return false;
    }
  }

  /**
   * 检查块副本在两个DataNode之间移动是否满足升级域约束
   * @param locs 当前所有副本位置
   * @param source 源待删除DataNode
   * @param target 目标新增DataNode
   * @return 是否允许移动
   */
  @Override
  public boolean isMovable(Collection<DatanodeInfo> locs,
      DatanodeInfo source, DatanodeInfo target) {
    if (super.isMovable(locs, source, target)) {
      return isMovableBasedOnUpgradeDomain(locs, source, target);
    } else {
      return false;
    }
  }
}