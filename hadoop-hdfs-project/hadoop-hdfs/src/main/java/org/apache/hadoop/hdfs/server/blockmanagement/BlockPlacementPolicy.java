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

import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.AddBlockFlag;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 
 * HDFS 块副本放置策略抽象基类，定义了选择数据节点放置块副本的统一接口规范。
 * 不同的放置策略实现负责根据集群拓扑、存储类型等条件决定副本的分布位置。
 */
@InterfaceAudience.Private
public abstract class BlockPlacementPolicy {
  public static final Logger LOG = LoggerFactory.getLogger(
      BlockPlacementPolicy.class);

  /**
   * 副本数量不足异常，当无法满足要求的副本数量时抛出该异常。
   */
  @InterfaceAudience.Private
  public static class NotEnoughReplicasException extends Exception {
    private static final long serialVersionUID = 1L;
    NotEnoughReplicasException(String msg) {
      super(msg);
    }
  }
    
  /**
   * 为块重复制操作选择指定数量的数据节点用于放置新增副本。
   * 如果无法满足所需数量，则返回尽可能多的可用节点。
   *
   * @param srcPath 当前块所属的文件路径
   * @param numOfReplicas 需要新增的副本数量
   * @param writer 写入节点，写入者不在集群内时为null
   * @param chosen 已经选择好的目标节点列表
   * @param returnChosenNodes 是否需要将已选择节点一起返回
   * @param excludedNodes 需要排除的节点列表，这些节点不能作为目标
   * @param blocksize 块大小
   * @param storagePolicy 块存储策略
   * @param flags 块放置标志位
   * @return 排序为管道顺序的目标数据节点存储信息数组
   */
  public abstract DatanodeStorageInfo[] chooseTarget(String srcPath,
                                             int numOfReplicas,
                                             Node writer,
                                             List<DatanodeStorageInfo> chosen,
                                             boolean returnChosenNodes,
                                             Set<Node> excludedNodes,
                                             long blocksize,
                                             BlockStoragePolicy storagePolicy,
                                             EnumSet<AddBlockFlag> flags);
  
  /**
   * 基于优先节点列表选择块副本放置目标节点。
   * 优先节点仅作为提示，NameNode可能因集群状态无法满足放置要求。
   *
   * @param src 当前块所属的文件路径
   * @param numOfReplicas 需要的副本数量
   * @param writer 写入节点，写入者不在集群内时为null
   * @param excludedNodes 需要排除的节点列表
   * @param blocksize 块大小
   * @param favoredNodes 优先放置节点列表
   * @param storagePolicy 块存储策略
   * @param flags 块放置标志位
   * @return 排序为管道顺序的目标数据节点存储信息数组
   */
  DatanodeStorageInfo[] chooseTarget(String src,
      int numOfReplicas, Node writer,
      Set<Node> excludedNodes,
      long blocksize,
      List<DatanodeDescriptor> favoredNodes,
      BlockStoragePolicy storagePolicy,
      EnumSet<AddBlockFlag> flags) {
    // This class does not provide the functionality of placing
    // a block in favored datanodes. The implementations of this class
    // are expected to provide this functionality

    return chooseTarget(src, numOfReplicas, writer, 
        new ArrayList<DatanodeStorageInfo>(numOfReplicas), false,
        excludedNodes, blocksize, storagePolicy, flags);
  }

  /**
   * 基于指定存储类型要求选择块副本放置目标节点。
   *
   * @param storageTypes 目标存储类型及对应数量要求
   * @return 排序为管道顺序的目标数据节点存储信息数组
   */
  public DatanodeStorageInfo[] chooseTarget(String srcPath, int numOfReplicas,
      Node writer, List<DatanodeStorageInfo> chosen, boolean returnChosenNodes,
      Set<Node> excludedNodes, long blocksize, BlockStoragePolicy storagePolicy,
      EnumSet<AddBlockFlag> flags, EnumMap<StorageType, Integer> storageTypes) {
    return chooseTarget(srcPath, numOfReplicas, writer, chosen,
        returnChosenNodes, excludedNodes, blocksize, storagePolicy, flags);
  }

  /**
   * 验证现有块副本放置是否满足当前策略要求。
   * 例如验证副本是否分布在最小要求数量的不同机架上。
   *
   * @param locs 当前块的所有副本位置信息
   * @param numOfReplicas 文件期望的副本数量
   * @return 验证结果对象，包含是否符合要求及具体状态信息
   */
  public abstract BlockPlacementStatus verifyBlockPlacement(
      DatanodeInfo[] locs, int numOfReplicas);

  /**
   * 根据节点提示和多余存储类型选择需要删除的超额副本。
   * 用于副本过量时清理多余副本，维持期望副本数量。
   *
   * @param availableReplicas 当前所有可用副本
   * @param delCandidates 待删除候选副本集合，普通复制与availableReplicas相同，EC条带块是子集
   * @param expectedNumOfReplicas 删除后剩余期望副本数量
   * @param excessTypes 需要删除的多余存储类型列表
   * @param addedNode 新增的副本节点，可为null
   * @param delNodeHint 优先删除节点提示，可为null
   * @return 选中需要删除的超额副本存储信息列表
   */
  public abstract List<DatanodeStorageInfo> chooseReplicasToDelete(
      Collection<DatanodeStorageInfo> availableReplicas,
      Collection<DatanodeStorageInfo> delCandidates, int expectedNumOfReplicas,
      List<StorageType> excessTypes, DatanodeDescriptor addedNode,
      DatanodeDescriptor delNodeHint);

  /**
   * 初始化块放置策略对象，所有具体实现必须实现该方法。
   * 在策略对象创建后调用，完成配置加载和依赖初始化。
   *
   * @param conf Hadoop配置对象
   * @param stats 集群状态信息获取接口
   * @param clusterMap 集群网络拓扑
   * @param host2datanodeMap 主机到数据节点映射
   */
  protected abstract void initialize(Configuration conf,  FSClusterStats stats,
                                     NetworkTopology clusterMap, 
                                     Host2NodesMap host2datanodeMap);

  /**
   * 检查副本移动操作是否被当前放置策略允许。
   * 用于均衡器等工具进行数据负载均衡时的判断。
   *
   * @param candidates 包含源和目标的所有副本集合
   * @param source 移动操作的源副本
   * @param target 移动操作的目标副本
   * @return 是否允许移动
   */
  public abstract boolean isMovable(Collection<DatanodeInfo> candidates,
      DatanodeInfo source, DatanodeInfo target);

  /**
   * 在移除选中副本后调整机架分组集合，更新多副本机架和单副本机架列表。
   *
   * @param rackMap 机架到副本存储列表的映射
   * @param moreThanOne 包含超过一个副本的机架上的存储列表
   * @param exactlyOne 仅包含一个副本的机架上的存储列表
   * @param cur 需要移除的当前副本存储
   */
  public void adjustSetsWithChosenReplica(
      final Map<String, List<DatanodeStorageInfo>> rackMap,
      final List<DatanodeStorageInfo> moreThanOne,
      final List<DatanodeStorageInfo> exactlyOne,
      final DatanodeStorageInfo cur) {
    
    // 获取当前副本所在机架
    final String rack = getRack(cur.getDatanodeDescriptor());
    // 获取该机架下的所有存储列表
    final List<DatanodeStorageInfo> storages = rackMap.get(rack);
    // 移除当前副本
    storages.remove(cur);
    // 如果机架下没有存储了，从机架映射中移除该机架
    if (storages.isEmpty()) {
      rackMap.remove(rack);
    }
    // 如果当前副本原本在多副本机架列表中
    if (moreThanOne.remove(cur)) {
      // 移除后该机架仅剩一个副本
      if (storages.size() == 1) {
        final DatanodeStorageInfo remaining = storages.get(0);
        // 从多副本列表移除剩余副本，加入单副本列表
        if (moreThanOne.remove(remaining)) {
          exactlyOne.add(remaining);
        }
      }
    } else {
      // 当前副本原本在单副本列表，直接移除
      exactlyOne.remove(cur);
    }
  }

  /**
   * 从输入对象中提取数据节点信息。
   * 支持直接输入DatanodeInfo或DatanodeStorageInfo两种类型。
   *
   * @param datanode 输入对象，可以是DatanodeInfo或DatanodeStorageInfo
   * @return 提取出的DatanodeInfo对象
   */
  protected <T> DatanodeInfo getDatanodeInfo(T datanode) {
    Preconditions.checkArgument(
        datanode instanceof DatanodeInfo ||
        datanode instanceof DatanodeStorageInfo,
        "class " + datanode.getClass().getName() + " not allowed");
    if (datanode instanceof DatanodeInfo) {
      return ((DatanodeInfo)datanode);
    } else {
      return ((DatanodeStorageInfo)datanode).getDatanodeDescriptor();
    }
  }

  /**
   * 获取数据节点所在机架的网络位置字符串。
   *
   * @param datanode 数据节点信息
   * @return 机架位置字符串
   */
  protected String getRack(final DatanodeInfo datanode) {
    return datanode.getNetworkLocation();
  }

  /**
   * 根据机架信息将候选节点拆分到两个集合：所在机架有多个副本的节点集合、所在机架仅有一个副本的节点集合。
   *
   * @param availableSet 块所有可用的节点/存储集合
   * @param candidates 需要拆分的候选节点/存储集合
   * @param rackMap 机架到节点/存储列表的映射（输出参数）
   * @param moreThanOne 输出：所在机架有多个副本的候选节点集合
   * @param exactlyOne 输出：所在机架仅有一个副本的候选节点集合
   */
  public <T> void splitNodesWithRack(
      final Iterable<T> availableSet,
      final Collection<T> candidates,
      final Map<String, List<T>> rackMap,
      final List<T> moreThanOne,
      final List<T> exactlyOne) {
    // 第一步：按机架分组所有可用节点
    for(T s: availableSet) {
      final String rackName = getRack(getDatanodeInfo(s));
      List<T> storageList = rackMap.get(rackName);
      if (storageList == null) {
        storageList = new ArrayList<>();
        rackMap.put(rackName, storageList);
      }
      storageList.add(s);
    }
    // 第二步：将候选节点拆分到两个集合
    for (T candidate : candidates) {
      final String rackName = getRack(getDatanodeInfo(candidate));
      if (rackMap.get(rackName).size() == 1) {
        // 该机架只有一个副本，加入单副本集合
        exactlyOne.add(candidate);
      } else {
        // 该机架有多个副本，加入多副本集合
        moreThanOne.add(candidate);
      }
    }
  }

  /**
   * 更新是否排除慢节点的配置项，初始值由配置参数
   * DFS_NAMENODE_BLOCKPLACEMENTPOLICY_EXCLUDE_SLOW_NODES_ENABLED_KEY指定。
   *
   * @param enable true表示选择块目标节点时过滤慢节点，false不过滤
   */
  public abstract void setExcludeSlowNodesEnabled(boolean enable);

  /**
   * 获取当前是否排除慢节点的配置值。
   *
   * @return 是否排除慢节点
   */
  public abstract boolean getExcludeSlowNodesEnabled();

  /**
   * 更新允许写入的最小块数量配置，初始值由配置参数
   * DFS_NAMENODE_BLOCKPLACEMENTPOLICY_MIN_BLOCKS_FOR_WRITE_KEY指定。
   *
   * @param minBlocksForWrite 写入操作要求的最小块数量
   */
  public abstract void setMinBlocksForWrite(int minBlocksForWrite);

  /**
   * 获取当前允许写入的最小块数量配置值。
   *
   * @return 最小块数量
   */
  public abstract int getMinBlocksForWrite();
}