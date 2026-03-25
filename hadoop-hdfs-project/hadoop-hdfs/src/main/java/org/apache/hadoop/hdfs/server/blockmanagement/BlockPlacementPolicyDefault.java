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

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_BLOCKPLACEMENTPOLICY_EXCLUDE_SLOW_NODES_ENABLED_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_BLOCKPLACEMENTPOLICY_EXCLUDE_SLOW_NODES_ENABLED_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_BLOCKPLACEMENTPOLICY_MIN_BLOCKS_FOR_WRITE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_BLOCKPLACEMENTPOLICY_MIN_BLOCKS_FOR_WRITE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_CONSIDERLOADBYSTORAGETYPE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_CONSIDERLOADBYSTORAGETYPE_KEY;
import static org.apache.hadoop.util.Time.monotonicNow;

import java.util.*;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.AddBlockFlag;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.BlockStoragePolicy;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.net.DFSNetworkTopology;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NodeBase;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * @file BlockPlacementPolicyDefault.java
 * @brief 默认HDFS块副本放置策略实现类，负责为块副本选择合适的存储节点
 * 
 * 默认副本放置策略规则：
 * 1. 如果写入端在DataNode上，第一个副本默认放在本地节点（可通过NO_LOCAL_WRITE标志禁用本地写入）
 * 2. 如果写入端不在DataNode上，第一个副本随机选择一个节点
 * 3. 第二个副本放在与第一个副本不同的机架上
 * 4. 第三个副本放在与第二个副本相同机架的不同节点上
 * 后续副本遵循相同的跨机架分布原则，保证高可用性
 */
@InterfaceAudience.Private
public class BlockPlacementPolicyDefault extends BlockPlacementPolicy {

  private static final String enableDebugLogging =
      "For more information, please enable DEBUG log level on "
          + BlockPlacementPolicy.class.getName() + " and "
          + NetworkTopology.class.getName();

  // 线程局部存储，用于调试日志构建，避免多线程竞争
  private static final ThreadLocal<StringBuilder> debugLoggingBuilder
      = new ThreadLocal<StringBuilder>() {
        @Override
        protected StringBuilder initialValue() {
          return new StringBuilder();
        }
      };

  // 线程局部存储，统计节点未被选中的原因，用于日志统计
  private static final ThreadLocal<HashMap<NodeNotChosenReason, Integer>>
      CHOOSE_RANDOM_REASONS = ThreadLocal
      .withInitial(() -> new HashMap<NodeNotChosenReason, Integer>());

  // 单集群单机架场景下的块放置状态常量
  private static final BlockPlacementStatus ONE_RACK_PLACEMENT =
      new BlockPlacementStatusDefault(1, 1, 1);

  /**
   * 节点未被选中放置副本的原因枚举
   */
  protected enum NodeNotChosenReason {
    NOT_IN_SERVICE("the node is not in service"),
    NODE_STALE("the node is stale"),
    NODE_TOO_BUSY("the node is too busy"),
    NODE_TOO_BUSY_BY_VOLUME("the node is too busy based on volume load"),
    TOO_MANY_NODES_ON_RACK("the rack has too many chosen nodes"),
    NOT_ENOUGH_STORAGE_SPACE("not enough storage space to place the block"),
    NO_REQUIRED_STORAGE_TYPE("required storage types are unavailable"),
    NODE_SLOW("the node is too slow"),
    NODE_NOT_CONFORM_TO_UD("the node doesn't conform to upgrade domain policy");

    private final String text;

    NodeNotChosenReason(final String logText) {
      text = logText;
    }

    private String getText() {
      return text;
    }
  }

  // 是否考虑节点负载进行节点选择
  protected boolean considerLoad;
  // 是否按存储类型分别统计负载
  private boolean considerLoadByStorageType;
  // 负载因子，节点负载超过平均负载乘以该因子则不选择
  protected double considerLoadFactor;
  // 是否按磁盘可用卷数量考虑负载
  private boolean considerLoadByVolume = false;
  // 是否偏好选择本地节点放置第一个副本
  private boolean preferLocalNode;
  // 是否启用DataNode对等节点性能统计
  private boolean dataNodePeerStatsEnabled;
  // 是否排除慢节点不放置新块
  private volatile boolean excludeSlowNodesEnabled;
  // 集群网络拓扑
  protected NetworkTopology clusterMap;
  // 主机到DataNode映射
  protected Host2NodesMap host2datanodeMap;
  // 集群统计信息
  private FSClusterStats stats;
  protected long heartbeatInterval;   // interval for DataNode heartbeats
  private long staleInterval;   // interval used to identify stale DataNodes
  private volatile int minBlocksForWrite; // minimum number of blocks required for write operations.

  /**
   * 副本删除策略中容忍的心跳丢失倍数，超过该倍数认为节点失联
   */
  protected int tolerateHeartbeatMultiplier;

  protected BlockPlacementPolicyDefault() {
  }
    
  /**
   * 初始化块放置策略，从配置加载参数
   * @param conf 配置对象
   * @param stats 集群统计信息
   * @param clusterMap 集群网络拓扑
   * @param host2datanodeMap 主机到DataNode映射
   */
  @Override
  public void initialize(Configuration conf,  FSClusterStats stats,
                         NetworkTopology clusterMap, 
                         Host2NodesMap host2datanodeMap) {
    this.considerLoad = conf.getBoolean(
        DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_CONSIDERLOAD_KEY,
        DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_CONSIDERLOAD_DEFAULT);
    this.considerLoadByStorageType = conf.getBoolean(
        DFS_NAMENODE_REDUNDANCY_CONSIDERLOADBYSTORAGETYPE_KEY,
        DFS_NAMENODE_REDUNDANCY_CONSIDERLOADBYSTORAGETYPE_DEFAULT);
    this.considerLoadFactor = conf.getDouble(
        DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_CONSIDERLOAD_FACTOR,
        DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_CONSIDERLOAD_FACTOR_DEFAULT);
    this.considerLoadByVolume = conf.getBoolean(
        DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_CONSIDERLOADBYVOLUME_KEY,
        DFSConfigKeys.DFS_NAMENODE_REDUNDANCY_CONSIDERLOADBYVOLUME_DEFAULT
    );
    this.stats = stats;
    this.clusterMap = clusterMap;
    this.host2datanodeMap = host2datanodeMap;
    this.heartbeatInterval = conf.getTimeDuration(
        DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_KEY,
        DFSConfigKeys.DFS_HEARTBEAT_INTERVAL_DEFAULT,
        TimeUnit.SECONDS, TimeUnit.MILLISECONDS);
    this.tolerateHeartbeatMultiplier = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_TOLERATE_HEARTBEAT_MULTIPLIER_KEY,
        DFSConfigKeys.DFS_NAMENODE_TOLERATE_HEARTBEAT_MULTIPLIER_DEFAULT);
    this.staleInterval = conf.getLong(
        DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_KEY, 
        DFSConfigKeys.DFS_NAMENODE_STALE_DATANODE_INTERVAL_DEFAULT);
    this.preferLocalNode = conf.getBoolean(
        DFSConfigKeys.
            DFS_NAMENODE_BLOCKPLACEMENTPOLICY_DEFAULT_PREFER_LOCAL_NODE_KEY,
        DFSConfigKeys.
            DFS_NAMENODE_BLOCKPLACEMENTPOLICY_DEFAULT_PREFER_LOCAL_NODE_DEFAULT);
    this.dataNodePeerStatsEnabled = conf.getBoolean(
        DFSConfigKeys.DFS_DATANODE_PEER_STATS_ENABLED_KEY,
        DFSConfigKeys.DFS_DATANODE_PEER_STATS_ENABLED_DEFAULT);
    this.excludeSlowNodesEnabled = conf.getBoolean(
        DFS_NAMENODE_BLOCKPLACEMENTPOLICY_EXCLUDE_SLOW_NODES_ENABLED_KEY,
        DFS_NAMENODE_BLOCKPLACEMENTPOLICY_EXCLUDE_SLOW_NODES_ENABLED_DEFAULT);
    this.minBlocksForWrite = conf.getInt(
        DFS_NAMENODE_BLOCKPLACEMENTPOLICY_MIN_BLOCKS_FOR_WRITE_KEY,
        DFS_NAMENODE_BLOCKPLACEMENTPOLICY_MIN_BLOCKS_FOR_WRITE_DEFAULT);
  }

  @Override
  public DatanodeStorageInfo[] chooseTarget(String srcPath,
                                    int numOfReplicas,
                                    Node writer,
                                    List<DatanodeStorageInfo> chosenNodes,
                                    boolean returnChosenNodes,
                                    Set<Node> excludedNodes,
                                    long blocksize,
                                    final BlockStoragePolicy storagePolicy,
                                    EnumSet<AddBlockFlag> flags) {
    return chooseTarget(numOfReplicas, writer, chosenNodes, returnChosenNodes,
        excludedNodes, blocksize, storagePolicy, flags, null);
  }

  @Override
  public DatanodeStorageInfo[] chooseTarget(String srcPath, int numOfReplicas,
      Node writer, List<DatanodeStorageInfo> chosen, boolean returnChosenNodes,
      Set<Node> excludedNodes, long blocksize, BlockStoragePolicy storagePolicy,
      EnumSet<AddBlockFlag> flags, EnumMap<StorageType, Integer> storageTypes) {
    return chooseTarget(numOfReplicas, writer, chosen, returnChosenNodes,
        excludedNodes, blocksize, storagePolicy, flags, storageTypes);
  }

  /**
   * 带优先节点的副本位置选择方法，优先使用指定的优先节点放置副本，不足部分回退到默认策略
   * @param src 源文件路径
   * @param numOfReplicas 需要的副本数量
   * @param writer 写入节点
   * @param excludedNodes 需要排除的节点集合
   * @param blocksize 块大小
   * @param favoredNodes 优先节点列表
   * @param storagePolicy 块存储策略
   * @param flags 添加块标志
   * @return 选择出的目标存储信息数组
   */
  @Override
  DatanodeStorageInfo[] chooseTarget(String src,
      int numOfReplicas,
      Node writer,
      Set<Node> excludedNodes,
      long blocksize,
      List<DatanodeDescriptor> favoredNodes,
      BlockStoragePolicy storagePolicy,
      EnumSet<AddBlockFlag> flags) {
    try {
      if (favoredNodes == null || favoredNodes.size() == 0) {
        // 没有指定优先节点，回退到常规块放置
        return chooseTarget(src, numOfReplicas, writer,
            new ArrayList<DatanodeStorageInfo>(numOfReplicas), false, 
            excludedNodes, blocksize, storagePolicy, flags);
      }

      // 合并排除节点和优先节点，已选优先节点后续不会重复选择
      Set<Node> favoriteAndExcludedNodes = excludedNodes == null ?
          new HashSet<Node>() : new HashSet<>(excludedNodes);
      // 根据存储策略获取需要的存储类型分布
      final List<StorageType> requiredStorageTypes = storagePolicy
          .chooseStorageTypes((short)numOfReplicas);
      final EnumMap<StorageType, Integer> storageTypes =
          getRequiredStorageTypes(requiredStorageTypes);

      // 优先选择优先节点
      List<DatanodeStorageInfo> results = new ArrayList<>();
      boolean avoidStaleNodes = stats != null
          && stats.isAvoidingStaleDataNodesForWrite();

      // 计算每个机架最大允许放置的副本数和实际需要放置的副本数
      int maxNodesAndReplicas[] = getMaxNodesPerRack(0, numOfReplicas);
      numOfReplicas = maxNodesAndReplicas[0];
      int maxNodesPerRack = maxNodesAndReplicas[1];

      // 从优先节点中选择符合条件的节点
      chooseFavouredNodes(src, numOfReplicas, favoredNodes,
          favoriteAndExcludedNodes, blocksize, maxNodesPerRack, results,
          avoidStaleNodes, storageTypes);

      if (results.size() < numOfReplicas) {
        // 优先节点数量不足，从其他节点补充选择
        numOfReplicas -= results.size();
        for (DatanodeStorageInfo storage : results) {
          // 将已选节点加入排除列表，避免重复选择
          addToExcludedNodes(storage.getDatanodeDescriptor(),
              favoriteAndExcludedNodes);
        }
        // 从剩余节点选择不足的副本
        DatanodeStorageInfo[] remainingTargets =
            chooseTarget(src, numOfReplicas, writer,
                new ArrayList<DatanodeStorageInfo>(numOfReplicas), false,
                favoriteAndExcludedNodes, blocksize, storagePolicy, flags,
                storageTypes);
        for (int i = 0; i < remainingTargets.length; i++) {
          results.add(remainingTargets[i]);
        }
      }
      // 排序生成数据流水线
      return getPipeline(writer,
          results.toArray(new DatanodeStorageInfo[results.size()]));
    } catch (NotEnoughReplicasException nr) {
      LOG.debug("Failed to choose with favored nodes (={}), disregard favored nodes hint and retry",
          favoredNodes, nr);
      // 优先节点选择失败，忽略优先节点提示，回退到常规块放置重试
      return chooseTarget(src, numOfReplicas, writer, 
          new ArrayList<DatanodeStorageInfo>(numOfReplicas), false, 
          excludedNodes, blocksize, storagePolicy, flags);
    }
  }

  /**
   * 从优先节点列表中选择符合条件的节点放置副本
   * @param src 源文件路径
   * @param numOfReplicas 需要的副本数量
   * @param favoredNodes 优先节点列表
   * @param favoriteAndExcludedNodes 排除节点集合，已选节点会加入此集合
   * @param blocksize 块大小
   * @param maxNodesPerRack 每个机架最大允许节点数
   * @param results 存储选择结果
   * @param avoidStaleNodes 是否避免 stale 节点
   * @param storageTypes 需要的存储类型分布
   * @throws NotEnoughReplicasException 当无法选择足够副本时抛出
   */
  protected void chooseFavouredNodes(String src, int numOfReplicas,
      List<DatanodeDescriptor> favoredNodes,
      Set<Node> favoriteAndExcludedNodes, long blocksize, int maxNodesPerRack,
      List<DatanodeStorageInfo> results, boolean avoidStaleNodes,
      EnumMap<StorageType, Integer> storageTypes)
      throws NotEnoughReplicasException {
    for (int i = 0; i < favoredNodes.size() && results.size() < numOfReplicas;
        i++) {
      DatanodeDescriptor favoredNode = favoredNodes.get(i);
      // 选择一个优先节点本地的存储，结果会直接添加到results
      final DatanodeStorageInfo target = chooseLocalOrFavoredStorage(
          favoredNode, true, favoriteAndExcludedNodes, blocksize,
          maxNodesPerRack, results, avoidStaleNodes, storageTypes);

      if (target == null) {
        LOG.warn("Could not find a target for file " + src
            + " with favored node " + favoredNode);
        continue;
      }
      favoriteAndExcludedNodes.add(target.getDatanodeDescriptor());
    }
  }

  /**
   * 核心副本位置选择实现，处理客户端请求的NO_LOCAL_WRITE和NO_LOCAL_RACK标志，根据情况回退
   * @param numOfReplicas 需要的副本数量
   * @param writer 写入节点
   * @param chosenStorage 已选择的存储列表
   * @param returnChosenNodes 是否返回已选择的节点
   * @param excludedNodes 需要排除的节点集合
   * @param blocksize 块大小
   * @param storagePolicy 块存储策略
   * @param addBlockFlags 添加块标志
   * @param sTypes 需要的存储类型分布
   * @return 选择出的目标存储信息数组，已排序形成流水线
   */