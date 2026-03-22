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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.BlockType;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * HDFS块放置策略管理器，负责管理不同类型块对应的放置策略实现，根据块类型分发策略选择请求。
 * 核心职责是为连续复制块和EC纠删码条带块分别维护对应的块放置策略实例，提供统一的策略获取入口。
 */
@InterfaceAudience.Private
public class BlockPlacementPolicies{

  private final BlockPlacementPolicy replicationPolicy;
  private final BlockPlacementPolicy ecPolicy;

  /**
   * 构造块放置策略管理器，从配置加载并初始化两种块类型对应的放置策略实例。
   * @param conf HDFS配置对象，用于获取策略实现类配置
   * @param stats 集群状态统计对象，提供集群负载信息给放置策略
   * @param clusterMap 网络拓扑结构，用于感知节点网络位置
   * @param host2datanodeMap 主机到数据节点映射，用于主机级别的放置判断
   */
  public BlockPlacementPolicies(Configuration conf, FSClusterStats stats,
                                NetworkTopology clusterMap,
                                Host2NodesMap host2datanodeMap){
    final Class<? extends BlockPlacementPolicy> replicatorClass = conf
        .getClass(DFSConfigKeys.DFS_BLOCK_REPLICATOR_CLASSNAME_KEY,
            DFSConfigKeys.DFS_BLOCK_REPLICATOR_CLASSNAME_DEFAULT,
            BlockPlacementPolicy.class);
    replicationPolicy = ReflectionUtils.newInstance(replicatorClass, conf);
    replicationPolicy.initialize(conf, stats, clusterMap, host2datanodeMap);
    final Class<? extends BlockPlacementPolicy> blockPlacementECClass =
        conf.getClass(DFSConfigKeys.DFS_BLOCK_PLACEMENT_EC_CLASSNAME_KEY,
            DFSConfigKeys.DFS_BLOCK_PLACEMENT_EC_CLASSNAME_DEFAULT,
            BlockPlacementPolicy.class);
    ecPolicy = ReflectionUtils.newInstance(blockPlacementECClass, conf);
    ecPolicy.initialize(conf, stats, clusterMap, host2datanodeMap);
  }

  /**
   * 根据块类型获取对应的块放置策略实例。
   * @param blockType 块类型，CONTIGUOUS表示普通复制块，STRIPED表示EC纠删码条带块
   * @return 对应类型的块放置策略实例
   */
  public BlockPlacementPolicy getPolicy(BlockType blockType){
    switch (blockType) {
    case CONTIGUOUS: return replicationPolicy;
    case STRIPED: return ecPolicy;
    default:
      throw new IllegalArgumentException(
          "getPolicy received a BlockType that isn't supported.");
    }
  }
}