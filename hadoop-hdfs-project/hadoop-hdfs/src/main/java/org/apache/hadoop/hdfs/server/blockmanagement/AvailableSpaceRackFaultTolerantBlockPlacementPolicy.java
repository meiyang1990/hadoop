// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.net.DFSNetworkTopology;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Random;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_AVAILABLE_SPACE_BLOCK_RACK_FAULT_TOLERANT_PLACEMENT_POLICY_BALANCED_SPACE_PREFERENCE_FRACTION_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_AVAILABLE_SPACE_BLOCK_RACK_FAULT_TOLERANT_PLACEMENT_POLICY_BALANCED_SPACE_TOLERANCE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_AVAILABLE_SPACE_RACK_FAULT_TOLERANT_BLOCK_PLACEMENT_POLICY_BALANCED_SPACE_PREFERENCE_FRACTION_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_AVAILABLE_SPACE_RACK_FAULT_TOLERANT_BLOCK_PLACEMENT_POLICY_BALANCED_SPACE_TOLERANCE_KEY;

/**
 * 基于可用空间均衡的机架容错块放置策略，在保持机架容错的基础上，优先将块分配到剩余空间更充足的DataNode，实现空间负载均衡。
 */
public class AvailableSpaceRackFaultTolerantBlockPlacementPolicy
    extends BlockPlacementPolicyRackFaultTolerant {

  private static final Logger LOG = LoggerFactory
      .getLogger(AvailableSpaceRackFaultTolerantBlockPlacementPolicy.class);
  private static final Random RAND = new Random();
  private int balancedPreference = (int) (100
      * DFS_NAMENODE_AVAILABLE_SPACE_BLOCK_RACK_FAULT_TOLERANT_PLACEMENT_POLICY_BALANCED_SPACE_PREFERENCE_FRACTION_DEFAULT);
  private int balancedSpaceTolerance =
        DFS_NAMENODE_AVAILABLE_SPACE_BLOCK_RACK_FAULT_TOLERANT_PLACEMENT_POLICY_BALANCED_SPACE_TOLERANCE_DEFAULT;

  /**
   * 初始化块放置策略，从配置中加载空间均衡相关参数，并对参数合法性进行校验。
   * @param conf 配置对象
   * @param stats 集群统计信息
   * @param clusterMap 集群网络拓扑
   * @param host2datanodeMap 主机到DataNode的映射
   */
  @Override
  public void initialize(Configuration conf, FSClusterStats stats,
      NetworkTopology clusterMap, Host2NodesMap host2datanodeMap) {
    super.initialize(conf, stats, clusterMap, host2datanodeMap);
    // 读取空间均衡偏好比例配置
    float balancedPreferencePercent = conf.getFloat(
        DFS_NAMENODE_AVAILABLE_SPACE_RACK_FAULT_TOLERANT_BLOCK_PLACEMENT_POLICY_BALANCED_SPACE_PREFERENCE_FRACTION_KEY,
        DFS_NAMENODE_AVAILABLE_SPACE_BLOCK_RACK_FAULT_TOLERANT_PLACEMENT_POLICY_BALANCED_SPACE_PREFERENCE_FRACTION_DEFAULT);

    // 读取空间均衡容差配置
    balancedSpaceTolerance = conf.getInt(
            DFS_NAMENODE_AVAILABLE_SPACE_RACK_FAULT_TOLERANT_BLOCK_PLACEMENT_POLICY_BALANCED_SPACE_TOLERANCE_KEY,
            DFS_NAMENODE_AVAILABLE_SPACE_BLOCK_RACK_FAULT_TOLERANT_PLACEMENT_POLICY_BALANCED_SPACE_TOLERANCE_DEFAULT);

    LOG.info("Available space rack fault tolerant block placement policy "
        + "initialized: "
        + DFSConfigKeys.DFS_NAMENODE_AVAILABLE_SPACE_RACK_FAULT_TOLERANT_BLOCK_PLACEMENT_POLICY_BALANCED_SPACE_PREFERENCE_FRACTION_KEY
        + " = " + balancedPreferencePercent);

    // 偏好比例超出范围告警
    if (balancedPreferencePercent > 1.0) {
      LOG.warn("The value of "
          + DFS_NAMENODE_AVAILABLE_SPACE_RACK_FAULT_TOLERANT_BLOCK_PLACEMENT_POLICY_BALANCED_SPACE_PREFERENCE_FRACTION_KEY
          + " is greater than 1.0 but should be in the range 0.0 - 1.0");
    }
    // 偏好比例过低告警，会导致空间使用多的节点分配更多块
    if (balancedPreferencePercent < 0.5) {
      LOG.warn("The value of "
          + DFS_NAMENODE_AVAILABLE_SPACE_RACK_FAULT_TOLERANT_BLOCK_PLACEMENT_POLICY_BALANCED_SPACE_PREFERENCE_FRACTION_KEY
          + " is less than 0.5 so datanodes with more used percent will"
          + " receive  more block allocations.");
    }

    // 校验容差参数合法性，不合法则使用默认值
    if (balancedSpaceTolerance > 20 || balancedSpaceTolerance < 0) {
      LOG.warn("The value of "
          + DFS_NAMENODE_AVAILABLE_SPACE_RACK_FAULT_TOLERANT_BLOCK_PLACEMENT_POLICY_BALANCED_SPACE_TOLERANCE_KEY
          + " is invalid, Current value is " + balancedSpaceTolerance + ", Default value " +
            DFS_NAMENODE_AVAILABLE_SPACE_BLOCK_RACK_FAULT_TOLERANT_PLACEMENT_POLICY_BALANCED_SPACE_TOLERANCE_DEFAULT
          + " will be used instead.");
      balancedSpaceTolerance =
            DFS_NAMENODE_AVAILABLE_SPACE_BLOCK_RACK_FAULT_TOLERANT_PLACEMENT_POLICY_BALANCED_SPACE_TOLERANCE_DEFAULT;
    }

    // 转换为百分比整数形式
    balancedPreference = (int) (100 * balancedPreferencePercent);
  }

  /**
   * 根据指定范围和排除节点，选择对应存储类型的DataNode，采用二选一空间均衡策略选择结果。
   * @param scope 网络拓扑范围
   * @param excludedNode 需要排除的节点集合
   * @param type 存储类型
   * @return 选中的DataNode描述符
   */
  @Override
  protected DatanodeDescriptor chooseDataNode(final String scope,
      final Collection<Node> excludedNode, StorageType type) {
    // 确保只有DFSNetworkTopology会进入该代码路径
    Preconditions.checkArgument(clusterMap instanceof DFSNetworkTopology);
    DFSNetworkTopology dfsClusterMap = (DFSNetworkTopology) clusterMap;
    // 随机抽取两个候选节点
    DatanodeDescriptor a = (DatanodeDescriptor) dfsClusterMap
        .chooseRandomWithStorageTypeTwoTrial(scope, excludedNode, type);
    DatanodeDescriptor b = (DatanodeDescriptor) dfsClusterMap
        .chooseRandomWithStorageTypeTwoTrial(scope, excludedNode, type);
    // 空间均衡策略选择
    return select(a, b);
  }

  /**
   * 根据指定范围和排除节点，选择DataNode，采用二选一空间均衡策略选择结果。
   * @param scope 网络拓扑范围
   * @param excludedNode 需要排除的节点集合
   * @return 选中的DataNode描述符
   */
  @Override
  protected DatanodeDescriptor chooseDataNode(final String scope,
      final Collection<Node> excludedNode) {
    // 随机抽取两个候选节点
    DatanodeDescriptor a =
        (DatanodeDescriptor) clusterMap.chooseRandom(scope, excludedNode);
    DatanodeDescriptor b =
        (DatanodeDescriptor) clusterMap.chooseRandom(scope, excludedNode);
    // 空间均衡策略选择
    return select(a, b);
  }

  /**
   * 从两个候选DataNode中根据空间使用情况和均衡偏好概率选择一个节点。
   * @param a 第一个候选节点
   * @param b 第二个候选节点
   * @return 选中的DataNode描述符
   */
  private DatanodeDescriptor select(DatanodeDescriptor a,
      DatanodeDescriptor b) {
    if (a != null && b != null) {
      int ret = compareDataNode(a, b);
      if (ret == 0) {
        // 两个节点空间使用率差异在容差范围内，直接返回第一个
        return a;
      } else if (ret < 0) {
        // a空间使用率更低，按偏好概率选中a，否则选中b
        return (RAND.nextInt(100) < balancedPreference) ? a : b;
      } else {
        // b空间使用率更低，按偏好概率选中b，否则选中a
        return (RAND.nextInt(100) < balancedPreference) ? b : a;
      }
    } else {
      // 其中一个节点为空，返回非空的那个
      return a == null ? b : a;
    }
  }

  /**
   * 比较两个DataNode的磁盘使用率，判断空间差异是否超过容差阈值。
   * @param a 第一个DataNode
   * @param b 第二个DataNode
   * @return 0表示差异在容差范围内；负数表示a使用率更低；正数表示b使用率更低
   */
  protected int compareDataNode(final DatanodeDescriptor a,
      final DatanodeDescriptor b) {
    if (a.equals(b)
        || Math.abs(a.getDfsUsedPercent() - b.getDfsUsedPercent()) < balancedSpaceTolerance) {
      return 0;
    }
    return a.getDfsUsedPercent() < b.getDfsUsedPercent() ? -1 : 1;
  }
}