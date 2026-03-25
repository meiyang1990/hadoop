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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.protocol.OutlierMetrics;
import org.apache.hadoop.util.Timer;

/**
 * 文件说明：慢节点追踪器的空实现，当dfs.datanode.peer.stats.enabled配置关闭时使用
 * 类功能说明：提供所有虚方法实现，不存储任何慢节点报告数据，当慢节点追踪功能禁用时代替实际追踪器工作，避免空判断
 */
@InterfaceAudience.Private
public class SlowPeerDisabledTracker extends SlowPeerTracker {

  private static final Logger LOG = LoggerFactory.getLogger(SlowPeerDisabledTracker.class);

  /**
   * 构造函数：初始化禁用状态的慢节点追踪器
   * @param conf Hadoop配置对象
   * @param timer 时间工具对象
   */
  public SlowPeerDisabledTracker(Configuration conf, Timer timer) {
    super(conf, timer);
  }

  /**
   * 获取慢节点追踪器是否启用
   * @return 固定返回false，表示功能未启用
   */
  @Override
  public boolean isSlowPeerTrackerEnabled() {
    return false;
  }

  /**
   * 添加一条慢节点报告
   * @param slowNode 被检测为慢节点的datanode标识
   * @param reportingNode 报告慢节点的datanode标识
   * @param slowNodeMetrics 慢节点的性能异常指标
   */
  @Override
  public void addReport(String slowNode, String reportingNode, OutlierMetrics slowNodeMetrics) {
    LOG.trace("Adding slow peer report is disabled. To enable it, please enable config {}.",
        DFSConfigKeys.DFS_DATANODE_PEER_STATS_ENABLED_KEY);
  }

  /**
   * 获取指定节点的所有慢节点报告
   * @param slowNode 目标慢节点标识
   * @return 固定返回空集合，表示无报告数据
   */
  @Override
  public Set<SlowPeerLatencyWithReportingNode> getReportsForNode(String slowNode) {
    LOG.trace("Retrieval of slow peer report is disabled. To enable it, please enable config {}.",
        DFSConfigKeys.DFS_DATANODE_PEER_STATS_ENABLED_KEY);
    return ImmutableSet.of();
  }

  /**
   * 获取所有数据节点的慢节点报告
   * @return 固定返回空Map，表示无报告数据
   */
  @Override
  public Map<String, SortedSet<SlowPeerLatencyWithReportingNode>> getReportsForAllDataNodes() {
    LOG.trace("Retrieval of slow peer report for all nodes is disabled. "
            + "To enable it, please enable config {}.",
        DFSConfigKeys.DFS_DATANODE_PEER_STATS_ENABLED_KEY);
    return ImmutableMap.of();
  }

  /**
   * 将所有慢节点报告转换为JSON字符串用于监控展示
   * @return 固定返回null，表示无数据
   */
  @Override
  public String getJson() {
    LOG.trace("Retrieval of slow peer reports as json string is disabled. "
            + "To enable it, please enable config {}.",
        DFSConfigKeys.DFS_DATANODE_PEER_STATS_ENABLED_KEY);
    return null;
  }

  /**
   * 获取最慢的N个节点列表，用于节点选择时排除
   * @param numNodes 需要返回的节点数量
   * @return 固定返回空列表，表示无慢节点
   */
  @Override
  public List<String> getSlowNodes(int numNodes) {
    return ImmutableList.of();
  }

}