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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.thirdparty.com.google.common.primitives.Ints;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.protocol.OutlierMetrics;
import org.apache.hadoop.hdfs.server.protocol.SlowPeerReports;
import org.apache.hadoop.util.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * 文件说明: HDFS数据节点慢节点追踪器，聚合来自各个DataNode心跳上报的慢节点报告，统计被多个DataNode标记为慢节点的异常节点
 * 核心职责: 收集、过滤和统计慢节点报告，为集群异常节点检测提供数据支持
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class SlowPeerTracker {
  public static final Logger LOG =
      LoggerFactory.getLogger(SlowPeerTracker.class);

  /**
   * 报告过期时间，超过该时间的报告被视为 stale 无效报告
   * 取值为数据节点异常报告间隔的3倍，确保至少保留两次连续上报的有效报告
   */
  private final long reportValidityMs;

  /**
   * 时间器，用于获取当前单调时间，分离实现方便单元测试
   */
  private final Timer timer;

  /**
   * JSON序列化对象写入器，用于将慢节点报告转换为JSON字符串输出
   */
  private static final ObjectWriter WRITER = new ObjectMapper().writer();
  /**
   * JSON报告中最大返回节点数，返回得票最高（被最多节点标记为慢）的节点
   */
  private volatile int maxNodesToReport;

  /**
   * 所有慢节点报告存储结构：外层key是被标记为慢节点的节点ID，
   * 内层key是上报该节点为慢的报告节点ID，value存储上报时间和延迟 metrics
   *  stale报告不会主动清理，仅在查询时过滤
   */
  private final ConcurrentMap<String, ConcurrentMap<String, LatencyWithLastReportTime>>
      allReports;

  /**
   * 构造SlowPeerTracker实例，从配置初始化参数
   * @param conf Hadoop配置对象
   * @param timer 时间器，用于获取当前时间
   */
  public SlowPeerTracker(Configuration conf, Timer timer) {
    this.timer = timer;
    this.allReports = new ConcurrentHashMap<>();
    this.reportValidityMs = conf.getTimeDuration(
        DFSConfigKeys.DFS_DATANODE_OUTLIERS_REPORT_INTERVAL_KEY,
        DFSConfigKeys.DFS_DATANODE_OUTLIERS_REPORT_INTERVAL_DEFAULT,
        TimeUnit.MILLISECONDS) * 3;
    this.setMaxSlowPeersToReport(conf.getInt(DFSConfigKeys.DFS_DATANODE_MAX_NODES_TO_REPORT_KEY,
        DFSConfigKeys.DFS_DATANODE_MAX_NODES_TO_REPORT_DEFAULT));
  }

  /**
   * 检查慢节点追踪功能是否启用
   * @return 始终返回true，表示启用慢节点追踪
   */
  public boolean isSlowPeerTrackerEnabled() {
    return true;
  }

  /**
   * 添加一条慢节点上报报告
   * @param slowNode 被怀疑为慢节点的节点ID
   * @param reportingNode 执行上报的数据节点ID
   * @param slowNodeMetrics 上报节点统计得到的慢节点延迟指标
   */
  public void addReport(String slowNode, String reportingNode, OutlierMetrics slowNodeMetrics) {
    ConcurrentMap<String, LatencyWithLastReportTime> nodeEntries = allReports.get(slowNode);

    if (nodeEntries == null) {
      // putIfAbsent保证并发写入安全
      allReports.putIfAbsent(slowNode, new ConcurrentHashMap<>());
      nodeEntries = allReports.get(slowNode);
    }

    // 覆盖该上报节点之前的旧报告，保留最新上报
    nodeEntries.put(reportingNode,
        new LatencyWithLastReportTime(timer.monotonicNow(), slowNodeMetrics));
  }

  /**
   * 获取指定节点的所有未过期有效慢节点报告，过滤掉stale报告
   * @param slowNode 目标慢节点ID
   * @return 指向该节点的所有有效报告集合
   */
  public Set<SlowPeerLatencyWithReportingNode> getReportsForNode(String slowNode) {
    final ConcurrentMap<String, LatencyWithLastReportTime> nodeEntries =
        allReports.get(slowNode);

    if (nodeEntries == null || nodeEntries.isEmpty()) {
      return Collections.emptySet();
    }

    return filterNodeReports(nodeEntries, timer.monotonicNow());
  }

  /**
   * 获取所有节点的所有未过期有效慢节点报告，过滤掉stale报告
   * @return 慢节点ID -> 有效报告集合 的映射
   */
  public Map<String, SortedSet<SlowPeerLatencyWithReportingNode>> getReportsForAllDataNodes() {
    if (allReports.isEmpty()) {
      return ImmutableMap.of();
    }

    final Map<String, SortedSet<SlowPeerLatencyWithReportingNode>> allNodesValidReports =
        new HashMap<>();
    final long now = timer.monotonicNow();

    // 遍历所有节点，过滤每个节点的无效报告
    for (Map.Entry<String, ConcurrentMap<String, LatencyWithLastReportTime>> entry
        : allReports.entrySet()) {
      SortedSet<SlowPeerLatencyWithReportingNode> validReports =
          filterNodeReports(entry.getValue(), now);
      if (!validReports.isEmpty()) {
        allNodesValidReports.put(entry.getKey(), validReports);
      }
    }
    return allNodesValidReports;
  }

  /**
   * 过滤输入报告，只保留有效期内的有效报告
   * @param reports 当前节点的所有上报报告
   * @param now 当前时间戳
   * @return 所有未过期的有效报告排序集合
   */
  private SortedSet<SlowPeerLatencyWithReportingNode> filterNodeReports(
      ConcurrentMap<String, LatencyWithLastReportTime> reports, long now) {
    final SortedSet<SlowPeerLatencyWithReportingNode> validReports = new TreeSet<>();

    // 遍历所有上报，检查时间是否在有效期内
    for (Map.Entry<String, LatencyWithLastReportTime> entry : reports.entrySet()) {
      if (now - entry.getValue().getTime() < reportValidityMs) {
        OutlierMetrics outlierMetrics = entry.getValue().getLatency();
        validReports.add(
            new SlowPeerLatencyWithReportingNode(entry.getKey(), outlierMetrics.getActualLatency(),
                outlierMetrics.getMedian(), outlierMetrics.getMad(),
                outlierMetrics.getUpperLimitLatency()));
      }
    }
    return validReports;
  }

  /**
   * 将所有有效慢节点报告序列化为JSON字符串
   * @return 序列化后的JSON字符串，序列化失败返回null
   */
  public String getJson() {
    Collection<SlowPeerJsonReport> validReports = getJsonReports(
        maxNodesToReport);
    try {
      return WRITER.writeValueAsString(validReports);
    } catch (JsonProcessingException e) {
      // 序列化失败，仅打印debug日志不输出栈追踪
      LOG.debug("Failed to serialize statistics" + e);
      return null;
    }
  }

  /**
   * 获取得票最多的前N个慢节点ID列表
   * @param numNodes 需要返回的最大节点数量
   * @return 慢节点ID列表，按得票从少到多排列
   */
  public List<String> getSlowNodes(int numNodes) {
    Collection<SlowPeerJsonReport> jsonReports = getJsonReports(numNodes);
    ArrayList<String> slowNodes = new ArrayList<>();
    for (SlowPeerJsonReport jsonReport : jsonReports) {
      slowNodes.add(jsonReport.getSlowNode());
    }
    if (!slowNodes.isEmpty()) {
      LOG.warn("Slow nodes list: " + slowNodes);
    }
    return slowNodes;
  }

  /**
   * 获取得票最多的前N个慢节点的报告结构，用于生成JSON输出
   * 优先保留被最多节点标记为慢的节点，限制输出数量避免JSON过大
   * @param numNodes 需要返回的最大节点数量
   * @return 前N个慢节点的报告集合
   */
  private Collection<SlowPeerJsonReport> getJsonReports(int numNodes) {
    if (allReports.isEmpty()) {
      return Collections.emptyList();
    }

    // 小顶堆，保存topN得票最高的节点，堆顶是当前topN中得票最少的节点
    final PriorityQueue<SlowPeerJsonReport> topNReports = new PriorityQueue<>(allReports.size(),
        (o1, o2) -> Ints.compare(o1.getSlowPeerLatencyWithReportingNodes().size(),
            o2.getSlowPeerLatencyWithReportingNodes().size()));

    final long now = timer.monotonicNow();

    // 遍历所有节点，筛选出得票最高的前numNodes个节点
    for (Map.Entry<String, ConcurrentMap<String, LatencyWithLastReportTime>> entry
        : allReports.entrySet()) {
      SortedSet<SlowPeerLatencyWithReportingNode> validReports =
          filterNodeReports(entry.getValue(), now);
      if (!validReports.isEmpty()) {
        if (topNReports.size() < numNodes) {
          // 堆还没满，直接加入
          topNReports.add(new SlowPeerJsonReport(entry.getKey(), validReports));
        } else if (topNReports.peek() != null
            && topNReports.peek().getSlowPeerLatencyWithReportingNodes().size()
            < validReports.size()) {
          // 当前节点得票比堆中最少的多，替换堆顶
          topNReports.poll();
          topNReports.add(new SlowPeerJsonReport(entry.getKey(), validReports));
        }
      }
    }
    return topNReports;
  }

  @VisibleForTesting
  long getReportValidityMs() {
    return reportValidityMs;
  }

  /**
   * 设置JSON报告最大返回节点数，线程安全
   * @param maxSlowPeersToReport 最大返回节点数
   */
  public synchronized void setMaxSlowPeersToReport(int maxSlowPeersToReport) {
    this.maxNodesToReport = maxSlowPeersToReport;
  }

  /**
   * 内部存储类，保存单条上报的时间戳和延迟指标
   */
  private static class LatencyWithLastReportTime {
    private final Long time;
    private final OutlierMetrics latency;

    LatencyWithLastReportTime(Long time, OutlierMetrics latency) {
      this.time = time;
      this.latency = latency;
    }

    public Long getTime() {
      return time;
    }

    public OutlierMetrics getLatency() {
      return latency;
    }
  }

}