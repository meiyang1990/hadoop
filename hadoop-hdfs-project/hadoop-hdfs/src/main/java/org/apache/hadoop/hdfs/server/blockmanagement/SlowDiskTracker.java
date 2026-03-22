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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.hadoop.thirdparty.com.google.common.primitives.Doubles;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.protocol.SlowDiskReports;
import org.apache.hadoop.hdfs.server.protocol.SlowDiskReports.DiskOp;
import org.apache.hadoop.util.Lists;
import org.apache.hadoop.util.Timer;
import org.apache.hadoop.util.concurrent.SubjectInheritingThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 慢磁盘追踪器，聚合DataNode心跳上报的慢磁盘检测信息，生成全局慢磁盘统计报告供监控使用
 * 收集所有DataNode上报的慢磁盘延迟数据，定期清理过期数据并整理出延迟最高的慢磁盘列表
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class SlowDiskTracker {
  public static final Logger LOG =
      LoggerFactory.getLogger(SlowDiskTracker.class);

  /**
   * 慢磁盘报告的过期时间，超过该时间未更新的报告将被视为无效并清理
   * 默认为数据节点异常报告间隔的3倍，保证至少保留连续两次上报的数据
   */
  private long reportValidityMs;

  /**
   * 时间计时器，用于获取当前时间戳，分离接口便于单元测试
   */
  private final Timer timer;

  /**
   * JSON序列化对象写器，用于将慢磁盘报告转换为JSON字符串
   */
  private static final ObjectWriter WRITER = new ObjectMapper().writer();

  /**
   * JSON报告中每个操作最多包含的慢磁盘数量，仅返回延迟最高的指定数量磁盘
   */
  private final int maxDisksToReport;
  private static final String DATANODE_DISK_SEPARATOR = ":";
  /**
   * 慢磁盘报告更新的时间间隔，控制多久重新生成一次报告
   */
  private final long reportGenerationIntervalMs;

  private volatile long lastUpdateTime;
  private AtomicBoolean isUpdateInProgress = new AtomicBoolean(false);

  /**
   * 存储所有上报的慢磁盘延迟信息，Key为慢磁盘唯一ID，Value为包含延迟和时间戳的信息对象
   */
  private final Map<String, DiskLatency> diskIDLatencyMap;

  /**
   * 当前生成的慢磁盘报告列表，存储延迟最高的慢磁盘信息
   */
  private volatile ArrayList<DiskLatency> slowDisksReport =
      Lists.newArrayList();
  /**
   * 待清理的过期慢磁盘列表，存储本次更新中检测到的过期报告
   */
  private volatile ArrayList<DiskLatency> oldSlowDisksCheck;

  /**
   * 构造慢磁盘追踪器，从配置中初始化报告间隔、报告数量等参数
   * @param conf Hadoop配置对象
   * @param timer 时间计时器
   */
  public SlowDiskTracker(Configuration conf, Timer timer) {
    this.timer = timer;
    this.lastUpdateTime = timer.monotonicNow();
    this.diskIDLatencyMap = new ConcurrentHashMap<>();
    this.reportGenerationIntervalMs = conf.getTimeDuration(
        DFSConfigKeys.DFS_DATANODE_OUTLIERS_REPORT_INTERVAL_KEY,
        DFSConfigKeys.DFS_DATANODE_OUTLIERS_REPORT_INTERVAL_DEFAULT,
        TimeUnit.MILLISECONDS);
    this.maxDisksToReport = conf.getInt(
        DFSConfigKeys.DFS_DATANODE_MAX_DISKS_TO_REPORT_KEY,
        DFSConfigKeys.DFS_DATANODE_MAX_DISKS_TO_REPORT_DEFAULT);
    this.reportValidityMs = reportGenerationIntervalMs * 3;
  }

  /**
   * 生成慢磁盘的全局唯一ID，格式为数据节点ID:磁盘ID
   * @param datanodeID 数据节点ID
   * @param slowDisk 磁盘ID
   * @return 全局唯一慢磁盘ID
   */
  @VisibleForTesting
  public static String getSlowDiskIDForReport(String datanodeID,
      String slowDisk) {
    return datanodeID + DATANODE_DISK_SEPARATOR + slowDisk;
  }

  /**
   * 添加来自数据节点的慢磁盘上报信息，将所有慢磁盘存入全局映射
   * @param dataNodeID 上报数据的节点ID
   * @param dnSlowDiskReport 数据节点上报的慢磁盘报告
   */
  public void addSlowDiskReport(String dataNodeID,
      SlowDiskReports dnSlowDiskReport) {
    Map<String, Map<DiskOp, Double>> slowDisks =
        dnSlowDiskReport.getSlowDisks();

    long now = timer.monotonicNow();

    for (Map.Entry<String, Map<DiskOp, Double>> slowDiskEntry :
        slowDisks.entrySet()) {

      String diskID = getSlowDiskIDForReport(dataNodeID,
          slowDiskEntry.getKey());

      Map<DiskOp, Double> latencies = slowDiskEntry.getValue();

      DiskLatency diskLatency = new DiskLatency(diskID, latencies, now);
      diskIDLatencyMap.put(diskID, diskLatency);
    }

  }

  /**
   * 检查是否需要更新慢磁盘报告，如果达到更新间隔则异步更新
   */
  public void checkAndUpdateReportIfNecessary() {
    // Check if it is time for update
    long now = timer.monotonicNow();
    if (now - lastUpdateTime > reportGenerationIntervalMs) {
      updateSlowDiskReportAsync(now);
    }
  }

  /**
   * 异步更新慢磁盘报告，启动独立线程处理避免阻塞主线程
   * @param now 当前时间戳
   */
  @VisibleForTesting
  public void updateSlowDiskReportAsync(long now) {
    if (isUpdateInProgress.compareAndSet(false, true)) {
      lastUpdateTime = now;
      new SubjectInheritingThread(new Runnable() {
        @Override
        public void run() {
          slowDisksReport = getSlowDisks(diskIDLatencyMap,
              maxDisksToReport, now);

          cleanUpOldReports(now);

          isUpdateInProgress.set(false);
        }
      }).start();
    }
  }

  /**
   * 存储单个慢磁盘的延迟信息，包含磁盘ID、各操作延迟和上报时间戳
   * 支持JSON序列化，用于生成监控报告
   */
  public static class DiskLatency {
    @JsonProperty("SlowDiskID")
    final private String slowDiskID;
    @JsonProperty("Latencies")
    final private Map<DiskOp, Double> latencyMap;
    @JsonIgnore
    private long timestamp;

    /**
     * Jackson JSON反序列化需要的空参构造对应的构造方法
     */
    public DiskLatency(
        @JsonProperty("SlowDiskID") String slowDiskID,
        @JsonProperty("Latencies") Map<DiskOp, Double> latencyMap) {
      this.slowDiskID = slowDiskID;
      this.latencyMap = latencyMap;
    }

    /**
     * 构造磁盘延迟对象
     * @param slowDiskID 慢磁盘唯一ID
     * @param latencyMap 各磁盘操作对应的延迟映射
     * @param timestamp 上报时间戳
     */
    public DiskLatency(String slowDiskID, Map<DiskOp, Double> latencyMap,
        long timestamp) {
      this.slowDiskID = slowDiskID;
      this.latencyMap = latencyMap;
      this.timestamp = timestamp;
    }

    String getSlowDiskID() {
      return this.slowDiskID;
    }

    /**
     * 获取该磁盘所有操作中的最大延迟值
     * @return 最大延迟
     */
    double getMaxLatency() {
      double maxLatency = 0;
      for (double latency : latencyMap.values()) {
        if (latency > maxLatency) {
          maxLatency = latency;
        }
      }
      return maxLatency;
    }

    Double getLatency(DiskOp op) {
      return this.latencyMap.get(op);
    }
  }

  /**
   * 从所有上报中筛选出延迟最高的N个有效慢磁盘
   * @param reports 所有上报的慢磁盘映射
   * @param numDisks 需要返回的最大磁盘数量，限制JSON报告大小
   * @param now 当前时间戳，用于判断报告是否过期
   * @return 按延迟排序的top N慢磁盘列表
   */
  private ArrayList<DiskLatency> getSlowDisks(
      Map<String, DiskLatency> reports, int numDisks, long now) {
    if (reports.isEmpty()) {
      return new ArrayList(ImmutableList.of());
    }

    // 使用优先队列维护top N延迟最高的磁盘
    final PriorityQueue<DiskLatency> topNReports = new PriorityQueue<>(
        reports.size(),
        new Comparator<DiskLatency>() {
          @Override
          public int compare(DiskLatency o1, DiskLatency o2) {
            return Doubles.compare(
                o1.getMaxLatency(), o2.getMaxLatency());
          }
        });

    ArrayList<DiskLatency> oldSlowDiskIDs = Lists.newArrayList();

    // 遍历所有上报，筛选有效报告并找出top N
    for (Map.Entry<String, DiskLatency> entry : reports.entrySet()) {
      DiskLatency diskLatency = entry.getValue();
      // 报告未过期，参与top N筛选
      if (now - diskLatency.timestamp < reportValidityMs) {
        if (topNReports.size() < numDisks) {
          topNReports.add(diskLatency);
        } else if (topNReports.peek().getMaxLatency() <
            diskLatency.getMaxLatency()) {
          topNReports.poll();
          topNReports.add(diskLatency);
        }
      } else {
        // 报告已过期，加入待清理列表
        oldSlowDiskIDs.add(diskLatency);
      }
    }

    oldSlowDisksCheck = oldSlowDiskIDs;

    return Lists.newArrayList(topNReports);
  }

  /**
   * 将当前有效的慢磁盘报告序列化为JSON字符串
   * @return 序列化后的JSON字符串，无数据或序列化失败返回null
   */
  public String getSlowDiskReportAsJsonString() {
    try {
      if (slowDisksReport.isEmpty()) {
        return null;
      }
      return WRITER.writeValueAsString(slowDisksReport);
    } catch (JsonProcessingException e) {
      // Failed to serialize. Don't log the exception call stack.
      LOG.debug("Failed to serialize statistics" + e);
      return null;
    }
  }

  /**
   * 从全局映射中清理过期的慢磁盘报告
   */
  private void cleanUpOldReports(long now) {
    if (oldSlowDisksCheck != null) {
      for (DiskLatency oldDiskLatency : oldSlowDisksCheck) {
        diskIDLatencyMap.remove(oldDiskLatency.getSlowDiskID(), oldDiskLatency);
      }
    }
    // 清空待清理列表
    oldSlowDisksCheck = null;
  }

  @VisibleForTesting
  ArrayList<DiskLatency> getSlowDisksReport() {
    return this.slowDisksReport;
  }

  @VisibleForTesting
  long getReportValidityMs() {
    return reportValidityMs;
  }

  @VisibleForTesting
  void setReportValidityMs(long reportValidityMs) {
    this.reportValidityMs = reportValidityMs;
  }
}