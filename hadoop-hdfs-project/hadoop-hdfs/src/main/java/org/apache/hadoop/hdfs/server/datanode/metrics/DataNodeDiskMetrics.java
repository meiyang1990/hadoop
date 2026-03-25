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
package org.apache.hadoop.hdfs.server.datanode.metrics;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.thirdparty.com.google.common.collect.Maps;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.DataNodeVolumeMetrics;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.protocol.SlowDiskReports.DiskOp;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_MIN_OUTLIER_DETECTION_DISKS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_SLOWDISK_LOW_THRESHOLD_MS_KEY;

/**
 * DataNode磁盘离群点（慢磁盘）检测与管理类，负责定期检测DataNode上性能异常的慢磁盘，
 * 记录不同磁盘操作（元数据、读、写）的延迟统计，并输出需要排除的慢磁盘列表。
 * 核心职责：周期性采集磁盘延迟指标、通过离群点算法识别慢磁盘、维护异常磁盘状态。
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class DataNodeDiskMetrics {

  public static final Logger LOG = LoggerFactory.getLogger(
      DataNodeDiskMetrics.class);

  private DataNode dn;
  private final long detectionInterval;
  private volatile boolean shouldRun;
  private OutlierDetector slowDiskDetector;
  private Daemon slowDiskDetectionDaemon;
  private volatile Map<String, Map<DiskOp, Double>>
      diskOutliersStats = Maps.newHashMap();

  // 测试专用标记：当调用addSlowDiskForTesting后，后台线程不会覆盖测试添加的状态
  private boolean overrideStatus = true;

  /**
   * 触发离群点检测所需的最小磁盘数量，磁盘数低于该值不进行检测。
   */
  private volatile long minOutlierDetectionDisks;
  /**
   * 磁盘延迟低阈值（毫秒），延迟低于该值的磁盘一定不会被判定为慢磁盘。
   */
  private volatile long lowThresholdMs;
  /**
   * 最多需要排除的慢磁盘数量。
   */
  private volatile int maxSlowDisksToExclude;
  /**
   * 需要被排除的慢磁盘路径列表。
   */
  private List<String> slowDisksToExclude = new ArrayList<>();

  /**
   * 构造DataNode磁盘离群点检测管理器，加载配置并启动后台检测线程。
   * @param dn 所属DataNode实例
   * @param diskOutlierDetectionIntervalMs 检测周期（毫秒）
   * @param conf Hadoop配置对象
   */
  public DataNodeDiskMetrics(DataNode dn, long diskOutlierDetectionIntervalMs,
      Configuration conf) {
    this.dn = dn;
    this.detectionInterval = diskOutlierDetectionIntervalMs;
    minOutlierDetectionDisks =
        conf.getLong(DFSConfigKeys.DFS_DATANODE_MIN_OUTLIER_DETECTION_DISKS_KEY,
            DFSConfigKeys.DFS_DATANODE_MIN_OUTLIER_DETECTION_DISKS_DEFAULT);
    lowThresholdMs =
        conf.getLong(DFSConfigKeys.DFS_DATANODE_SLOWDISK_LOW_THRESHOLD_MS_KEY,
            DFSConfigKeys.DFS_DATANODE_SLOWDISK_LOW_THRESHOLD_MS_DEFAULT);
    maxSlowDisksToExclude =
        conf.getInt(DFSConfigKeys.DFS_DATANODE_MAX_SLOWDISKS_TO_EXCLUDE_KEY,
            DFSConfigKeys.DFS_DATANODE_MAX_SLOWDISKS_TO_EXCLUDE_DEFAULT);
    slowDiskDetector =
        new OutlierDetector(minOutlierDetectionDisks, lowThresholdMs);
    shouldRun = true;
    startDiskOutlierDetectionThread();
  }

  /**
   * 启动后台守护线程，定期执行慢磁盘离群点检测。
   */
  private void startDiskOutlierDetectionThread() {
    slowDiskDetectionDaemon = new Daemon(new Runnable() {
      @Override
      public void run() {
        while (shouldRun) {
          if (dn.getFSDataset() != null) {
            // 分别存储元数据、读、写三种操作的平均延迟
            Map<String, Double> metadataOpStats = Maps.newHashMap();
            Map<String, Double> readIoStats = Maps.newHashMap();
            Map<String, Double> writeIoStats = Maps.newHashMap();
            FsDatasetSpi.FsVolumeReferences fsVolumeReferences = null;
            try {
              // 获取所有数据卷引用
              fsVolumeReferences = dn.getFSDataset().getFsVolumeReferences();
              Iterator<FsVolumeSpi> volumeIterator = fsVolumeReferences
                  .iterator();
              while (volumeIterator.hasNext()) {
                FsVolumeSpi volume = volumeIterator.next();
                DataNodeVolumeMetrics metrics = volume.getMetrics();
                String volumeName = volume.getBaseURI().getPath();

                // 收集三种操作的平均延迟
                metadataOpStats.put(volumeName,
                    metrics.getMetadataOperationMean());
                readIoStats.put(volumeName, metrics.getReadIoMean());
                writeIoStats.put(volumeName, metrics.getWriteIoMean());
              }
            } finally {
              // 释放卷引用
              if (fsVolumeReferences != null) {
                try {
                  fsVolumeReferences.close();
                } catch (IOException e) {
                  LOG.error("Error in releasing FS Volume references", e);
                }
              }
            }
            // 没有可用磁盘统计数据，跳过本次检测
            if (metadataOpStats.isEmpty() && readIoStats.isEmpty()
                && writeIoStats.isEmpty()) {
              LOG.debug("No disk stats available for detecting outliers.");
              continue;
            }

            // 执行离群点检测并更新异常磁盘统计
            detectAndUpdateDiskOutliers(metadataOpStats, readIoStats,
                writeIoStats);

            // 按延迟排序，提取延迟最高的N个慢磁盘加入排除列表
            if (maxSlowDisksToExclude > 0) {
              ArrayList<DiskLatency> diskLatencies = new ArrayList<>();
              for (Map.Entry<String, Map<DiskOp, Double>> diskStats :
                  diskOutliersStats.entrySet()) {
                diskLatencies.add(new DiskLatency(diskStats.getKey(), diskStats.getValue()));
              }

              // 按最大延迟降序排序
              Collections.sort(diskLatencies, (o1, o2)
                  -> Double.compare(o2.getMaxLatency(), o1.getMaxLatency()));

              // 截取前maxSlowDisksToExclude个作为待排除列表
              slowDisksToExclude = diskLatencies.stream().limit(maxSlowDisksToExclude)
                  .map(DiskLatency::getSlowDisk).collect(Collectors.toList());
            }
          }

          try {
            // 等待下一个检测周期
            Thread.sleep(detectionInterval);
          } catch (InterruptedException e) {
            LOG.error("Disk Outlier Detection thread interrupted", e);
            Thread.currentThread().interrupt();
          }
        }
      }
    });
    slowDiskDetectionDaemon.start();
  }

  /**
   * 对三种磁盘操作分别执行离群点检测，更新慢磁盘统计结果。
   * @param metadataOpStats 所有磁盘元数据操作平均延迟
   * @param readIoStats 所有磁盘读操作平均延迟
   * @param writeIoStats 所有磁盘写操作平均延迟
   */
  private void detectAndUpdateDiskOutliers(Map<String, Double> metadataOpStats,
      Map<String, Double> readIoStats, Map<String, Double> writeIoStats) {
    Map<String, Map<DiskOp, Double>> diskStats = Maps.newHashMap();

    // 检测元数据操作离群点
    Map<String, Double> metadataOpOutliers = slowDiskDetector
        .getOutliers(metadataOpStats);
    for (Map.Entry<String, Double> entry : metadataOpOutliers.entrySet()) {
      addDiskStat(diskStats, entry.getKey(), DiskOp.METADATA, entry.getValue());
    }

    // 检测读操作离群点
    Map<String, Double> readIoOutliers = slowDiskDetector
        .getOutliers(readIoStats);
    for (Map.Entry<String, Double> entry : readIoOutliers.entrySet()) {
      addDiskStat(diskStats, entry.getKey(), DiskOp.READ, entry.getValue());
    }

    // 检测写操作离群点
    Map<String, Double> writeIoOutliers = slowDiskDetector
        .getOutliers(writeIoStats);
    for (Map.Entry<String, Double> entry : writeIoOutliers.entrySet()) {
      addDiskStat(diskStats, entry.getKey(), DiskOp.WRITE, entry.getValue());
    }
    // 非测试模式下，更新全局异常磁盘统计
    if (overrideStatus) {
      diskOutliersStats = diskStats;
      LOG.debug("Updated disk outliers.");
    }
  }

  /**
   * 磁盘延迟信息包装类，存储单个磁盘所有操作的延迟，并提供获取最大延迟的方法。
   * 用于对慢磁盘按延迟排序，筛选需要排除的磁盘。
   */
  public static class DiskLatency {
    final private String slowDisk;
    final private Map<DiskOp, Double> latencyMap;

    public DiskLatency(
        String slowDiskID,
        Map<DiskOp, Double> latencyMap) {
      this.slowDisk = slowDiskID;
      this.latencyMap = latencyMap;
    }

    /**
     * 获取当前磁盘所有操作中的最大延迟值。
     * @return 最大延迟值
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

    public String getSlowDisk() {
      return slowDisk;
    }
  }

  /**
   * 添加磁盘操作延迟统计到结果集合中。
   * @param diskStats 整体结果集合
   * @param disk 磁盘路径
   * @param diskOp 磁盘操作类型
   * @param latency 平均延迟
   */
  private void addDiskStat(Map<String, Map<DiskOp, Double>> diskStats,
      String disk, DiskOp diskOp, double latency) {
    if (!diskStats.containsKey(disk)) {
      diskStats.put(disk, new HashMap<>());
    }
    diskStats.get(disk).put(diskOp, latency);
  }

  /**
   * 获取当前所有离群慢磁盘的延迟统计。
   * @return 离群磁盘统计：key为磁盘路径，value为该磁盘各操作的延迟
   */
  public Map<String, Map<DiskOp, Double>> getDiskOutliersStats() {
    return diskOutliersStats;
  }

  /**
   * 关闭后台检测线程，等待线程退出。
   */
  public void shutdownAndWait() {
    shouldRun = false;
    slowDiskDetectionDaemon.interrupt();
    try {
      slowDiskDetectionDaemon.join();
    } catch (InterruptedException e) {
      LOG.error("Disk Outlier Detection daemon did not shutdown", e);
    }
  }

  /**
   * 测试专用方法，手动添加测试用慢磁盘。
   * @param slowDiskPath 慢磁盘路径
   * @param latencies 各操作延迟
   */
  @VisibleForTesting
  public void addSlowDiskForTesting(String slowDiskPath,
      Map<DiskOp, Double> latencies) {
    overrideStatus = false;
    if (latencies == null) {
      diskOutliersStats.put(slowDiskPath, ImmutableMap.of());
    } else {
      diskOutliersStats.put(slowDiskPath, latencies);
    }
  }

  /**
   * 获取需要排除的慢磁盘路径列表。
   * @return 待排除慢磁盘列表
   */
  public List<String> getSlowDisksToExclude() {
    return slowDisksToExclude;
  }

  public int getMaxSlowDisksToExclude() {
    return maxSlowDisksToExclude;
  }

  public void setMaxSlowDisksToExclude(int maxSlowDisksToExclude) {
    this.maxSlowDisksToExclude = maxSlowDisksToExclude;
  }

  /**
   * 设置慢磁盘检测低阈值，同时更新离群点检测器配置。
   * @param thresholdMs 低阈值（毫秒）
   */
  public void setLowThresholdMs(long thresholdMs) {
    Preconditions.checkArgument(thresholdMs > 0,
        DFS_DATANODE_SLOWDISK_LOW_THRESHOLD_MS_KEY + " should be larger than 0");
    lowThresholdMs = thresholdMs;
    this.slowDiskDetector.setLowThresholdMs(thresholdMs);
  }

  public long getLowThresholdMs() {
    return lowThresholdMs;
  }

  /**
   * 设置离群点检测最小磁盘数，同时更新离群点检测器配置。
   * @param minDisks 最小磁盘数
   */
  public void setMinOutlierDetectionDisks(long minDisks) {
    Preconditions.checkArgument(minDisks > 0,
        DFS_DATANODE_MIN_OUTLIER_DETECTION_DISKS_KEY + " should be larger than 0");
    minOutlierDetectionDisks = minDisks;
    this.slowDiskDetector.setMinNumResources(minDisks);
  }

  public long getMinOutlierDetectionDisks() {
    return minOutlierDetectionDisks;
  }

  @VisibleForTesting
  public OutlierDetector getSlowDiskDetector() {
    return this.slowDiskDetector;
  }
}