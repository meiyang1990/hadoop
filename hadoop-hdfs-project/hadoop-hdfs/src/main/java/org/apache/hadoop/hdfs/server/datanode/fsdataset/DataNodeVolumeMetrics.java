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
package org.apache.hadoop.hdfs.server.datanode.fsdataset;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableQuantiles;
import org.apache.hadoop.metrics2.lib.MutableRate;

import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 文件级注释：DataNode单个存储卷的IO相关指标管理类，负责维护存储卷各类IO操作的统计信息，并通过Hadoop metrics2系统对外发布。
 * 此类为每个DataNode存储卷维护独立的指标集，用于监控磁盘IO性能和错误率，辅助排查存储层面的性能问题。
 *
 * This class is for maintaining Datanode Volume IO related statistics and
 * publishing them through the metrics interfaces.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
@Metrics(name = "DataNodeVolume", about = "DataNode Volume metrics",
    context = "dfs")
public class DataNodeVolumeMetrics {
  private final MetricsRegistry registry = new MetricsRegistry("FsVolume");

  @Metric(value = {"VolumeName", "Current VolumeName"}, type = Metric.Type.TAG)
  public String getVolumeName() {
    // 正则提取存储卷真实名称，去除前缀
    Pattern pattern = Pattern.compile("(?:DataNodeVolume-|UndefinedDataNodeVolume)(.*)");
    Matcher matcher = pattern.matcher(name);
    return matcher.find() ? matcher.group(1) : name;
  }

  @Metric("number of metadata operations")
  private MutableCounterLong totalMetadataOperations;
  @Metric("metadata operation rate")
  private MutableRate metadataOperationRate;
  private MutableQuantiles[] metadataOperationLatencyQuantiles;

  @Metric("number of data file io operations")
  private MutableCounterLong totalDataFileIos;
  @Metric("data file io operation rate")
  private MutableRate dataFileIoRate;
  private MutableQuantiles[] dataFileIoLatencyQuantiles;

  @Metric("file io flush rate")
  private MutableRate flushIoRate;
  private MutableQuantiles[] flushIoLatencyQuantiles;

  @Metric("file io sync rate")
  private MutableRate syncIoRate;
  private MutableQuantiles[] syncIoLatencyQuantiles;

  @Metric("file io read rate")
  private MutableRate readIoRate;
  private MutableQuantiles[] readIoLatencyQuantiles;

  @Metric("file io write rate")
  private MutableRate writeIoRate;
  private MutableQuantiles[] writeIoLatencyQuantiles;

  @Metric("file io transfer rate")
  private MutableRate transferIoRate;
  private MutableQuantiles[] transferIoLatencyQuantiles;

  @Metric("file io nativeCopy rate")
  private MutableRate nativeCopyIoRate;
  private MutableQuantiles[] nativeCopyIoLatencyQuantiles;

  @Metric("number of file io errors")
  private MutableCounterLong totalFileIoErrors;
  @Metric("file io error rate")
  private MutableRate fileIoErrorRate;

  /**
   * 获取元数据操作总次数。
   * @return 元数据操作总次数
   */
  public long getTotalMetadataOperations() {
    return totalMetadataOperations.value();
  }

  // Based on metadataOperationRate
  /**
   * 获取元数据操作采样数。
   * @return 元数据操作采样数
   */
  public long getMetadataOperationSampleCount() {
    return metadataOperationRate.lastStat().numSamples();
  }

  /**
   * 获取元数据操作平均耗时。
   * @return 元数据操作平均耗时
   */
  public double getMetadataOperationMean() {
    return metadataOperationRate.lastStat().mean();
  }

  /**
   * 获取元数据操作耗时标准差。
   * @return 元数据操作耗时标准差
   */
  public double getMetadataOperationStdDev() {
    return metadataOperationRate.lastStat().stddev();
  }

  /**
   * 获取数据文件IO总次数。
   * @return 数据文件IO总次数
   */
  public long getTotalDataFileIos() {
    return totalDataFileIos.value();
  }

  // Based on dataFileIoRate
  /**
   * 获取数据文件IO采样数。
   * @return 数据文件IO采样数
   */
  public long getDataFileIoSampleCount() {
    return dataFileIoRate.lastStat().numSamples();
  }

  /**
   * 获取数据文件IO平均耗时。
   * @return 数据文件IO平均耗时
   */
  public double getDataFileIoMean() {
    return dataFileIoRate.lastStat().mean();
  }

  /**
   * 获取数据文件IO耗时标准差。
   * @return 数据文件IO耗时标准差
   */
  public double getDataFileIoStdDev() {
    return dataFileIoRate.lastStat().stddev();
  }

  // Based on flushIoRate
  /**
   * 获取flush IO操作采样数。
   * @return flush IO操作采样数
   */
  public long getFlushIoSampleCount() {
    return flushIoRate.lastStat().numSamples();
  }

  /**
   * 获取flush IO平均耗时。
   * @return flush IO平均耗时
   */
  public double getFlushIoMean() {
    return flushIoRate.lastStat().mean();
  }

  /**
   * 获取flush IO耗时标准差。
   * @return flush IO耗时标准差
   */
  public double getFlushIoStdDev() {
    return flushIoRate.lastStat().stddev();
  }

  // Based on syncIoRate
  /**
   * 获取sync IO操作采样数。
   * @return sync IO操作采样数
   */
  public long getSyncIoSampleCount() {
    return syncIoRate.lastStat().numSamples();
  }

  /**
   * 获取sync IO平均耗时。
   * @return sync IO平均耗时
   */
  public double getSyncIoMean() {
    return syncIoRate.lastStat().mean();
  }

  /**
   * 获取sync IO耗时标准差。
   * @return sync IO耗时标准差
   */
  public double getSyncIoStdDev() {
    return syncIoRate.lastStat().stddev();
  }

  // Based on readIoRate
  /**
   * 获取读IO操作采样数。
   * @return 读IO操作采样数
   */
  public long getReadIoSampleCount() {
    return readIoRate.lastStat().numSamples();
  }

  /**
   * 获取读IO平均耗时。
   * @return 读IO平均耗时
   */
  public double getReadIoMean() {
    return readIoRate.lastStat().mean();
  }

  /**
   * 获取读IO耗时标准差。
   * @return 读IO耗时标准差
   */
  public double getReadIoStdDev() {
    return readIoRate.lastStat().stddev();
  }

  // Based on writeIoRate
  /**
   * 获取写IO操作采样数。
   * @return 写IO操作采样数
   */
  public long getWriteIoSampleCount() {
    return writeIoRate.lastStat().numSamples();
  }

  /**
   * 获取写IO平均耗时。
   * @return 写IO平均耗时
   */
  public double getWriteIoMean() {
    return writeIoRate.lastStat().mean();
  }

  /**
   * 获取写IO耗时标准差。
   * @return 写IO耗时标准差
   */
  public double getWriteIoStdDev() {
    return writeIoRate.lastStat().stddev();
  }

  // Based on transferIoRate
  /**
   * 获取transfer IO操作采样数。
   * @return transfer IO操作采样数
   */
  public long getTransferIoSampleCount() {
    return transferIoRate.lastStat().numSamples();
  }

  /**
   * 获取transfer IO平均耗时。
   * @return transfer IO平均耗时
   */
  public double getTransferIoMean() {
    return transferIoRate.lastStat().mean();
  }

  /**
   * 获取transfer IO耗时标准差。
   * @return transfer IO耗时标准差
   */
  public double getTransferIoStdDev() {
    return transferIoRate.lastStat().stddev();
  }

  /**
   * 获取transfer IO延迟分位数数组。
   * @return transfer IO延迟分位数数组
   */
  public MutableQuantiles[] getTransferIoQuantiles() {
    return transferIoLatencyQuantiles;
  }

  // Based on nativeCopyIoRate
  /**
   * 获取本地拷贝IO操作采样数。
   * @return 本地拷贝IO操作采样数
   */
  public long getNativeCopyIoSampleCount() {
    return nativeCopyIoRate.lastStat().numSamples();
  }

  /**
   * 获取本地拷贝IO平均耗时。
   * @return 本地拷贝IO平均耗时
   */
  public double getNativeCopyIoMean() {
    return nativeCopyIoRate.lastStat().mean();
  }

  /**
   * 获取本地拷贝IO耗时标准差。
   * @return 本地拷贝IO耗时标准差
   */
  public double getNativeCopyIoStdDev() {
    return nativeCopyIoRate.lastStat().stddev();
  }

  /**
   * 获取本地拷贝IO延迟分位数数组。
   * @return 本地拷贝IO延迟分位数数组
   */
  public MutableQuantiles[] getNativeCopyIoQuantiles() {
    return nativeCopyIoLatencyQuantiles;
  }

  /**
   * 获取文件IO错误总次数。
   * @return 文件IO错误总次数
   */
  public long getTotalFileIoErrors() {
    return totalFileIoErrors.value();
  }

  // Based on fileIoErrorRate
  /**
   * 获取文件IO错误采样数。
   * @return 文件IO错误采样数
   */
  public long getFileIoErrorSampleCount() {
    return fileIoErrorRate.lastStat().numSamples();
  }

  /**
   * 获取文件IO错误平均耗时。
   * @return 文件IO错误平均耗时
   */
  public double getFileIoErrorMean() {
    return fileIoErrorRate.lastStat().mean();
  }

  /**
   * 获取文件IO错误耗时标准差。
   * @return 文件IO错误耗时标准差
   */
  public double getFileIoErrorStdDev() {
    return fileIoErrorRate.lastStat().stddev();
  }

  private final String name;
  private final MetricsSystem ms;

  /**
   * 构造DataNode存储卷指标实例，初始化各类IO延迟分位数统计数组。
   * @param metricsSystem 指标系统实例
   * @param volumeName 存储卷名称
   * @param intervals 分位数统计间隔数组（单位秒）
   */
  public DataNodeVolumeMetrics(final MetricsSystem metricsSystem,
      final String volumeName, final int[] intervals) {
    this.ms = metricsSystem;
    this.name = volumeName;
    final int len = intervals.length;
    // 为每类IO操作创建对应长度的分位数数组
    metadataOperationLatencyQuantiles = new MutableQuantiles[len];
    dataFileIoLatencyQuantiles = new MutableQuantiles[len];
    flushIoLatencyQuantiles = new MutableQuantiles[len];
    syncIoLatencyQuantiles = new MutableQuantiles[len];
    readIoLatencyQuantiles = new MutableQuantiles[len];
    writeIoLatencyQuantiles = new MutableQuantiles[len];
    transferIoLatencyQuantiles = new MutableQuantiles[len];
    nativeCopyIoLatencyQuantiles = new MutableQuantiles[len];
    // 遍历每个间隔，在注册表中创建对应分位数统计对象
    for (int i = 0; i < len; i++) {
      int interval = intervals[i];
      metadataOperationLatencyQuantiles[i] = registry.newQuantiles(
          "metadataOperationLatency" + interval + "s",
          "Metadata Operation Latency in ms", "ops", "latency", interval);
      dataFileIoLatencyQuantiles[i] = registry.newQuantiles(
          "dataFileIoLatency" + interval + "s",
          "Data File Io Latency in ms", "ops", "latency", interval);
      flushIoLatencyQuantiles[i] = registry.newQuantiles(
          "flushIoLatency" + interval + "s",
          "Data flush Io Latency in ms", "ops", "latency", interval);
      syncIoLatencyQuantiles[i] = registry.newQuantiles(
          "syncIoLatency" + interval + "s",
          "Data sync Io Latency in ms", "ops", "latency", interval);
      readIoLatencyQuantiles[i] = registry.newQuantiles(
          "readIoLatency" + interval + "s",
          "Data read Io Latency in ms", "ops", "latency", interval);
      writeIoLatencyQuantiles[i] = registry.newQuantiles(
          "writeIoLatency" + interval + "s",
          "Data write Io Latency in ms", "ops", "latency", interval);
      transferIoLatencyQuantiles[i] = registry.newQuantiles(
          "transferIoLatency" + interval + "s",
          "Data transfer Io Latency in ms", "ops", "latency", interval);
      nativeCopyIoLatencyQuantiles[i] = registry.newQuantiles(
          "nativeCopyIoLatency" + interval + "s",
          "Data nativeCopy Io Latency in ms", "ops", "latency", interval);
    }
  }

  /**
   * 工厂方法，根据配置创建并注册DataNode存储卷指标实例。
   * @param conf Hadoop配置对象
   * @param volumeName 存储卷名称
   * @return 创建完成的DataNodeVolumeMetrics实例
   */
  public static DataNodeVolumeMetrics create(final Configuration conf,
      final String volumeName) {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    // 生成指标名称，处理空名称和特殊字符替换
    String name = "DataNodeVolume-"+ (volumeName.isEmpty()
        ? "UndefinedDataNodeVolume"+ ThreadLocalRandom.current().nextInt()
        : volumeName.replace(':', '-'));

    // Percentile measurement is off by default, by watching no intervals
    // 从配置获取分位数统计间隔，未配置则默认关闭分位数统计
    int[] intervals =
        conf.getInts(DFSConfigKeys.DFS_METRICS_PERCENTILES_INTERVALS_KEY);
    return ms.register(name, null, new DataNodeVolumeMetrics(ms, name,
        intervals));
  }

  /**
   * 获取指标实例名称。
   * @return 指标实例名称
   */
  public String name() {
    return name;
  }

  /**
   * 从指标系统注销当前存储卷指标。
   */
  public void unRegister() {
    ms.unregisterSource(name);
  }

  /**
   * 添加一次元数据操作的延迟统计。
   * @param latency 操作耗时（毫秒）
   */
  public void addMetadataOperationLatency(final long latency) {
    totalMetadataOperations.incr();
    metadataOperationRate.add(latency);
    for (MutableQuantiles q : metadataOperationLatencyQuantiles) {
      q.add(latency);
    }
  }

  /**
   * 添加一次数据文件IO的延迟统计。
   * @param latency 操作耗时（毫秒）
   */
  public void addDataFileIoLatency(final long latency) {
    totalDataFileIos.incr();
    dataFileIoRate.add(latency);
    for (MutableQuantiles q : dataFileIoLatencyQuantiles) {
      q.add(latency);
    }
  }

  /**
   * 添加一次sync IO的延迟统计。
   * @param latency 操作耗时（毫秒）
   */
  public void addSyncIoLatency(final long latency) {
    syncIoRate.add(latency);
    for (MutableQuantiles q : syncIoLatencyQuantiles) {
      q.add(latency);
    }
  }

  /**
   * 添加一次flush IO的延迟统计。
   * @param latency