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

package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.classification.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.common.Util;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.DataNodeVolumeMetrics;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.util.Time;

import javax.annotation.Nullable;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 文件路径：hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/ProfilingFileIoEvents.java
 * 数据节点磁盘IO性能分析器，对数据节点卷上的元数据和数据IO操作进行性能采样统计，
 * 将延迟数据上报到对应卷的指标系统，用于监控磁盘IO性能。
 * Profiles the performance of the metadata and data related operations on
 * datanode volumes.
 */
@InterfaceAudience.Private
class ProfilingFileIoEvents {
  static final Logger LOG =
      LoggerFactory.getLogger(ProfilingFileIoEvents.class);

  /** 是否启用IO性能分析采样 */
  private volatile boolean isEnabled;
  /** 采样范围上限，随机数小于该值则命中采样，按采样百分比换算得到 */
  private volatile int sampleRangeMax;

  /**
   * 构造IO性能分析器，从配置中加载采样百分比参数并初始化状态。
   * @param conf Hadoop配置对象，为null时直接禁用采样
   */
  public ProfilingFileIoEvents(@Nullable Configuration conf) {
    if (conf != null) {
      int fileIOSamplingPercentage = conf.getInt(
          DFSConfigKeys.DFS_DATANODE_FILEIO_PROFILING_SAMPLING_PERCENTAGE_KEY,
          DFSConfigKeys
              .DFS_DATANODE_FILEIO_PROFILING_SAMPLING_PERCENTAGE_DEFAULT);
      setSampleRangeMax(fileIOSamplingPercentage);
    } else {
      isEnabled = false;
      sampleRangeMax = 0;
    }
  }

  /**
   * 元数据操作开始前的处理，记录操作开始时间用于后续延迟计算。
   * @param volume 目标数据卷
   * @param op IO操作类型
   * @return 操作开始时间戳（纳秒），未采样时返回0
   */
  public long beforeMetadataOp(@Nullable FsVolumeSpi volume,
      FileIoProvider.OPERATION op) {
    if (isEnabled) {
      DataNodeVolumeMetrics metrics = getVolumeMetrics(volume);
      if (metrics != null) {
        return Time.monotonicNow();
      }
    }
    return 0;
  }

  /**
   * 元数据操作完成后的处理，计算操作延迟并上报到指标系统。
   * @param volume 目标数据卷
   * @param op IO操作类型
   * @param begin 操作开始时间戳
   */
  public void afterMetadataOp(@Nullable FsVolumeSpi volume,
      FileIoProvider.OPERATION op, long begin) {
    if (isEnabled) {
      DataNodeVolumeMetrics metrics = getVolumeMetrics(volume);
      if (metrics != null) {
        metrics.addMetadataOperationLatency(Time.monotonicNow() - begin);
      }
    }
  }

  /**
   * 文件数据IO操作开始前的处理，按采样概率判断是否命中采样，命中则记录开始时间。
   * @param volume 目标数据卷
   * @param op IO操作类型
   * @param len IO操作数据长度
   * @return 操作开始时间戳，未命中采样返回0
   */
  public long beforeFileIo(@Nullable FsVolumeSpi volume,
      FileIoProvider.OPERATION op, long len) {
    if (isEnabled && ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE) < sampleRangeMax) {
      DataNodeVolumeMetrics metrics = getVolumeMetrics(volume);
      if (metrics != null) {
        return Time.monotonicNow();
      }
    }
    return 0;
  }

  /**
   * 文件数据IO操作完成后的处理，计算操作延迟，按操作类型分类上报到指标系统。
   * @param volume 目标数据卷
   * @param op IO操作类型
   * @param begin 操作开始时间戳
   * @param len IO操作数据长度
   */
  public void afterFileIo(@Nullable FsVolumeSpi volume,
      FileIoProvider.OPERATION op, long begin, long len) {
    if (isEnabled && begin != 0) {
      DataNodeVolumeMetrics metrics = getVolumeMetrics(volume);
      if (metrics != null) {
        long latency = Time.monotonicNow() - begin;
        metrics.addDataFileIoLatency(latency);
        // 按操作类型分别上报延迟指标
        switch (op) {
        case SYNC:
          metrics.addSyncIoLatency(latency);
          break;
        case FLUSH:
          metrics.addFlushIoLatency(latency);
          break;
        case READ:
          metrics.addReadIoLatency(latency);
          break;
        case WRITE:
          metrics.addWriteIoLatency(latency);
          break;
        case TRANSFER:
          metrics.addTransferIoLatency(latency);
          break;
        case NATIVE_COPY:
          metrics.addNativeCopyIoLatency(latency);
          break;
        default:
        }
      }
    }
  }

  /**
   * IO操作失败处理，记录失败操作的延迟并增加失败指标计数。
   * @param volume 目标数据卷
   * @param begin 操作开始时间戳
   */
  public void onFailure(@Nullable FsVolumeSpi volume, long begin) {
    if (isEnabled) {
      DataNodeVolumeMetrics metrics = getVolumeMetrics(volume);
      if (metrics != null) {
        metrics.addFileIoError(Time.monotonicNow() - begin);
      }
    }
  }

  /**
   * 获取指定数据卷的指标对象。
   * @param volume 目标数据卷
   * @return 数据卷对应的指标对象，禁用采样或volume为null时返回null
   */
  private DataNodeVolumeMetrics getVolumeMetrics(final FsVolumeSpi volume) {
    if (isEnabled) {
      if (volume != null) {
        return volume.getMetrics();
      }
    }
    return null;
  }

  /**
   * 设置采样百分比，计算采样范围上限并更新启用状态。
   * @param fileIOSamplingPercentage IO采样百分比（0-100，超过100会被截断为100）
   */
  public void setSampleRangeMax(int fileIOSamplingPercentage) {
    isEnabled = Util.isDiskStatsEnabled(fileIOSamplingPercentage);
    if (fileIOSamplingPercentage > 100) {
      LOG.warn(DFSConfigKeys
          .DFS_DATANODE_FILEIO_PROFILING_SAMPLING_PERCENTAGE_KEY +
          " value cannot be more than 100. Setting value to 100");
      fileIOSamplingPercentage = 100;
    }
    sampleRangeMax = (int) ((double) fileIOSamplingPercentage / 100 *
        Integer.MAX_VALUE);
  }

  /**
   * 获取磁盘统计采样是否启用，仅用于单元测试。
   * @return 采样启用状态
   */
  @VisibleForTesting
  public boolean getDiskStatsEnabled() {
    return isEnabled;
  }

  /**
   * 获取采样范围上限，仅用于单元测试。
   * @return 采样范围上限值
   */
  @VisibleForTesting
  public int getSampleRangeMax() {
    return sampleRangeMax;
  }
}