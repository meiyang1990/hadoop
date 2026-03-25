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
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.datanode.metrics;

import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsTag;
import org.apache.hadoop.metrics2.lib.Interns;

import java.io.IOException;

/**
 * DataNode指标收集辅助工具类，用于将FSDatasetMBean暴露的存储指标转换为Metrics2系统可采集的格式。
 * 承担DataNode存储相关指标的统一收集上报工作，为监控系统提供DataNode存储状态数据。
 */
public class DataNodeMetricHelper {

  /**
   * 从FSDatasetMBean中收集DataNode存储相关指标，并添加到Metrics2收集器中。
   * 作为Metrics2指标源的辅助函数，负责将MBean指标转换为标准Metrics2格式。
   * @param collector 指标收集器，用于接收转换后的指标
   * @param beanClass 实现了FSDatasetMBean接口的指标数据源对象
   * @param context 指标所属的metrics2上下文标识
   * @throws IOException 当数据源对象为null时抛出异常
   */
  public static void getMetrics(MetricsCollector collector,
                                FSDatasetMBean beanClass, String context)
    throws IOException {

    // 非空检查，确保数据源存在
    if (beanClass == null) {
      throw new IOException("beanClass cannot be null");
    }

    // 获取数据源类全限定名作为指标记录名称
    String className = beanClass.getClass().getName();

    collector.addRecord(className)
      // 设置指标上下文
      .setContext(context)
      // 添加总存储容量指标
      .addGauge(Interns.info("Capacity", "Total storage capacity"),
        beanClass.getCapacity())
      // 添加DFS已使用存储容量指标
      .addGauge(Interns.info("DfsUsed", "Total bytes used by dfs datanode"),
        beanClass.getDfsUsed())
      // 添加剩余可用存储容量指标
      .addGauge(Interns.info("Remaining", "Total bytes of free storage"),
        beanClass.getRemaining())
      // 添加存储ID标签标识当前存储
      .add(new MetricsTag(Interns.info("StorageInfo", "Storage ID"),
        beanClass.getStorageInfo()))
      // 添加故障卷数量指标
      .addGauge(Interns.info("NumFailedVolumes", "Number of failed Volumes" +
        " in the data Node"), beanClass.getNumFailedVolumes())
      // 添加最近一次卷故障时间戳指标
      .addGauge(Interns.info("LastVolumeFailureDate", "Last Volume failure in" +
        " milliseconds from epoch"), beanClass.getLastVolumeFailureDate())
      // 添加卷故障导致的总容量损失指标
      .addGauge(Interns.info("EstimatedCapacityLostTotal", "Total capacity lost"
        + " due to volume failure"), beanClass.getEstimatedCapacityLostTotal())
      // 添加已使用缓存容量指标
      .addGauge(Interns.info("CacheUsed", "Datanode cache used in bytes"),
        beanClass.getCacheUsed())
      // 添加缓存总容量指标
      .addGauge(Interns.info("CacheCapacity", "Datanode cache capacity"),
        beanClass.getCacheCapacity())
      // 添加已缓存块数量指标
      .addGauge(Interns.info("NumBlocksCached", "Datanode number" +
        " of blocks cached"), beanClass.getNumBlocksCached())
      // 添加缓存失败块数量指标
      .addGauge(Interns.info("NumBlocksFailedToCache", "Datanode number of " +
        "blocks failed to cache"), beanClass.getNumBlocksFailedToCache())
      // 添加缓存淘汰失败块数量指标
        .addGauge(Interns.info("NumBlocksFailedToUnCache", "Datanode number of" +
          " blocks failed in cache eviction"),
        beanClass.getNumBlocksFailedToUncache())
      // 添加最近一次目录扫描完成时间戳指标
        .addGauge(Interns.info("LastDirectoryScannerFinishTime",
        "Finish time of the last directory scan"), beanClass.getLastDirScannerFinishTime())
      // 添加待处理异步删除操作数量指标
        .addGauge(Interns.info("PendingAsyncDeletions",
            "The count of pending and running asynchronous disk operations"),
            beanClass.getPendingAsyncDeletions());
  }

}