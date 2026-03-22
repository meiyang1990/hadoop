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

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.metrics2.MetricsSource;

/**
 * 数据节点FSDataset存储状态的JMX MBean接口，用于暴露存储指标给监控系统
 * 遵循JMX命名规范，可通过JMX发布存储相关运行指标
 * 本接口保持稳定，未使用MetricsDynamicMBeanBase实现，直接以接口形式发布
 * <p>
 * 数据节点运行时统计信息在另一个MBean中发布
 * @see org.apache.hadoop.hdfs.server.datanode.metrics.DataNodeMetrics
 *
 */
@InterfaceAudience.Private
public interface FSDatasetMBean extends MetricsSource {
  
  /**
   * 获取指定块池已使用的存储空间大小（字节）
   * @param bpid 块池ID
   * @return 块池已使用空间字节数
   * @throws IO异常
   */  
  public long getBlockPoolUsed(String bpid) throws IOException;
  
  /**
   * 获取数据节点DFS已使用的总存储空间大小（字节）
   * @return DFS已使用总空间字节数
   * @throws IO异常
   */  
  public long getDfsUsed() throws IOException;
    
  /**
   * 获取存储总容量（字节，包含已用和未用空间）
   * @return 存储总容量字节数
   * @throws IO异常
   */
  public long getCapacity() throws IOException;

  /**
   * 获取剩余可用存储空间大小（字节）
   * @return 剩余可用空间字节数
   * @throws IO异常
   */
  public long getRemaining() throws IOException;
  
  /**
   * 获取底层存储的存储标识信息
   * @return 底层存储标识字符串
   */
  public String getStorageInfo();

  /**
   * 获取数据节点上故障卷的数量
   * @return 故障卷数量
   */
  public int getNumFailedVolumes();

  /**
   * 获取排序后的所有故障存储路径数组
   * @return 排序后的故障存储路径数组
   */
  String[] getFailedStorageLocations();

  /**
   * 获取上次卷故障发生的时间戳（毫秒，从纪元开始计算）
   * @return 上次卷故障的时间戳
   */
  long getLastVolumeFailureDate();

  /**
   * 获取因卷故障损失的总容量估算值（字节）
   * @return 容量损失估算值（字节）
   */
  long getEstimatedCapacityLostTotal();

  /**
   * 获取数据节点已使用的缓存大小（字节）
   * @return 已使用缓存字节数
   */
  public long getCacheUsed();

  /**
   * 获取数据节点缓存总容量（字节）
   * @return 缓存总容量字节数
   */
  public long getCacheCapacity();

  /**
   * 获取已缓存的块数量
   * @return 已缓存块数量
   */
  public long getNumBlocksCached();

  /**
   * 获取缓存失败的块数量
   * @return 缓存失败块数量
   */
  public long getNumBlocksFailedToCache();

  /**
   * 获取解除缓存失败的块数量
   * @return 解除缓存失败块数量
   */
  public long getNumBlocksFailedToUncache();

  /**
   * 获取目录扫描器上次成功完成扫描的时间戳（毫秒）
   * @return 上次目录扫描成功完成的时间戳
   */
  long getLastDirScannerFinishTime();

  /**
   * 获取等待中和运行中的异步磁盘操作数量
   * @return 待处理异步删除操作数量
   */
  long getPendingAsyncDeletions();
}