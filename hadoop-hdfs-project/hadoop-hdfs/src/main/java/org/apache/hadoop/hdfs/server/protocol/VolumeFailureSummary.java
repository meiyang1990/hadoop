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
package org.apache.hadoop.hdfs.server.protocol;

/**
 * DataNode节点数据卷故障信息汇总类，用于在DataNode向NameNode上报状态时传递本地卷故障情况
 */
public class VolumeFailureSummary {
  private final String[] failedStorageLocations;
  private final long lastVolumeFailureDate;
  private final long estimatedCapacityLostTotal;

  /**
   * 构造DataNode卷故障信息汇总对象
   *
   * @param failedStorageLocations 已发生故障的存储位置列表
   * @param lastVolumeFailureDate 最近一次卷故障发生的时间，单位为从纪元开始的毫秒数
   * @param estimatedCapacityLostTotal 故障导致损失的总存储容量估计值，单位为字节
   */
  public VolumeFailureSummary(String[] failedStorageLocations,
      long lastVolumeFailureDate, long estimatedCapacityLostTotal) {
    this.failedStorageLocations = failedStorageLocations;
    this.lastVolumeFailureDate = lastVolumeFailureDate;
    this.estimatedCapacityLostTotal = estimatedCapacityLostTotal;
  }

  /**
   * 获取所有已故障的存储位置列表（已排序）
   *
   * @return 已故障存储位置数组，已排序
   */
  public String[] getFailedStorageLocations() {
    return this.failedStorageLocations;
  }

  /**
   * 获取最近一次卷故障的时间戳
   *
   * @return 最近一次卷故障发生时间，单位为从纪元开始的毫秒数
   */
  public long getLastVolumeFailureDate() {
    return this.lastVolumeFailureDate;
  }

  /**
   * 获取故障导致损失的总存储容量估计值
   * 该值为估计值：如果卷故障发生前未成功获取到容量信息，则无法得到精确值
   *
   * @return 损失的总容量估计值，单位为字节
   */
  public long getEstimatedCapacityLostTotal() {
    return this.estimatedCapacityLostTotal;
  }
}