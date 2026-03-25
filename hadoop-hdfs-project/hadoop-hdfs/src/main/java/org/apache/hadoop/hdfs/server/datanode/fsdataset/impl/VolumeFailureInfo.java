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
package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import org.apache.hadoop.hdfs.server.datanode.StorageLocation;

/**
 * 文件存储卷故障信息记录类，用于记录DataNode上单个数据卷的故障详情，
 * 供DataNode故障卷管理和容量统计使用。
 * 追踪数据卷的故障相关信息。
 */
final class VolumeFailureInfo {
  private final StorageLocation failedStorageLocation;
  private final long failureDate;
  private final long estimatedCapacityLost;

  /**
   * 构造VolumeFailureInfo，适用于无法估算故障损失容量的场景，
   * 通常用于DataNode启动时就直接失败的卷，启动前无法获取其容量信息。
   *
   * @param failedStorageLocation 发生故障的存储位置
   * @param failureDate 故障发生时间，毫秒级时间戳（自纪元起）
   */
  public VolumeFailureInfo(StorageLocation failedStorageLocation,
      long failureDate) {
    this(failedStorageLocation, failureDate, 0);
  }

  /**
   * 构造VolumeFailureInfo，包含完整的故障信息。
   *
   * @param failedStorageLocation 发生故障的存储位置
   * @param failureDate 故障发生时间，毫秒级时间戳（自纪元起）
   * @param estimatedCapacityLost 预估损失容量，单位字节
   */
  public VolumeFailureInfo(StorageLocation failedStorageLocation,
      long failureDate, long estimatedCapacityLost) {
    this.failedStorageLocation = failedStorageLocation;
    this.failureDate = failureDate;
    this.estimatedCapacityLost = estimatedCapacityLost;
  }

  /**
   * 获取发生故障的存储位置。
   *
   * @return 发生故障的存储位置对象
   */
  public StorageLocation getFailedStorageLocation() {
    return this.failedStorageLocation;
  }

  /**
   * 获取故障发生时间。
   *
   * @return 故障发生时间，毫秒级时间戳（自纪元起）
   */
  public long getFailureDate() {
    return this.failureDate;
  }

  /**
   * 获取故障导致的预估损失容量，该值为预估值，
   * 当卷在获取容量前就发生故障时无法得到准确值。
   *
   * @return 预估损失容量，单位字节
   */
  public long getEstimatedCapacityLost() {
    return this.estimatedCapacityLost;
  }
}