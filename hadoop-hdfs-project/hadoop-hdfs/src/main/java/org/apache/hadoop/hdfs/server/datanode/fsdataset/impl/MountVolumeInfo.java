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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeReference;

import java.nio.channels.ClosedChannelException;
import java.util.EnumMap;
import java.util.Map;

/**
 * 挂载卷信息封装类，用于MountVolumeMap存储单个挂载点下的多存储类型卷详细信息。
 * 支持同一挂载点按存储类型（如DISK/ARCHIVE）划分容量，管理不同存储类型卷的映射和容量分配比例。
 */
@InterfaceAudience.Private
class MountVolumeInfo {
  /** 存储类型到对应数据卷的映射 */
  private final EnumMap<StorageType, FsVolumeImpl>
      storageTypeVolumeMap;
  /** 存储类型到容量分配比例的映射 */
  private final EnumMap<StorageType, Double>
      capacityRatioMap;
  /** ARCHIVE存储类型默认预留容量比例 */
  private double reservedForArchiveDefault;

  /**
   * 构造方法，从配置中初始化ARCHIVE默认预留容量比例并做边界校验。
   * @param conf Hadoop配置对象
   */
  MountVolumeInfo(Configuration conf) {
    storageTypeVolumeMap = new EnumMap<>(StorageType.class);
    capacityRatioMap = new EnumMap<>(StorageType.class);
    reservedForArchiveDefault = conf.getDouble(
        DFSConfigKeys.DFS_DATANODE_RESERVE_FOR_ARCHIVE_DEFAULT_PERCENTAGE,
        DFSConfigKeys
            .DFS_DATANODE_RESERVE_FOR_ARCHIVE_DEFAULT_PERCENTAGE_DEFAULT);
    // 超过100%时截断为100%
    if (reservedForArchiveDefault > 1) {
      FsDatasetImpl.LOG.warn("Value of reserve-for-archival is > 100%." +
          " Setting it to 100%.");
      reservedForArchiveDefault = 1;
    }
    // 小于0时截断为0
    if (reservedForArchiveDefault < 0) {
      FsDatasetImpl.LOG.warn("Value of reserve-for-archival is < 0." +
          " Setting it to 0.0");
      reservedForArchiveDefault = 0;
    }
  }

  /**
   * 根据存储类型获取对应数据卷的引用，增加引用计数。
   * @param storageType 目标存储类型
   * @return 数据卷引用，获取失败返回null
   */
  FsVolumeReference getVolumeRef(StorageType storageType) {
    try {
      FsVolumeImpl volumeImpl = storageTypeVolumeMap
          .getOrDefault(storageType, null);
      if (volumeImpl != null) {
        return volumeImpl.obtainReference();
      }
    } catch (ClosedChannelException e) {
      FsDatasetImpl.LOG.warn("Volume closed when getting volume" +
          " by storage type: " + storageType);
    }
    return null;
  }

  /**
   * 获取指定存储类型的容量分配比例。
   * @param storageType 目标存储类型
   * @return 容量分配比例（0~1之间）
   */
  double getCapacityRatio(StorageType storageType) {
    // 如果已配置当前存储类型比例，直接返回
    if (capacityRatioMap.containsKey(storageType)) {
      return capacityRatioMap.get(storageType);
    }
    // 如果已配置其他存储类型比例，当前存储类型使用剩余容量
    if (!capacityRatioMap.isEmpty()) {
      double leftOver = 1;
      for (Map.Entry<StorageType, Double> e : capacityRatioMap.entrySet()) {
        leftOver -= e.getValue();
      }
      return leftOver;
    }
    // 默认规则：存在多个存储类型时，ARCHIVE使用默认预留比例，DISK使用剩余容量
    if (storageTypeVolumeMap.containsKey(storageType)
        && storageTypeVolumeMap.size() > 1) {
      if (storageType == StorageType.ARCHIVE) {
        return reservedForArchiveDefault;
      } else if (storageType == StorageType.DISK) {
        return 1 - reservedForArchiveDefault;
      }
    }
    // 单存储类型场景使用全部容量
    return 1;
  }

  /**
   * 添加数据卷到当前挂载点，同一存储类型只能存在一个卷。
   * @param volume 待添加的数据卷
   * @return 添加成功返回true，已存在同类型卷返回false
   */
  boolean addVolume(FsVolumeImpl volume) {
    if (storageTypeVolumeMap.containsKey(volume.getStorageType())) {
      FsDatasetImpl.LOG.error("Found storage type already exist." +
          " Skipping for now. Please check disk configuration");
      return false;
    }
    storageTypeVolumeMap.put(volume.getStorageType(), volume);
    return true;
  }

  /**
   * 移除指定数据卷，同时清除容量比例配置。
   * @param target 待移除的数据卷
   */
  void removeVolume(FsVolumeImpl target) {
    storageTypeVolumeMap.remove(target.getStorageType());
    capacityRatioMap.remove(target.getStorageType());
  }

  /**
   * 设置指定存储类型的自定义容量分配比例，校验总容量不超过100%。
   * @param storageType 目标存储类型
   * @param capacityRatio 期望分配的容量比例
   * @return 设置成功返回true，总比例超过100%返回false
   */
  boolean setCapacityRatio(StorageType storageType,
      double capacityRatio) {
    double leftover = 1;
    // 计算已有其他存储类型占用的总比例，得到剩余可用比例
    for (Map.Entry<StorageType, Double> e : capacityRatioMap.entrySet()) {
      if (e.getKey() != storageType) {
        leftover -= e.getValue();
      }
    }
    // 剩余比例不足，设置失败
    if (leftover < capacityRatio) {
      return false;
    }
    capacityRatioMap.put(storageType, capacityRatio);
    return true;
  }

  /**
   * 获取当前挂载点下已添加的数据卷数量。
   * @return 数据卷数量
   */
  int size() {
    return storageTypeVolumeMap.size();
  }
}