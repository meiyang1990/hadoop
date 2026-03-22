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

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_AVAILABLE_SPACE_VOLUME_CHOOSING_POLICY_BALANCED_SPACE_THRESHOLD_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_AVAILABLE_SPACE_VOLUME_CHOOSING_POLICY_BALANCED_SPACE_THRESHOLD_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_AVAILABLE_SPACE_VOLUME_CHOOSING_POLICY_BALANCED_SPACE_PREFERENCE_FRACTION_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_AVAILABLE_SPACE_VOLUME_CHOOSING_POLICY_BALANCED_SPACE_PREFERENCE_FRACTION_KEY;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.util.DiskChecker.DiskOutOfSpaceException;

/**
 * 基于可用空间的数据节点卷选择策略，在分配新数据块副本时，会根据各卷的剩余可用空间选择目标卷。
 * 默认偏好将副本分配到剩余空间更多的卷，从而实现数据节点内所有卷的可用空间动态平衡。
 * 使用细粒度锁，支持不同存储类型的卷选择操作并发执行。
 */
public class AvailableSpaceVolumeChoosingPolicy<V extends FsVolumeSpi>
    implements VolumeChoosingPolicy<V>, Configurable {
  
  private static final Logger LOG =
      LoggerFactory.getLogger(AvailableSpaceVolumeChoosingPolicy.class);

  /** 按存储类型索引的同步锁数组，支持不同存储类型并发选择卷 */
  private Object[] syncLocks;
  
  /** 随机数生成器，用于概率选择高可用空间卷 */
  private final Random random;
  
  /** 平衡空间阈值，最大最小可用空间差低于该阈值时视为空间平衡，默认从配置读取 */
  private long balancedSpaceThreshold = DFS_DATANODE_AVAILABLE_SPACE_VOLUME_CHOOSING_POLICY_BALANCED_SPACE_THRESHOLD_DEFAULT;
  /** 高可用空间卷的偏好比例，范围0-1，值越大越偏好选择高可用空间卷 */
  private float balancedPreferencePercent = DFS_DATANODE_AVAILABLE_SPACE_VOLUME_CHOOSING_POLICY_BALANCED_SPACE_PREFERENCE_FRACTION_DEFAULT;

  /**
   * 带随机数生成器的构造函数，用于测试注入
   * @param random 随机数生成器实例
   */
  AvailableSpaceVolumeChoosingPolicy(Random random) {
    this.random = random;
    initLocks();
  }

  /**
   * 默认构造函数，使用默认随机数生成器
   */
  public AvailableSpaceVolumeChoosingPolicy() {
    this(new Random());
  }

  /**
   * 初始化按存储类型分类的同步锁数组
   */
  private void initLocks() {
    // 根据存储类型枚举数量创建对应长度的锁数组
    int numStorageTypes = StorageType.values().length;
    syncLocks = new Object[numStorageTypes];
    for (int i = 0; i < numStorageTypes; i++) {
      syncLocks[i] = new Object();
    }
  }

  @Override
  public void setConf(Configuration conf) {
    // 从配置读取平衡空间阈值，使用默认值作为兜底
    balancedSpaceThreshold = conf.getLongBytes(
        DFS_DATANODE_AVAILABLE_SPACE_VOLUME_CHOOSING_POLICY_BALANCED_SPACE_THRESHOLD_KEY,
        DFS_DATANODE_AVAILABLE_SPACE_VOLUME_CHOOSING_POLICY_BALANCED_SPACE_THRESHOLD_DEFAULT);
    // 从配置读取高可用空间偏好比例，使用默认值作为兜底
    balancedPreferencePercent = conf.getFloat(
        DFS_DATANODE_AVAILABLE_SPACE_VOLUME_CHOOSING_POLICY_BALANCED_SPACE_PREFERENCE_FRACTION_KEY,
        DFS_DATANODE_AVAILABLE_SPACE_VOLUME_CHOOSING_POLICY_BALANCED_SPACE_PREFERENCE_FRACTION_DEFAULT);
    
    // 打印初始化配置日志
    LOG.info("Available space volume choosing policy initialized: " +
        DFS_DATANODE_AVAILABLE_SPACE_VOLUME_CHOOSING_POLICY_BALANCED_SPACE_THRESHOLD_KEY +
        " = " + balancedSpaceThreshold + ", " +
        DFS_DATANODE_AVAILABLE_SPACE_VOLUME_CHOOSING_POLICY_BALANCED_SPACE_PREFERENCE_FRACTION_KEY +
        " = " + balancedPreferencePercent);

    // 偏好比例大于1.0时打印警告
    if (balancedPreferencePercent > 1.0) {
      LOG.warn("The value of " + DFS_DATANODE_AVAILABLE_SPACE_VOLUME_CHOOSING_POLICY_BALANCED_SPACE_PREFERENCE_FRACTION_KEY +
               " is greater than 1.0 but should be in the range 0.0 - 1.0");
    }

    // 偏好比例小于0.5时打印警告，会导致低可用空间卷获得更多分配
    if (balancedPreferencePercent < 0.5) {
      LOG.warn("The value of " + DFS_DATANODE_AVAILABLE_SPACE_VOLUME_CHOOSING_POLICY_BALANCED_SPACE_PREFERENCE_FRACTION_KEY +
               " is less than 0.5 so volumes with less available disk space will receive more block allocations");
    }
  }
  
  @Override
  public Configuration getConf() {
    // Nothing to do. Only added to fulfill the Configurable contract.
    return null;
  }
  
  // 三种场景下使用的轮询卷选择策略实例
  /** 空间平衡时使用的轮询策略 */
  private final VolumeChoosingPolicy<V> roundRobinPolicyBalanced =
      new RoundRobinVolumeChoosingPolicy<V>();
  /** 选择高可用空间卷时使用的轮询策略 */
  private final VolumeChoosingPolicy<V> roundRobinPolicyHighAvailable =
      new RoundRobinVolumeChoosingPolicy<V>();
  /** 选择低可用空间卷时使用的轮询策略 */
  private final VolumeChoosingPolicy<V> roundRobinPolicyLowAvailable =
      new RoundRobinVolumeChoosingPolicy<V>();

  @Override
  public V chooseVolume(List<V> volumes, long replicaSize, String storageId)
      throws IOException {
    // 无可用卷时直接抛出空间不足异常
    if (volumes.size() < 1) {
      throw new DiskOutOfSpaceException("No more available volumes");
    }
    // 输入卷列表中所有卷存储类型相同，只需取第一个卷的存储类型
    StorageType storageType = volumes.get(0).getStorageType();
    int index = storageType != null ?
            storageType.ordinal() : StorageType.DEFAULT.ordinal();

    // 对当前存储类型加锁，保证同存储类型卷选择线程安全，同时支持不同存储类型并发选择
    synchronized (syncLocks[index]) {
      return doChooseVolume(volumes, replicaSize, storageId);
    }
  }

  /**
   * 实际执行卷选择逻辑，已被对应存储类型的锁保护
   * @param volumes 候选卷列表
   * @param replicaSize 需要分配的副本大小
   * @param storageId 存储ID
   * @return 选中的卷
   * @throws IOException 空间不足时抛出异常
   */
  private V doChooseVolume(final List<V> volumes, long replicaSize,
      String storageId) throws IOException {
    // 封装候选卷，一次性获取所有卷的可用空间
    AvailableSpaceVolumeList volumesWithSpaces =
        new AvailableSpaceVolumeList(volumes);
    
    // 如果所有卷空间差在平衡阈值内，直接使用轮询策略选择
    if (volumesWithSpaces.areAllVolumesWithinFreeSpaceThreshold()) {
      V volume = roundRobinPolicyBalanced.chooseVolume(volumes, replicaSize,
          storageId);
      if (LOG.isDebugEnabled()) {
        LOG.debug("All volumes are within the configured free space balance " +
            "threshold. Selecting " + volume + " for write of block size " +
            replicaSize);
      }
      return volume;
    } else {
      V volume = null;
      // 获取低可用空间卷中的最大可用空间
      long mostAvailableAmongLowVolumes = volumesWithSpaces
          .getMostAvailableSpaceAmongVolumesWithLowAvailableSpace();
      
      // 分别提取高可用空间卷和低可用空间卷列表
      List<V> highAvailableVolumes = extractVolumesFromPairs(
          volumesWithSpaces.getVolumesWithHighAvailableSpace());
      List<V> lowAvailableVolumes = extractVolumesFromPairs(
          volumesWithSpaces.getVolumesWithLowAvailableSpace());
      
      // 根据高/低可用卷数量缩放偏好比例，保证概率计算正确
      float preferencePercentScaler =
          (highAvailableVolumes.size() * balancedPreferencePercent) +
          (lowAvailableVolumes.size() * (1 - balancedPreferencePercent));
      float scaledPreferencePercent =
          (highAvailableVolumes.size() * balancedPreferencePercent) /
          preferencePercentScaler;
      // 如果低可用空间卷都放不下当前副本，或者随机命中高可用空间偏好，选择高可用空间卷
      if (mostAvailableAmongLowVolumes < replicaSize ||
          random.nextFloat() < scaledPreferencePercent) {
        volume = roundRobinPolicyHighAvailable.chooseVolume(
            highAvailableVolumes, replicaSize, storageId);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Volumes are imbalanced. Selecting " + volume +
              " from high available space volumes for write of block size "
              + replicaSize);
        }
      } else {
        // 否则从低可用空间卷中选择
        volume = roundRobinPolicyLowAvailable.chooseVolume(
            lowAvailableVolumes, replicaSize, storageId);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Volumes are imbalanced. Selecting " + volume +
              " from low available space volumes for write of block size "
              + replicaSize);
        }
      }
      return volume;
    }
  }
  
  /**
   * 存储卷与可用空间的封装列表，用于一次性获取所有候选卷的可用空间，避免重复查询
   */
  private class AvailableSpaceVolumeList {
    /** 存储卷-可用空间对列表 */
    private final List<AvailableSpaceVolumePair> volumes;
    
    /**
     * 构造函数，一次性获取所有卷的可用空间并封装
     * @param volumes 原始候选卷列表
     * @throws IOException 获取可用空间时可能抛出IO异常
     */
    public AvailableSpaceVolumeList(List<V> volumes) throws IOException {
      this.volumes = new ArrayList<AvailableSpaceVolumePair>();
      for (V volume : volumes) {
        this.volumes.add(new AvailableSpaceVolumePair(volume));
      }
    }
    
    /**
     * 检查所有卷的可用空间差是否在平衡阈值内
     * @return 所有卷空间差小于等于阈值返回true，否则返回false
     */
    public boolean areAllVolumesWithinFreeSpaceThreshold() {
      long leastAvailable = Long.MAX_VALUE;
      long mostAvailable = 0;
      // 找出最大和最小可用空间
      for (AvailableSpaceVolumePair volume : volumes) {
        leastAvailable = Math.min(leastAvailable, volume.getAvailable());
        mostAvailable = Math.max(mostAvailable, volume.getAvailable());
      }
      // 比较最大最小差是否小于等于平衡阈值
      return (mostAvailable - leastAvailable) <= balancedSpaceThreshold;
    }
    
    /**
     * 获取所有卷中最小的可用空间值
     * @return 最小可用空间
     */
    private long getLeastAvailableSpace() {
      long leastAvailable = Long.MAX_VALUE;
      for (AvailableSpaceVolumePair volume : volumes) {
        leastAvailable = Math.min(leastAvailable, volume.getAvailable());
      }
      return leastAvailable;
    }
    
    /**
     * 获取所有低可用空间卷中的最大可用空间值
     * @return 低可用空间卷中的最大可用空间
     */
    public long getMostAvailableSpaceAmongVolumesWithLowAvailableSpace() {
      long mostAvailable = Long.MIN_VALUE;
      for (AvailableSpaceVolumePair volume : getVolumesWithLowAvailableSpace()) {
        mostAvailable = Math.max(mostAvailable, volume.getAvailable());
      }
      return mostAvailable;
    }
    
    /**
     * 获取所有低可用空间卷列表
     * @return 低可用空间卷列表
     */
    public List<AvailableSpaceVolumePair> getVolumesWithLowAvailableSpace() {
      long leastAvailable = getLeastAvailableSpace();
      List<AvailableSpaceVolumePair> ret = new ArrayList<AvailableSpaceVolumePair>();
      // 可用空间 <= 最小可用 + 平衡阈值 判定为低可用空间卷
      for (AvailableSpaceVolumePair volume : volumes) {
        if (volume.getAvailable() <= leastAvailable + balancedSpaceThreshold) {
          ret.add(volume);
        }
      }
      return ret;
    }
    
    /**
     * 获取所有高可用空间卷列表
     * @return 高可用空间卷列表
     */
    public List<AvailableSpaceVolumePair> getVolumesWithHighAvailableSpace() {
      long leastAvailable = getLeastAvailableSpace();
      List<AvailableSpaceVolumePair> ret = new ArrayList<AvailableSpaceVolumePair>();
      // 可用空间 > 最小可用 + 平衡阈值 判定为高可用空间卷
      for (AvailableSpaceVolumePair volume : volumes) {
        if (volume.getAvailable() > leastAvailable + balancedSpaceThreshold) {
          ret.add(volume);
        }
      }
      return ret;
    }
    
  }
  
  /**
   * 封装单个卷及其可用空间，避免重复查询卷的可用空间
   */
  private class AvailableSpaceVolumePair {
    /** 原始卷实例 */
    private final V volume;
    /** 构造时获取的可用空间 */
    private final long availableSpace;
    
    /**
     * 构造函数，获取并存储卷的可用空间
     * @param volume 需要封装的卷
     * @throws IOException 获取可用空间时可能抛出IO异常
     */
    public AvailableSpaceVolumePair(V volume) throws IOException {
      this.volume = volume;
      this.availableSpace = volume.getAvailable();
    }
    
    /**
     * 获取缓存的可用空间
     * @return 可用空间大小
     */
    public long getAvailable() {
      return availableSpace;
    }
    
    /**
     * 获取原始卷实例
     * @return 原始卷
     */
    public V getVolume() {
      return volume;
    }
  }
  
  /**
   * 从卷-空间对列表中提取原始卷实例列表
   * @param volumes 卷-空间对列表
   * @return 原始卷实例列表
   */
  private List<V> extractVolumesFromPairs(List<AvailableSpaceVolumePair> volumes) {
    List<V> ret = new ArrayList<V>();
    for (AvailableSpaceVolumePair volume : volumes) {
      ret.add(volume.getVolume());
    }
    return ret;
  }

}