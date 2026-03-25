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

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_ROUND_ROBIN_VOLUME_CHOOSING_POLICY_ADDITIONAL_AVAILABLE_SPACE_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_ROUND_ROBIN_VOLUME_CHOOSING_POLICY_ADDITIONAL_AVAILABLE_SPACE_KEY;

import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.util.DiskChecker.DiskOutOfSpaceException;

/**
 * 轮询卷选择策略实现，按轮询顺序选择相同存储类型的存储卷。
 * 使用细粒度锁同步卷选择过程，保证并发选择时的线程安全。
 * 核心职责是为DataNode的数据块写入选择合适的存储卷，均衡不同磁盘的写入压力。
 */
public class RoundRobinVolumeChoosingPolicy<V extends FsVolumeSpi>
    implements VolumeChoosingPolicy<V>, Configurable {
  public static final Logger LOG =
      LoggerFactory.getLogger(RoundRobinVolumeChoosingPolicy.class);

  // 存储每种存储类型对应的当前轮询位置，使用存储类型的枚举序号作为数组下标
  private int[] curVolumes;
  // 存储每种存储类型对应的同步锁，实现细粒度并发控制
  private Object[] syncLocks;

  // 选择卷时要求的额外可用空间阈值，保障空间预留避免空间耗尽
  private long additionalAvailableSpace;

  /**
   * 构造方法，初始化轮询计数器和同步锁数组，按存储类型数量预分配空间。
   */
  public RoundRobinVolumeChoosingPolicy() {
    int numStorageTypes = StorageType.values().length;
    curVolumes = new int[numStorageTypes];
    syncLocks = new Object[numStorageTypes];
    for (int i = 0; i < numStorageTypes; i++) {
      syncLocks[i] = new Object();
    }
  }

  @Override
  /**
   * 从配置中加载额外可用空间阈值，完成策略初始化。
   * @param conf Hadoop配置对象
   */
  public void setConf(Configuration conf) {
    additionalAvailableSpace = conf.getLong(
        DFS_DATANODE_ROUND_ROBIN_VOLUME_CHOOSING_POLICY_ADDITIONAL_AVAILABLE_SPACE_KEY,
        DFS_DATANODE_ROUND_ROBIN_VOLUME_CHOOSING_POLICY_ADDITIONAL_AVAILABLE_SPACE_DEFAULT);

    LOG.info("Round robin volume choosing policy initialized: " +
        DFS_DATANODE_ROUND_ROBIN_VOLUME_CHOOSING_POLICY_ADDITIONAL_AVAILABLE_SPACE_KEY +
        " = " + additionalAvailableSpace);
  }

  @Override
  public Configuration getConf() {
    // Nothing to do. Only added to fulfill the Configurable contract.
    return null;
  }

  @Override
  /**
   * 根据轮询策略从符合存储类型的可用卷中选择一个满足空间要求的卷。
   * @param volumes 同存储类型的可用卷列表
   * @param blockSize 要写入的数据块大小
   * @param storageId 存储ID
   * @return 选中的存储卷
   * @throws IOException 当无可用卷或空间不足时抛出异常
   */
  public V chooseVolume(final List<V> volumes, long blockSize, String storageId)
      throws IOException {

    if (volumes.size() < 1) {
      throw new DiskOutOfSpaceException("No more available volumes");
    }

    // 输入列表中所有卷存储类型相同，直接取第一个卷的存储类型
    StorageType storageType = volumes.get(0).getStorageType();
    int index = storageType != null ?
            storageType.ordinal() : StorageType.DEFAULT.ordinal();

    // 对当前存储类型加锁，保证轮询状态的线程安全
    synchronized (syncLocks[index]) {
      return chooseVolume(index, volumes, blockSize);
    }
  }

  /**
   * 内部实际执行轮询选择的方法，从当前位置开始遍历查找满足空间要求的卷。
   * @param curVolumeIndex 当前存储类型对应的轮询计数器数组下标
   * @param volumes 同存储类型的可用卷列表
   * @param blockSize 要写入的数据块大小
   * @return 选中的存储卷
   * @throws IOException 遍历所有卷都不满足空间要求时抛出异常
   */
  private V chooseVolume(final int curVolumeIndex, final List<V> volumes,
                         long blockSize) throws IOException {
    // 确保当前轮询位置不越界，处理卷被移除的场景
    int curVolume = curVolumes[curVolumeIndex] < volumes.size()
            ? curVolumes[curVolumeIndex] : 0;

    int startVolume = curVolume;
    long maxAvailable = 0;

    // 轮询查找可用卷
    while (true) {
      final V volume = volumes.get(curVolume);
      // 计算下一个轮询位置
      curVolume = (curVolume + 1) % volumes.size();
      // 获取当前卷的可用空间
      long availableVolumeSize = volume.getAvailable();
      // 检查当前卷空间满足要求（数据块大小+额外预留空间）
      if (availableVolumeSize > blockSize + additionalAvailableSpace) {
        // 更新下一次轮询的起始位置
        curVolumes[curVolumeIndex] = curVolume;
        return volume;
      }

      // 记录当前找到的最大可用空间，用于异常信息
      if (availableVolumeSize > maxAvailable) {
        maxAvailable = availableVolumeSize;
      }

      // 输出空间不足警告日志
      LOG.warn("The volume[{}] with the available space (={} B) is "
              + "less than the block size (={} B).", volume.getBaseURI(),
          availableVolumeSize, blockSize);
      // 已经遍历完所有卷都不满足要求，抛出空间不足异常
      if (curVolume == startVolume) {
        throw new DiskOutOfSpaceException("Out of space: "
            + "The volume with the most available space (=" + maxAvailable
            + " B) is less than the block size (=" + blockSize + " B).");
      }
    }
  }
}