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

import java.io.IOException;
import java.net.URI;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeReference;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.VolumeChoosingPolicy;
import org.apache.hadoop.hdfs.server.datanode.BlockScanner;
import org.apache.hadoop.hdfs.server.datanode.StorageLocation;
import org.apache.hadoop.hdfs.server.datanode.metrics.DataNodeDiskMetrics;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.AutoCloseableLock;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.concurrent.SubjectInheritingThread;

/**
 * Datanode存储卷列表管理类，负责管理DataNode上所有数据卷的生命周期，
 * 提供卷选择、新增/删除卷、故障处理、块池管理等核心能力，支持同盘分层存储功能。
 */
class FsVolumeList {
  // 存储当前所有可用存储卷，使用写时复制列表保证并发安全
  private final CopyOnWriteArrayList<FsVolumeImpl> volumes =
      new CopyOnWriteArrayList<>();
  // 按存储位置排序记录故障卷信息，key为存储位置，value为故障详情
  // Tracks volume failures, sorted by volume path.
  // map from volume storageID to the volume failure info
  private final Map<StorageLocation, VolumeFailureInfo> volumeFailureInfos =
      Collections.synchronizedMap(
          new TreeMap<StorageLocation, VolumeFailureInfo>());
  // 正在移除过程中的存储卷队列，等待所有引用释放后完成清理
  private final ConcurrentLinkedQueue<FsVolumeImpl> volumesBeingRemoved =
      new ConcurrentLinkedQueue<>();
  // 目录检查锁，用于保护卷移除操作的并发安全
  private final AutoCloseableLock checkDirsLock;
  // 卷移除完成等待条件
  private final Condition checkDirsLockCondition;

  // 块存储卷选择策略实现
  private final VolumeChoosingPolicy<FsVolumeImpl> blockChooser;
  // 块扫描器引用，用于新增/移除卷的扫描器管理
  private final BlockScanner blockScanner;

  // 是否启用同盘分层存储功能
  private final boolean enableSameDiskTiering;
  // 挂载点与存储卷映射表，用于同盘分层存储的卷查找
  private final MountVolumeMap mountVolumeMap;
  // 存储位置容量配比映射，用于同盘分层存储
  private Map<URI, Double> capacityRatioMap;
  // DataNode磁盘指标统计，用于慢盘过滤
  private final DataNodeDiskMetrics diskMetrics;

  /**
   * 构造存储卷列表，初始化相关配置和数据结构。
   * @param initialVolumeFailureInfos 初始化时已有的故障卷信息
   * @param blockScanner 块扫描器实例
   * @param blockChooser 卷选择策略实例
   * @param config Hadoop配置对象
   * @param dataNodeDiskMetrics 磁盘指标统计对象
   */
  FsVolumeList(List<VolumeFailureInfo> initialVolumeFailureInfos,
      BlockScanner blockScanner,
      VolumeChoosingPolicy<FsVolumeImpl> blockChooser,
      Configuration config, DataNodeDiskMetrics dataNodeDiskMetrics) {
    this.blockChooser = blockChooser;
    this.blockScanner = blockScanner;
    this.checkDirsLock = new AutoCloseableLock();
    this.checkDirsLockCondition = checkDirsLock.newCondition();
    this.diskMetrics = dataNodeDiskMetrics;
    // 把初始化传入的故障信息存入故障映射表
    for (VolumeFailureInfo volumeFailureInfo: initialVolumeFailureInfos) {
      volumeFailureInfos.put(volumeFailureInfo.getFailedStorageLocation(),
          volumeFailureInfo);
    }
    // 从配置读取是否启用同盘分层存储
    enableSameDiskTiering = config.getBoolean(
        DFSConfigKeys.DFS_DATANODE_ALLOW_SAME_DISK_TIERING,
        DFSConfigKeys.DFS_DATANODE_ALLOW_SAME_DISK_TIERING_DEFAULT);
    // 初始化挂载卷映射表
    mountVolumeMap = new MountVolumeMap(config);
    // 初始化容量配比配置
    initializeCapacityRatio(config);
  }

  /**
   * 获取挂载卷映射表。
   * @return 挂载卷映射表实例
   */
  MountVolumeMap getMountVolumeMap() {
    return mountVolumeMap;
  }

  /**
   * 返回所有存储卷的不可变视图。
   */
  List<FsVolumeImpl> getVolumes() {
    return Collections.unmodifiableList(volumes);
  }

  /**
   * 根据指定候选卷列表选择一个可用存储卷，并返回其引用。
   * 会自动过滤已经关闭的卷，直到找到可用卷或所有卷都不可用。
   * @param list 候选存储卷列表
   * @param blockSize 需要分配的块大小
   * @param storageId 目标存储ID，供选择策略使用
   * @return 选中存储卷的引用
   * @throws IOException 当所有卷都不可用或无足够空间时抛出异常
   */
  private FsVolumeReference chooseVolume(List<FsVolumeImpl> list,
      long blockSize, String storageId) throws IOException {

    // 选择卷时过滤掉慢盘
    if (diskMetrics != null) {
      List<String> slowDisksToExclude = diskMetrics.getSlowDisksToExclude();
      list = list.stream()
          .filter(volume -> !slowDisksToExclude.contains(volume.getBaseURI().getPath()))
          .collect(Collectors.toList());
    }

    // 循环尝试选择，直到找到可用卷
    while (true) {
      FsVolumeImpl volume = blockChooser.chooseVolume(list, blockSize,
          storageId);
      try {
        // 获取卷引用，确保卷不会在使用中被关闭
        return volume.obtainReference();
      } catch (ClosedChannelException e) {
        FsDatasetImpl.LOG.warn("Chosen a closed volume: " + volume);
        // 选择策略会在列表为空时抛出空间不足异常，表示所有卷都已关闭
        list.remove(volume);
      }
    }
  }

  /**
   * 根据挂载点和存储类型选择存储卷，用于同盘分层存储场景。
   *
   * @param storageType 目标存储类型
   * @param mount 磁盘挂载点路径
   * @param blockSize 需要的可用空间大小
   * @return 符合条件的存储卷引用，找不到或空间不足返回null
   * @throws IOException 获取卷引用失败时抛出异常
   */
  FsVolumeReference getVolumeByMount(StorageType storageType,
      String mount, long blockSize) throws IOException {
    if (!enableSameDiskTiering) {
      return null;
    }
    FsVolumeReference volume = mountVolumeMap
        .getVolumeRefByMountAndStorageType(mount, storageType);
    // 检查卷是否有足够可用空间
    if (volume != null && volume.getVolume().getAvailable() > blockSize) {
      return volume;
    }
    return null;
  }

  /**
   * 从配置解析同盘分层存储的容量配比配置。
   * @param config Hadoop配置对象
   */
  private void initializeCapacityRatio(Configuration config) {
    if (capacityRatioMap == null) {
      String capacityRatioConfig = config.get(
          DFSConfigKeys
              .DFS_DATANODE_SAME_DISK_TIERING_CAPACITY_RATIO_PERCENTAGE,
          DFSConfigKeys
              .DFS_DATANODE_SAME_DISK_TIERING_CAPACITY_RATIO_PERCENTAGE_DEFAULT
      );

      this.capacityRatioMap = StorageLocation
          .parseCapacityRatio(capacityRatioConfig);
    }
  }

  /** 
   * 获取下一个可存储块的存储卷，按指定存储类型筛选。
   *
   * @param blockSize 需要的可用空间大小
   * @param storageType 目标存储类型
   * @param storageId 目标存储ID，供选择策略使用
   * @return 选中存储卷的引用
   * @throws IOException 找不到可用卷时抛出异常
   */
  FsVolumeReference getNextVolume(StorageType storageType, String storageId,
      long blockSize) throws IOException {
    // 筛选出同存储类型的所有卷
    final List<FsVolumeImpl> list = new ArrayList<>(volumes.size());
    for(FsVolumeImpl v : volumes) {
      if (v.getStorageType() == storageType) {
        list.add(v);
      }
    }
    return chooseVolume(list, blockSize, storageId);
  }

  /**
   * 获取下一个临时存储卷，用于临时块存储。
   *
   * @param blockSize 需要的可用空间大小
   * @return 选中存储卷的引用
   * @throws IOException 找不到可用卷时抛出异常
   */
  FsVolumeReference getNextTransientVolume(long blockSize) throws IOException {
    // 获取当前所有可用卷的快照
    final List<FsVolumeImpl> curVolumes = getVolumes();
    // 筛选出临时存储卷
    final List<FsVolumeImpl> list = new ArrayList<>(curVolumes.size());
    for(FsVolumeImpl v : curVolumes) {
      if (v.isTransientStorage()) {
        list.add(v);
      }
    }
    return chooseVolume(list, blockSize, null);
  }

  /**
   * 统计所有存储卷的DFS已用空间总和。
   * @return 总已用空间字节数
   * @throws IOException 统计过程中IO异常
   */
  long getDfsUsed() throws IOException {
    long dfsUsed = 0L;
    for (FsVolumeImpl v : volumes) {
      try(FsVolumeReference ref = v.obtainReference()) {
        dfsUsed += v.getDfsUsed();
      } catch (ClosedChannelException e) {
        // 忽略已关闭的卷
      }
    }
    return dfsUsed;
  }

  /**
   * 统计指定块池在所有存储卷上的已用空间总和。
   * @param bpid 块池ID
   * @return 块池总已用空间字节数
   * @throws IOException 统计过程中IO异常
   */
  long getBlockPoolUsed(String bpid) throws IOException {
    long dfsUsed = 0L;
    for (FsVolumeImpl v : volumes) {
      try (FsVolumeReference ref = v.obtainReference()) {
        dfsUsed += v.getBlockPoolUsed(bpid);
      } catch (ClosedChannelException e) {
        // 忽略已关闭的卷
      }
    }
    return dfsUsed;
  }

  /**
   * 统计所有存储卷的总容量。
   * @return 总容量字节数
   */
  long getCapacity() {
    long capacity = 0L;
    for (FsVolumeImpl v : volumes) {
      try (FsVolumeReference ref = v.obtainReference()) {
        capacity += v.getCapacity();
      } catch (IOException e) {
        // 忽略异常卷
      }
    }
    return capacity;
  }
    
  /**
   * 统计所有存储卷的剩余可用空间总和。
   * @return 总剩余可用空间字节数
   * @throws IOException 统计过程中IO异常
   */
  long getRemaining() throws IOException {
    long remaining = 0L;
    for (FsVolumeSpi vol : volumes) {
      try (FsVolumeReference ref = vol.obtainReference()) {
        remaining += vol.getAvailable();
      } catch (ClosedChannelException e) {
        // 忽略已关闭的卷
      }
    }
    return remaining;
  }
  
  /**
   * 并发收集所有存储卷中指定块池的所有副本，存入副本映射表。
   * 采用多线程并发扫描每个卷提升加载速度。
   * @param bpid 目标块池ID
   * @param volumeMap 输出用副本映射表
   * @param ramDiskReplicaTracker RamDisk副本跟踪器
   * @throws IOException 当存在扫描失败的存储卷时抛出异常
   */
  void getAllVolumesMap(final String bpid,
                        final ReplicaMap volumeMap,
                        final RamDiskReplicaTracker ramDiskReplicaTracker)
      throws IOException {
    long totalStartTime = Time.monotonicNow();
    // 记录扫描失败的卷和对应异常
    final Map<FsVolumeSpi, IOException> unhealthyDataDirs =
        new ConcurrentHashMap<FsVolumeSpi, IOException>();
    List<Thread> replicaAddingThreads = new ArrayList<Thread>();
    // 为每个卷启动独立线程并发扫描
    for (final FsVolumeImpl v : volumes) {
      Thread t = new SubjectInheritingThread() {
        public void work() {
          try (FsVolumeReference ref = v.obtainReference()) {
            FsDatasetImpl.LOG.info("Adding replicas to map for block pool " +
                bpid + " on volume " + v + "...");
            long startTime = Time.monotonicNow();
            // 扫描当前卷的块池副本，存入映射表
            v.getVolumeMap(bpid, volumeMap, ramDiskReplicaTracker);
            long timeTaken = Time.monotonicNow() - startTime;
            FsDatasetImpl.LOG.info("Time to add replicas to map for block pool"
                + " " + bpid + " on volume " + v + ": " + timeTaken + "ms");
          } catch (IOException ioe) {
            FsDatasetImpl.LOG.info("Caught exception while adding replicas " +
                "from " + v + ". Will throw later.", ioe);
            unhealthyDataDirs.put(v, ioe);
          }
        }
      };
      replicaAddingThreads.add(t);
      t.start();
    }
    // 等待所有线程扫描完成
    for (Thread t : replicaAddingThreads) {
      try {
        t.join();
      } catch (InterruptedException ie) {
        throw new IOException(ie);
      }
    }
    long totalTimeTaken = Time.monotonicNow() - totalStartTime;
    FsDatasetImpl.LOG
        .info("Total time to add all replicas to map for block pool " + bpid
            + ": " + totalTimeTaken + "ms");
    // 如果有扫描失败，抛出异常
    if (!unhealthyDataDirs.isEmpty()) {
      throw new AddBlockPoolException(unhealthyDataDirs);
    }
  }

  /**
   * 处理一批故障存储卷，记录故障信息并从可用列表中移除，等待移除完成。
   *
   * @param failedVolumes 故障卷集合
   */
  void handleVolumeFailures(Set<FsVolumeSpi> failedVolumes) {
    try (AutoCloseableLock lock = checkDirsLock.acquire()) {
      // 逐个处理每个故障卷
      for(FsVolumeSpi vol : failedVolumes) {
        FsVolumeImpl fsv = (FsVolumeImpl) vol;
        try (FsVolumeReference ref = fsv.obtainReference()) {
          // 添加故障信息记录
          addVolumeFailureInfo(fsv);
          // 从可用列表移除卷
          removeVolume(fsv);
        } catch (ClosedChannelException e) {
          FsDatasetImpl.LOG.debug("Caught exception when obtaining " +
            "reference count on closed volume", e);
        } catch (IOException e) {
          FsDatasetImpl.LOG.error("Unexpected IOException", e);
        }
      }
      // 等待所有待移除卷的引用全部释放
      waitVolumeRemoved(5000, checkDirsLockCondition);
    }
  }

  /**
   * 等待所有待移除存储卷的引用全部释放。
   *
   * @param sleepMillis 每次检查间隔毫秒数
   * @param condition 等待条件对象
   */
  void waitVolumeRemoved(int sleepMillis, Condition condition) {
    while (!checkVol