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

import org.apache.hadoop.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Time;

import java.io.File;
import java.util.concurrent.atomic.AtomicLong;

/**
 * RAM磁盘Replica追踪器抽象基类，定义延迟持久化场景下RamDisk上块副本的追踪管理接口，负责管理RamDisk中未持久化副本的生命周期、淘汰选择和持久化队列管理
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public abstract class RamDiskReplicaTracker {
  static final Logger LOG =
      LoggerFactory.getLogger(RamDiskReplicaTracker.class);

  FsDatasetImpl fsDataset;

  /**
   * 表示RamDisk上的一个块副本信息，记录副本位置、持久化状态等元数据，支持按块池ID和块ID排序比较
   */
  static class RamDiskReplica implements Comparable<RamDiskReplica>  {
    private final String bpid;
    private final long blockId;
    private File savedBlockFile;
    private File savedMetaFile;
    private long lockedBytesReserved;

    private long creationTime;
    protected AtomicLong numReads = new AtomicLong(0);
    protected boolean isPersisted;

    /**
     * RAM_DISK volume that holds the original replica.
     */
    final FsVolumeSpi ramDiskVolume;

    /**
     * Persistent volume that holds or will hold the saved replica.
     */
    FsVolumeImpl lazyPersistVolume;

    /**
     * 构造RamDisk副本对象，初始化副本元数据
     * @param bpid 块池ID
     * @param blockId 块ID
     * @param ramDiskVolume 存储该副本的RamDisk卷
     * @param lockedBytesReserved 该副本预留的锁定字节数
     */
    RamDiskReplica(final String bpid, final long blockId,
                   final FsVolumeImpl ramDiskVolume,
                   long lockedBytesReserved) {
      this.bpid = bpid;
      this.blockId = blockId;
      this.ramDiskVolume = ramDiskVolume;
      this.lockedBytesReserved = lockedBytesReserved;
      lazyPersistVolume = null;
      savedMetaFile = null;
      savedBlockFile = null;
      creationTime = Time.monotonicNow();
      isPersisted = false;
    }

    long getBlockId() {
      return blockId;
    }

    String getBlockPoolId() {
      return bpid;
    }

    FsVolumeImpl getLazyPersistVolume() {
      return lazyPersistVolume;
    }

    void setLazyPersistVolume(FsVolumeImpl volume) {
      Preconditions.checkState(!volume.isTransientStorage());
      this.lazyPersistVolume = volume;
    }

    File getSavedBlockFile() {
      return savedBlockFile;
    }

    File getSavedMetaFile() {
      return savedMetaFile;
    }

    long getNumReads() { return numReads.get(); }

    long getCreationTime() { return creationTime; }

    boolean getIsPersisted() {return isPersisted; }

    /**
     * Record the saved meta and block files on the given volume.
     *
     * @param files Meta and block files, in that order.
     */
    void recordSavedBlockFiles(File[] files) {
      this.savedMetaFile = files[0];
      this.savedBlockFile = files[1];
    }

    @Override
    public int hashCode() {
      return bpid.hashCode() ^ (int) blockId;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }

      if (other == null || getClass() != other.getClass()) {
        return false;
      }

      RamDiskReplica otherState = (RamDiskReplica) other;
      return (otherState.bpid.equals(bpid) && otherState.blockId == blockId);
    }

    // Delete the saved meta and block files. Failure to delete can be
    // ignored, the directory scanner will retry the deletion later.
    void deleteSavedFiles() {
      if (savedBlockFile != null) {
        if (!savedBlockFile.delete()) {
          LOG.warn("Failed to delete block file " + savedBlockFile);
        }
        savedBlockFile = null;
      }

      if (savedMetaFile != null) {
        if (!savedMetaFile.delete()) {
          LOG.warn("Failed to delete meta file " + savedMetaFile);
        }
        savedMetaFile = null;
      }
    }

    @Override
    public int compareTo(RamDiskReplica other) {
      int bpidResult = bpid.compareTo(other.bpid);
      if (bpidResult == 0)
        if (blockId == other.blockId) {
          return 0;
        } else if (blockId < other.blockId) {
          return -1;
        } else {
          return 1;
        }
      return bpidResult;
    }

    @Override
    public String toString() {
      return "[BlockPoolID=" + bpid + "; BlockId=" + blockId + "]";
    }

    public long getLockedBytesReserved() {
      return lockedBytesReserved;
    }
  }

  /**
   * 根据配置创建RamDiskReplicaTracker实例，从配置中读取实现类并反射实例化
   * @param conf Hadoop配置对象
   * @param fsDataset 数据节点文件数据集对象
   * @return 初始化完成的RamDiskReplicaTracker实例
   */
  static RamDiskReplicaTracker getInstance(final Configuration conf,
                                           final FsDatasetImpl fsDataset) {
    final Class<? extends RamDiskReplicaTracker> trackerClass = conf.getClass(
        DFSConfigKeys.DFS_DATANODE_RAM_DISK_REPLICA_TRACKER_KEY,
        DFSConfigKeys.DFS_DATANODE_RAM_DISK_REPLICA_TRACKER_DEFAULT,
        RamDiskReplicaTracker.class);
    final RamDiskReplicaTracker tracker = ReflectionUtils.newInstance(
        trackerClass, conf);
    tracker.initialize(fsDataset);
    return tracker;
  }

  /**
   * 初始化追踪器，绑定对应数据集对象
   * @param fsDataset 数据节点文件数据集对象
   */
  void initialize(final FsDatasetImpl fsDataset) {
    this.fsDataset = fsDataset;
  }

  /**
   * 添加一个新的已完成RamDisk副本到追踪器开始追踪
   * @param bpid 块池ID
   * @param blockId 块ID
   * @param transientVolume 存储该副本的RamDisk卷
   * @param lockedBytesReserved 该副本预留的锁定字节数
   */
  abstract void addReplica(final String bpid, final long blockId,
                           final FsVolumeImpl transientVolume,
                           long lockedBytesReserved);

  /**
   * 当客户端打开该副本时触发，用于更新副本访问信息，作为淘汰策略的启发式依据
   * @param bpid 块池ID
   * @param blockId 块ID
   */
  abstract void touch(final String bpid, final long blockId);

  /**
   * 从待持久化队列中取出下一个需要持久化到磁盘的副本
   * @return 待持久化的RamDisk副本对象
   */
  abstract RamDiskReplica dequeueNextReplicaToPersist();

  /**
   * 将持久化失败的副本重新加入待持久化队列，供后续重试
   * @param ramDiskReplica 持久化失败的副本对象
   */
  abstract void reenqueueReplicaNotPersisted(
      final RamDiskReplica ramDiskReplica);

  /**
   * Invoked when the Lazy persist operation is started by the DataNode.
   * @param checkpointVolume
   */
  abstract void recordStartLazyPersist(
      final String bpid, final long blockId, FsVolumeImpl checkpointVolume);

  /**
   * Invoked when the Lazy persist operation is complete.
   *
   * @param savedFiles The saved meta and block files, in that order.
   */
  abstract void recordEndLazyPersist(
      final String bpid, final long blockId, final File[] savedFiles);

  /**
   * 根据淘汰策略获取下一个可以从RamDisk淘汰的候选副本
   * @return 候选淘汰副本对象
   */
  abstract RamDiskReplica getNextCandidateForEviction();

  /**
   * 获取当前等待持久化到磁盘的副本数量
   * @return 待持久化副本数量
   */
  abstract int numReplicasNotPersisted();

  /**
   * 从追踪器中移除指定副本的所有追踪状态，可选择是否删除磁盘上的持久化副本
   * @param bpid 块池ID
   * @param blockId 块ID
   * @param deleteSavedCopies 是否删除已持久化的副本文件
   */
  abstract void discardReplica(
      final String bpid, final long blockId,
      boolean deleteSavedCopies);

  /**
   * 根据块池ID和块ID获取RamDisk中对应副本的追踪信息，不存在则返回null
   * @param bpid 块池ID
   * @param blockId 块ID
   * @return RamDisk副本对象，不存在则返回null
   */
  abstract RamDiskReplica getReplica(
    final String bpid, final long blockId);
}