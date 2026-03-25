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


import org.apache.hadoop.thirdparty.com.google.common.collect.TreeMultimap;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.util.Time;

import java.io.File;
import java.util.*;

/**
 * 文件级注释：RamDisk瞬时存储副本的LRU淘汰跟踪器实现，用于管理DataNode内存磁盘上的块副本，基于LRU策略淘汰持久化后的副本释放内存空间
 *
 * An implementation of RamDiskReplicaTracker that uses an LRU
 * eviction scheme.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class RamDiskReplicaLruTracker extends RamDiskReplicaTracker {

  /**
   * 内部类：LRU策略下的RamDisk副本信息扩展类，增加了最后使用时间戳用于LRU排序
   */
  private static class RamDiskReplicaLru extends RamDiskReplica {
    // 副本最后访问时间戳
    long lastUsedTime;

    private RamDiskReplicaLru(String bpid, long blockId,
                              FsVolumeImpl ramDiskVolume,
                              long lockedBytesReserved) {
      super(bpid, blockId, ramDiskVolume, lockedBytesReserved);
    }

    @Override
    public int hashCode() {
      return super.hashCode();
    }

    @Override
    public boolean equals(Object other) {
      return super.equals(other);
    }
  }

  /**
   * Map of blockpool ID to <map of blockID to ReplicaInfo>.
   * 按块池分组存储所有RamDisk上的副本信息，键为块池ID，值为块ID到副本对象的映射
   */
  Map<String, Map<Long, RamDiskReplicaLru>> replicaMaps;

  /**
   * Queue of replicas that need to be written to disk.
   * Stale entries are GC'd by dequeueNextReplicaToPersist.
   * 等待持久化到磁盘的副本队列，使用懒清理方式删除无效条目
   */
  Queue<RamDiskReplicaLru> replicasNotPersisted;

  /**
   * Map of persisted replicas ordered by their last use times.
   * 已持久化副本按最后使用时间排序的多值映射，键为最后使用时间戳，值为对应副本对象
   */
  TreeMultimap<Long, RamDiskReplicaLru> replicasPersisted;

  /**
   * 构造函数：初始化LRU跟踪器的各个数据结构
   */
  RamDiskReplicaLruTracker() {
    replicaMaps = new HashMap<>();
    replicasNotPersisted = new LinkedList<>();
    replicasPersisted = TreeMultimap.create();
  }

  /**
   * 添加新的RamDisk副本到跟踪器，加入待持久化队列
   * @param bpid 块池ID
   * @param blockId 块ID
   * @param transientVolume RamDisk瞬时存储卷
   * @param lockedBytesReserved 锁定预留的字节数
   */
  @Override
  synchronized void addReplica(final String bpid, final long blockId,
                               final FsVolumeImpl transientVolume,
                               long lockedBytesReserved) {
    Map<Long, RamDiskReplicaLru> map = replicaMaps.get(bpid);
    // 如果块池不存在则新建映射
    if (map == null) {
      map = new HashMap<>();
      replicaMaps.put(bpid, map);
    }
    RamDiskReplicaLru ramDiskReplicaLru =
        new RamDiskReplicaLru(bpid, blockId, transientVolume,
            lockedBytesReserved);
    map.put(blockId, ramDiskReplicaLru);
    // 添加到待持久化队列
    replicasNotPersisted.add(ramDiskReplicaLru);
  }

  /**
   * 更新副本访问时间，刷新LRU顺序，表示副本刚被访问过
   * @param bpid 块池ID
   * @param blockId 块ID
   */
  @Override
  synchronized void touch(final String bpid,
                          final long blockId) {
    Map<Long, RamDiskReplicaLru> map = replicaMaps.get(bpid);
    RamDiskReplicaLru ramDiskReplicaLru = map.get(blockId);

    if (ramDiskReplicaLru == null) {
      return;
    }

    // 增加访问计数
    ramDiskReplicaLru.numReads.getAndIncrement();

    // 重新插入副本更新时间戳：仅针对已持久化副本
    if (replicasPersisted.remove(ramDiskReplicaLru.lastUsedTime, ramDiskReplicaLru)) {
      ramDiskReplicaLru.lastUsedTime = Time.monotonicNow();
      replicasPersisted.put(ramDiskReplicaLru.lastUsedTime, ramDiskReplicaLru);
    }
  }

  /**
   * 记录开始懒持久化操作，设置目标持久化卷信息
   * @param bpid 块池ID
   * @param blockId 块ID
   * @param checkpointVolume 目标持久化检查点卷
   */
  @Override
  synchronized void recordStartLazyPersist(
      final String bpid, final long blockId, FsVolumeImpl checkpointVolume) {
    Map<Long, RamDiskReplicaLru> map = replicaMaps.get(bpid);
    RamDiskReplicaLru ramDiskReplicaLru = map.get(blockId);
    ramDiskReplicaLru.setLazyPersistVolume(checkpointVolume);
  }

  /**
   * 记录懒持久化完成，将副本标记为已持久化并加入LRU淘汰队列
   * @param bpid 块池ID
   * @param blockId 块ID
   * @param savedFiles 持久化后保存的文件数组
   */
  @Override
  synchronized void recordEndLazyPersist(
      final String bpid, final long blockId, final File[] savedFiles) {
    Map<Long, RamDiskReplicaLru> map = replicaMaps.get(bpid);
    RamDiskReplicaLru ramDiskReplicaLru = map.get(blockId);

    if (ramDiskReplicaLru == null) {
      throw new IllegalStateException("Unknown replica bpid=" +
          bpid + "; blockId=" + blockId);
    }
    // 记录持久化后的文件信息
    ramDiskReplicaLru.recordSavedBlockFiles(savedFiles);

    // 从待持久化队列中移除该副本
    if (replicasNotPersisted.peek() == ramDiskReplicaLru) {
      // 正常出队，队首匹配直接移除
      replicasNotPersisted.remove();
    } else {
      // 异常退队，执行线性移除
      replicasNotPersisted.remove(ramDiskReplicaLru);
    }

    // 更新最后使用时间，加入已持久化LRU队列
    ramDiskReplicaLru.lastUsedTime = Time.monotonicNow();
    replicasPersisted.put(ramDiskReplicaLru.lastUsedTime, ramDiskReplicaLru);
    ramDiskReplicaLru.isPersisted = true;
  }

  /**
   * 出队获取下一个需要持久化的副本，自动清理队列中已失效的条目
   * @return 下一个待持久化副本，没有则返回null
   */
  @Override
  synchronized RamDiskReplicaLru dequeueNextReplicaToPersist() {
    while (replicasNotPersisted.size() != 0) {
      RamDiskReplicaLru ramDiskReplicaLru = replicasNotPersisted.remove();
      Map<Long, RamDiskReplicaLru> replicaMap =
          replicaMaps.get(ramDiskReplicaLru.getBlockPoolId());

      // 确认副本仍然存在，有效则返回
      if (replicaMap != null && replicaMap.get(ramDiskReplicaLru.getBlockId()) != null) {
        return ramDiskReplicaLru;
      }

      // 副本已不存在，继续查找下一个，此处自动清理无效条目
    }
    return null;
  }

  /**
   * 将未完成持久化的副本重新加入待持久化队列
   * @param ramDiskReplicaLru 需要重新入队的副本对象
   */
  @Override
  synchronized void reenqueueReplicaNotPersisted(final RamDiskReplica ramDiskReplicaLru) {
    replicasNotPersisted.add((RamDiskReplicaLru) ramDiskReplicaLru);
  }

  /**
   * 获取当前待持久化副本数量
   * @return 待持久化队列大小
   */
  @Override
  synchronized int numReplicasNotPersisted() {
    return replicasNotPersisted.size();
  }

  /**
   * 获取下一个适合淘汰的LRU候选副本（最早未被访问的已持久化副本）
   * @return 下一个待淘汰副本，没有可用候选则返回null
   */
  @Override
  synchronized RamDiskReplicaLru getNextCandidateForEviction() {
    final Iterator<RamDiskReplicaLru> it = replicasPersisted.values().iterator();
    // 按最后使用时间从小到大遍历（最早访问的在前）
    while (it.hasNext()) {
      final RamDiskReplicaLru ramDiskReplicaLru = it.next();
      it.remove();

      Map<Long, RamDiskReplicaLru> replicaMap =
          replicaMaps.get(ramDiskReplicaLru.getBlockPoolId());

      // 确认副本仍然存在，有效则返回作为淘汰候选
      if (replicaMap != null && replicaMap.get(ramDiskReplicaLru.getBlockId()) != null) {
        return ramDiskReplicaLru;
      }

      // 副本已不存在，继续查找下一个
    }
    return null;
  }

  /**
   * Discard any state we are tracking for the given replica. This could mean
   * the block is either deleted from the block space or the replica is no longer
   * on transient storage.
   *
   * 从跟踪器中删除指定副本的所有状态信息，可选择同时删除持久化存储上的备份
   * @param bpid 块池ID
   * @param blockId 块ID
   * @param deleteSavedCopies 是否同时删除持久化存储上的备份，块完全删除时需要设置为true
   */
  @Override
  synchronized void discardReplica(
      final String bpid, final long blockId,
      boolean deleteSavedCopies) {
    Map<Long, RamDiskReplicaLru> map = replicaMaps.get(bpid);

    if (map == null) {
      return;
    }

    RamDiskReplicaLru ramDiskReplicaLru = map.get(blockId);

    if (ramDiskReplicaLru == null) {
      return;
    }

    // 需要则删除持久化备份文件
    if (deleteSavedCopies) {
      ramDiskReplicaLru.deleteSavedFiles();
    }

    // 从各个数据结构中移除副本信息
    map.remove(blockId);
    replicasPersisted.remove(ramDiskReplicaLru.lastUsedTime, ramDiskReplicaLru);

    // 待持久化队列中的条目会被懒清理，无需在此主动删除
  }

  /**
   * 根据块池ID和块ID获取跟踪的副本对象
   * @param bpid 块池ID
   * @param blockId 块ID
   * @return 对应副本对象，不存在则返回null
   */
  @Override
  synchronized RamDiskReplica getReplica(
    final String bpid, final long blockId) {
    Map<Long, RamDiskReplicaLru> map = replicaMaps.get(bpid);

    if (map == null) {
      return null;
    }

    return map.get(blockId);
  }
}