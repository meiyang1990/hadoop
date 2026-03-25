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
package org.apache.hadoop.hdfs.server.blockmanagement;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import org.apache.hadoop.classification.VisibleForTesting;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.net.DFSTopologyNodeImpl;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.namenode.CachedBlock;
import org.apache.hadoop.hdfs.server.protocol.BlockECReconstructionCommand.BlockECReconstructionInfo;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage.State;
import org.apache.hadoop.hdfs.server.protocol.StorageReport;
import org.apache.hadoop.hdfs.server.protocol.VolumeFailureSummary;
import org.apache.hadoop.hdfs.util.EnumCounters;
import org.apache.hadoop.hdfs.util.LightWeightHashSet;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.util.LightWeightLinkedSet;
import org.apache.hadoop.util.IntrusiveCollection;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 文件说明：DataNode描述符类，扩展DatanodeInfo，存储NameNode端私有临时数据，包括节点健康状态、容量信息、
 * 关联块信息等，不对外暴露给客户端，仅在NameNode内部使用。
 * 该类维护了DataNode所有存储信息、待处理数据块任务队列、退役状态等核心数据，是NameNode管理DataNode的核心数据结构。
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class DatanodeDescriptor extends DatanodeInfo {
  public static final Logger LOG =
      LoggerFactory.getLogger(DatanodeDescriptor.class);
  public static final DatanodeDescriptor[] EMPTY_ARRAY = {};
  private static final int BLOCKS_SCHEDULED_ROLL_INTERVAL = 600*1000; //10min

  /**
   * 存储块与目标DataNode对，用于表示需要复制到目标节点的块任务
   */
  @InterfaceAudience.Private
  @InterfaceStability.Evolving
  public static class BlockTargetPair {
    public final Block block;
    public final DatanodeStorageInfo[] targets;    

    BlockTargetPair(Block block, DatanodeStorageInfo[] targets) {
      this.block = block;
      this.targets = targets;
    }
  }

  /**
   * 块任务队列，线程安全的阻塞队列，用于缓存待DataNode执行的各类块任务
   */
  private static class BlockQueue<E> {
    private final Queue<E> blockq = new LinkedList<>();

    /** 获取队列大小 */
    synchronized int size() {return blockq.size();}

    /** 入队操作 */
    synchronized boolean offer(E e) { 
      return blockq.offer(e);
    }

    /** 出队指定数量元素，返回批量结果 */
    synchronized List<E> poll(int numBlocks) {
      if (numBlocks <= 0 || blockq.isEmpty()) {
        return null;
      }

      List<E> results = new ArrayList<>();
      for(; !blockq.isEmpty() && numBlocks > 0; numBlocks--) {
        results.add(blockq.poll());
      }
      return results;
    }

    /**
     * 判断队列是否包含指定元素
     */
    synchronized boolean contains(E e) {
      return blockq.contains(e);
    }

    synchronized void clear() {
      blockq.clear();
    }
  }

  /**
   * 该DataNode上缓存块列表，按缓存状态分为待缓存、已缓存、待移除缓存三类
   */
  public static class CachedBlocksList extends IntrusiveCollection<CachedBlock> {
    public enum Type {
      PENDING_CACHED,
      CACHED,
      PENDING_UNCACHED
    }

    private final DatanodeDescriptor datanode;

    private final Type type;

    CachedBlocksList(DatanodeDescriptor datanode, Type type) {
      this.datanode = datanode;
      this.type = type;
    }

    public DatanodeDescriptor getDatanode() {
      return datanode;
    }

    public Type getType() {
      return type;
    }
  }

  // 存储节点退役/维护状态信息，节点未退役时该对象不保存有效信息
  private final LeavingServiceStatus leavingServiceStatus =
      new LeavingServiceStatus();

  protected final Map<String, DatanodeStorageInfo> storageMap =
      new HashMap<>();

  /**
   * 待该DataNode缓存的块列表
   */
  private final CachedBlocksList pendingCached = 
      new CachedBlocksList(this, CachedBlocksList.Type.PENDING_CACHED);

  /**
   * 该DataNode上已经缓存的块列表，由定期缓存报告更新
   */
  private final CachedBlocksList cached = 
      new CachedBlocksList(this, CachedBlocksList.Type.CACHED);

  /**
   * 待该DataNode移除缓存的块列表
   */
  private final CachedBlocksList pendingUncached = 
      new CachedBlocksList(this, CachedBlocksList.Type.PENDING_UNCACHED);

  /**
   * 上一次发送缓存指令时间，单位为单调毫秒
   */
  private long lastCachingDirectiveSentTimeMs;

  // isAlive等价于heartbeats.contains(this)，这是一个性能优化，避免ArrayList的O(n)contains操作
  private boolean isAlive = false;
  private boolean needKeyUpdate = false;
  private boolean forceRegistration = false;

  // 均衡带宽参数，可通过dfsadmin动态调整，更新指令下发到DataNode后该值重置为0
  private long bandwidth;

  /** 待该DataNode执行复制的块任务队列 */
  private final BlockQueue<BlockTargetPair> replicateBlocks =
      new BlockQueue<>();
  /** 待该DataNode执行复制的EC块任务队列 */
  private final BlockQueue<BlockTargetPair> ecBlocksToBeReplicated = new BlockQueue<>();
  /** 待该DataNode执行EC重构的块任务队列 */
  private final BlockQueue<BlockECReconstructionInfo> ecBlocksToBeErasureCoded =
      new BlockQueue<>();
  /** 待该DataNode执行恢复的块任务队列 */
  private final BlockQueue<BlockInfo> recoverBlocks = new BlockQueue<>();
  /** 待该DataNode删除失效块集合 */
  private final LightWeightHashSet<Block> invalidateBlocks =
      new LightWeightHashSet<>();

  /* 维护该节点不同存储类型已调度写入块计数，该计数为近似值，写入错误可能导致计数略大于实际值
   * 因为DataNode写入出错后不一定会上报错误减少计数
   */
  private EnumCounters<StorageType> currApproxBlocksScheduled
      = new EnumCounters<>(StorageType.class);
  private EnumCounters<StorageType> prevApproxBlocksScheduled
      = new EnumCounters<>(StorageType.class);
  private long lastBlocksScheduledRollTime = 0;
  private int volumeFailures = 0;
  private VolumeFailureSummary volumeFailureSummary = null;
  
  /** 
   * 节点被禁止标记：当节点不在允许列表中时，禁止该节点和NameNode通信
   */
  private boolean disallowed = false;

  // 尚未确定目标节点的待复制任务数量
  private int pendingReplicationWithoutTargets = 0;

  // 心跳处理使用该标记判断是否是注册后第一次心跳
  private boolean heartbeatedSinceRegistration = false;

  /** 当前可用可写入卷数量 */
  private int numVolumesAvailable = 0;

  /**
   * 构造方法，通过DataNodeID创建描述符
   * @param nodeID DataNode唯一标识
   */
  public DatanodeDescriptor(DatanodeID nodeID) {
    super(nodeID);
    setLastUpdate(Time.now());
    setLastUpdateMonotonic(Time.monotonicNow());
  }

  /**
   * 构造方法，通过DataNodeID和网络位置创建描述符
   * @param nodeID DataNode唯一标识
   * @param networkLocation 节点在网络拓扑中的位置
   */
  public DatanodeDescriptor(DatanodeID nodeID, 
                            String networkLocation) {
    super(nodeID, networkLocation);
    setLastUpdate(Time.now());
    setLastUpdateMonotonic(Time.monotonicNow());
  }

  public CachedBlocksList getPendingCached() {
    return pendingCached;
  }

  public CachedBlocksList getCached() {
    return cached;
  }

  public CachedBlocksList getPendingUncached() {
    return pendingUncached;
  }

  public boolean isAlive() {
    return isAlive;
  }

  public void setAlive(boolean isAlive) {
    this.isAlive = isAlive;
  }

  public synchronized boolean needKeyUpdate() {
    return needKeyUpdate;
  }

  public synchronized void setNeedKeyUpdate(boolean needKeyUpdate) {
    this.needKeyUpdate = needKeyUpdate;
  }

  public LeavingServiceStatus getLeavingServiceStatus() {
    return leavingServiceStatus;
  }

  @VisibleForTesting
  public boolean isHeartbeatedSinceRegistration() {
   return heartbeatedSinceRegistration;
  }

  @VisibleForTesting
  public DatanodeStorageInfo getStorageInfo(String storageID) {
    synchronized (storageMap) {
      return storageMap.get(storageID);
    }
  }

  @VisibleForTesting
  public DatanodeStorageInfo[] getStorageInfos() {
    synchronized (storageMap) {
      final Collection<DatanodeStorageInfo> storages = storageMap.values();
      return storages.toArray(new DatanodeStorageInfo[storages.size()]);
    }
  }

  public EnumSet<StorageType> getStorageTypes() {
    EnumSet<StorageType> storageTypes = EnumSet.noneOf(StorageType.class);
    for (DatanodeStorageInfo dsi : getStorageInfos()) {
      storageTypes.add(dsi.getStorageType());
    }
    return storageTypes;
  }

  public StorageReport[] getStorageReports() {
    final DatanodeStorageInfo[] infos = getStorageInfos();
    final StorageReport[] reports = new StorageReport[infos.length];
    for(int i = 0; i < infos.length; i++) {
      reports[i] = infos[i].toStorageReport();
    }
    return reports;
  }

  /** 检查该节点是否存在内容过时的存储 */
  boolean hasStaleStorages() {
    synchronized (storageMap) {
      for (DatanodeStorageInfo storage : storageMap.values()) {
        if (StorageType.PROVIDED.equals(storage.getStorageType())) {
          // PROVIDED存储跳过过时检查，需要单独验证块报告ID
          continue;
        }
        if (storage.areBlockContentsStale()) {
          return true;
        }
      }
      return false;
    }
  }

  /** 重置该节点所有块相关统计信息和缓存列表 */
  public void resetBlocks() {
    updateStorageStats(this.getStorageReports(), 0L, 0L, 0, 0, null);
    synchronized (invalidateBlocks) {
      this.invalidateBlocks.clear();
    }
    this.volumeFailures = 0;
    // pendingCached, cached, and pendingUncached are protected by the
    // FSN lock.
    this.pendingCached.clear();
    this.cached.clear();
    this.pendingUncached.clear();
  }
  
  /** 清空该节点所有任务队列和缓存列表 */
  public void clearBlockQueues() {
    synchronized (invalidateBlocks) {
      this.invalidateBlocks.clear();
    }
    this.recoverBlocks.clear();
    this.replicateBlocks.clear();
    this.ecBlocksToBeReplicated.clear();
    this.ecBlocksToBeErasureCoded.clear();
    // pendingCached, cached, and pendingUncached are protected by the
    // FSN lock.
    this.pendingCached.clear();
    this.cached.clear();
    this.pendingUncached.clear();
  }

  /** 获取该节点所有存储上的总块数 */
  public int numBlocks() {
    int blocks = 0;
    for (DatanodeStorageInfo entry : getStorageInfos()) {
      blocks += entry.numBlocks();
    }
    return blocks;
  }

  /**
   * 从DataNode心跳更新节点统计信息，标记已收到注册后心跳
   */
  void updateHeartbeat(StorageReport[] reports, long cacheCapacity,
      long cacheUsed, int xceiverCount, int volFailures,
      VolumeFailureSummary volumeFailureSummary) {
    updateHeartbeatState(reports, cacheCapacity, cacheUsed, xceiverCount,
        volFailures, volumeFailureSummary);
    heartbeatedSinceRegistration = true;
  }

  /**
   * 处理DataNode心跳或状态初始化，更新存储统计和心跳时间
   */
  void updateHeartbeatState(StorageReport[] reports, long cacheCapacity,
      long cacheUsed, int xceiverCount, int volFailures,
      VolumeFailureSummary volumeFailureSummary) {
    updateStorageStats(reports, cacheCapacity, cacheUsed, xceiverCount,
        volFailures, volumeFailureSummary);
    setLastUpdate(Time.now());
    setLastUpdateMonotonic(Time.monotonicNow());
    rollBlocksScheduled(getLastUpdateMonotonic());
  }

  /** 更新该节点所有存储统计信息，处理存储失败和过期存储 */
  private void updateStorageStats(StorageReport[] reports, long cacheCapacity,
      long cacheUsed, int xceiverCount, int volFailures,
      VolumeFailureSummary volumeFailureSummary) {
    long totalCapacity = 0;
    long totalRemaining = 0;
    long totalBlockPoolUsed = 0;
    long totalDfsUsed = 0;
    long totalNonDfsUsed = 0;
    Set<String> visitedMount = new HashSet<>();
    Set<DatanodeStorageInfo> failedStorageInfos = null;
    int volumesAvailable = 0;

    // 决定是否需要检查缺失存储并标记失败，逻辑见代码内注释
    final boolean checkFailedStorages;
    if (volumeFailureSummary != null && this.volumeFailureSummary != null) {
      checkFailedStorages = volumeFailureSummary.getLastVolumeFailureDate() >
          this.volumeFailureSummary.getLastVolumeFailureDate();
    } else {
      checkFailedStorages = (volFailures > this.volumeFailures) ||
          !heartbeatedSinceRegistration;
    }

    if (checkFailedStorages) {
      if (this.volumeFailures != volFailures) {
        LOG.info("Number of failed storages changes from {} to {}",
            this.volumeFailures, volFailures);
      }
      synchronized (storageMap) {
        failedStorageInfos =
            new HashSet<>(storageMap.values());
      }
    }

    // 更新全局缓存和传输线程统计
    setCacheCapacity(cacheCapacity);
    setCacheUsed(cacheUsed);
    setXceiverCount(xceiverCount);
    this.volumeFailures = volFailures;
    this.volumeFailureSummary = volumeFailureSummary;
    for (StorageReport report : reports) {

      DatanodeStorageInfo storage = null;
      synchronized (storageMap) {
        storage =
            storageMap.get(report.getStorage().getStorageID());
      }
      if (checkFailedStorages) {
        // 移除本次心跳报告过的存储，剩余未报告的即为失败存储
        failedStorageInfos.remove(storage);
      }

      // 存储处理本次心跳报告
      storage.receivedHeartbeat(report);
      // PROVIDED存储不参与容量统计
      if (StorageType.PROVIDED.equals(storage.getStorageType())) {
        continue;
      }

      // 累加容量统计
      totalCapacity += report.getCapacity();
      totalRemaining += report.getRemaining();
      totalBlockPoolUsed += report.getBlockPoolUsed();
      totalDfsUsed += report.getDfsUsed();
      String mount = report.getMount();
      // 同一个挂载点下多个卷只统计一次非DFS使用空间，避免重复计算
      if (mount