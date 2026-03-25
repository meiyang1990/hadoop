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
package org.apache.hadoop.hdfs.server.datanode;

import static org.apache.hadoop.util.Time.monotonicNow;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.datanode.metrics.DataNodeMetrics;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo.BlockStatus;
import org.apache.hadoop.hdfs.server.protocol.StorageReceivedDeletedBlocks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.collect.Maps;

/**
 * 文件：增量块报告管理器
 * 功能：负责管理DataNode端的增量块报告（IBR），缓存未上报的块变更信息，按调度策略向NameNode上报块的接收/删除变更
 */
@InterfaceAudience.Private
class IncrementalBlockReportManager {
  private static final Logger LOG = LoggerFactory.getLogger(
      IncrementalBlockReportManager.class);

  /**
   * 单个存储目录的增量块报告缓存
   * 职责：缓存该存储下所有未上报的块变更信息，统计指标
   */
  private static class PerStorageIBR {
    /** 当前存储未上报的块变更，key为块对象，value为变更信息 */
    final Map<Block, ReceivedDeletedBlockInfo> blocks = Maps.newHashMap();

    private DataNodeMetrics dnMetrics;
    /**
     * 构造函数
     * @param dnMetrics DataNode指标统计对象
     */
    PerStorageIBR(final DataNodeMetrics dnMetrics) {
      this.dnMetrics = dnMetrics;
    }

    /**
     * 从当前缓存中移除指定块
     * @param block 要移除的块
     * @return 被移除的块变更信息，不存在则返回null
     */
    ReceivedDeletedBlockInfo remove(Block block) {
      return blocks.remove(block);
    }

    /**
     * 取出所有缓存的块变更并清空缓存
     * @return 所有块变更数组，缓存为空则返回null
     */
    ReceivedDeletedBlockInfo[] removeAll() {
      final int size = blocks.size();
      if (size == 0) {
        return null;
      }

      final ReceivedDeletedBlockInfo[] rdbis = blocks.values().toArray(
          new ReceivedDeletedBlockInfo[size]);
      blocks.clear();
      return rdbis;
    }

    /**
     * 添加块变更到当前缓存
     * @param rdbi 块变更信息
     */
    void put(ReceivedDeletedBlockInfo rdbi) {
      blocks.put(rdbi.getBlock(), rdbi);
      increaseBlocksCounter(rdbi);
    }

    /**
     * 根据块变更状态更新DataNode对应指标计数器
     * @param receivedDeletedBlockInfo 块变更信息
     */
    private void increaseBlocksCounter(
        final ReceivedDeletedBlockInfo receivedDeletedBlockInfo) {
      switch (receivedDeletedBlockInfo.getStatus()) {
      case RECEIVING_BLOCK:
        dnMetrics.incrBlocksReceivingInPendingIBR();
        break;
      case RECEIVED_BLOCK:
        dnMetrics.incrBlocksReceivedInPendingIBR();
        break;
      case DELETED_BLOCK:
        dnMetrics.incrBlocksDeletedInPendingIBR();
        break;
      default:
        break;
      }
      dnMetrics.incrBlocksInPendingIBR();
    }

    /**
     * 添加缺失的块变更到缓存，仅当块不存在时才添加
     * @param rdbis 待添加的块变更数组
     * @return 成功添加的缺失块数量
     */
    int putMissing(ReceivedDeletedBlockInfo[] rdbis) {
      int count = 0;
      for (ReceivedDeletedBlockInfo rdbi : rdbis) {
        if (!blocks.containsKey(rdbi.getBlock())) {
          put(rdbi);
          count++;
        }
      }
      return count;
    }
  }

  /** 存储所有存储目录对应的未上报增量块报告，key为存储目录，value为对应缓存 */
  private final Map<DatanodeStorage, PerStorageIBR> pendingIBRs
      = Maps.newHashMap();

  /** 标记是否有等待上报的增量块报告，触发线程发送 */
  private volatile boolean readyToSend = false;

  /** 两次增量块报告之间的最小时间间隔 */
  private final long ibrInterval;

  /** 上次发送增量块报告的时间戳 */
  private volatile long lastIBR;
  private DataNodeMetrics dnMetrics;

  /**
   * 构造增量块报告管理器
   * @param ibrInterval 增量块报告最小间隔
   * @param dnMetrics DataNode指标统计对象
   */
  IncrementalBlockReportManager(
      final long ibrInterval,
      final DataNodeMetrics dnMetrics) {
    this.ibrInterval = ibrInterval;
    this.lastIBR = monotonicNow() - ibrInterval;
    this.dnMetrics = dnMetrics;
  }

  /**
   * 检查是否满足立即发送增量块报告的条件
   * @return true表示可以立即发送，false表示需要等待
   */
  boolean sendImmediately() {
    return readyToSend && monotonicNow() - ibrInterval >= lastIBR;
  }

  /**
   * 等待到下一次可以发送增量块报告的时间
   * @param waitTime 最大等待时间
   */
  synchronized void waitTillNextIBR(long waitTime) {
    if (waitTime > 0 && !sendImmediately()) {
      try {
        wait(ibrInterval > 0 && ibrInterval < waitTime? ibrInterval: waitTime);
      } catch (InterruptedException ie) {
        LOG.warn(getClass().getSimpleName() + " interrupted");
      }
    }
  }

  /**
   * 生成所有存储目录待上报的增量块报告，清空已取出的缓存
   * @return 按存储组织的增量块报告数组
   */
  private synchronized StorageReceivedDeletedBlocks[] generateIBRs() {
    final List<StorageReceivedDeletedBlocks> reports
        = new ArrayList<>(pendingIBRs.size());
    for (Map.Entry<DatanodeStorage, PerStorageIBR> entry
        : pendingIBRs.entrySet()) {
      final PerStorageIBR perStorage = entry.getValue();

      // 获取该存储下所有待上报块变更
      final ReceivedDeletedBlockInfo[] rdbi = perStorage.removeAll();
      if (rdbi != null) {
        reports.add(new StorageReceivedDeletedBlocks(entry.getKey(), rdbi));
      }
    }

    // 重置待上报块指标计数器
    this.dnMetrics.resetBlocksInPendingIBR();

    readyToSend = false;
    return reports.toArray(new StorageReceivedDeletedBlocks[reports.size()]);
  }

  /**
   * 将发送失败的块变更重新放回待上报缓存，仅添加当前缓存不存在的块
   * @param reports 发送失败的增量块报告
   */
  private synchronized void putMissing(StorageReceivedDeletedBlocks[] reports) {
    for (StorageReceivedDeletedBlocks r : reports) {
      pendingIBRs.get(r.getStorage()).putMissing(r.getBlocks());
    }
    if (reports.length > 0) {
      readyToSend = true;
    }
  }

  /**
   * 生成并向NameNode发送增量块报告
   * @param namenode NameNode协议代理
   * @param registration DataNode注册信息
   * @param bpid 块池ID
   * @param nnRpcLatencySuffix NameNode RPC延迟指标后缀
   * @throws IOException 发送RPC调用失败时抛出
   */
  void sendIBRs(DatanodeProtocol namenode, DatanodeRegistration registration,
      String bpid, String nnRpcLatencySuffix) throws IOException {
    // 加锁生成待上报报告
    final StorageReceivedDeletedBlocks[] reports = generateIBRs();
    if (reports.length == 0) {
      // 无变更，无需上报
      return;
    }

    // 锁外发送RPC，避免阻塞其他操作
    if (LOG.isDebugEnabled()) {
      LOG.debug("call blockReceivedAndDeleted: " + Arrays.toString(reports));
    }
    boolean success = false;
    final long startTime = monotonicNow();
    try {
      namenode.blockReceivedAndDeleted(registration, bpid, reports);
      success = true;
    } finally {

      if (success) {
        dnMetrics.addIncrementalBlockReport(monotonicNow() - startTime,
            nnRpcLatencySuffix);
        lastIBR = startTime;
      } else {
        // 发送失败，将块变更放回待上报队列，下次重试
        putMissing(reports);
        LOG.warn("Failed to call blockReceivedAndDeleted: {}, nnId: {}"
            + ", duration(ms): {}", Arrays.toString(reports),
            nnRpcLatencySuffix, monotonicNow() - startTime);
      }
    }
  }

  /**
   * 获取指定存储对应的增量块报告缓存，不存在则创建
   * @param storage 目标存储
   * @return 指定存储对应的增量块报告缓存
   */
  private PerStorageIBR getPerStorageIBR(DatanodeStorage storage) {
    PerStorageIBR perStorage = pendingIBRs.get(storage);
    if (perStorage == null) {
      // 首次访问该存储，创建新缓存
      perStorage = new PerStorageIBR(dnMetrics);
      pendingIBRs.put(storage, perStorage);
    }
    return perStorage;
  }

  /**
   * 添加块变更到待上报队列，移除同一块已存在的旧变更
   * @param rdbi 块变更信息
   * @param storage 块所在存储
   */
  @VisibleForTesting
  synchronized void addRDBI(ReceivedDeletedBlockInfo rdbi,
      DatanodeStorage storage) {
    // 先移除其他存储中同一块的旧条目（理论上不会出现，做防御性处理）
    for (PerStorageIBR perStorage : pendingIBRs.values()) {
      if (perStorage.remove(rdbi.getBlock()) != null) {
        break;
      }
    }
    getPerStorageIBR(storage).put(rdbi);
  }

  /**
   * 通知NameNode块状态变更，根据块状态决定发送时机
   * @param rdbi 块变更信息
   * @param storage 块所在存储
   * @param isOnTransientStorage 是否在临时存储上
   */
  synchronized void notifyNamenodeBlock(ReceivedDeletedBlockInfo rdbi,
      DatanodeStorage storage, boolean isOnTransientStorage) {
    addRDBI(rdbi, storage);

    final BlockStatus status = rdbi.getStatus();
    if (status == BlockStatus.RECEIVING_BLOCK) {
      // 接收中块，在下一次心跳统一发送
      readyToSend = true;
    } else if (status == BlockStatus.RECEIVED_BLOCK) {
      // 接收完成块，立即触发上报
      triggerIBR(isOnTransientStorage);
    }
  }

  /**
   * 触发增量块报告发送，满足条件则唤醒等待线程
   * @param force 是否强制立即发送（忽略间隔限制）
   */
  synchronized void triggerIBR(boolean force) {
    readyToSend = true;
    if (force) {
      // 强制满足时间间隔条件
      lastIBR = monotonicNow() - ibrInterval;
    }
    if (sendImmediately()) {
      notifyAll();
    }
  }

  /**
   * 测试用：触发删除报告发送，等待所有变更上报完成
   */
  @VisibleForTesting
  synchronized void triggerDeletionReportForTests() {
    triggerIBR(true);

    while (sendImmediately()) {
      try {
        wait(100);
      } catch (InterruptedException e) {
        return;
      }
    }
  }

  /**
   * 清空所有待上报的增量块报告缓存
   */
  void clearIBRs() {
    pendingIBRs.clear();
  }

  /**
   * 测试用：获取待上报存储的数量
   * @return 有未上报变更的存储数量
   */
  @VisibleForTesting
  int getPendingIBRSize() {
    return pendingIBRs.size();
  }
}