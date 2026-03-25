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

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_RECONSTRUCTION_PENDING_TIMEOUT_SEC_DEFAULT;
import static org.apache.hadoop.util.Time.monotonicNow;

import java.io.PrintWriter;
import java.sql.Time;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.util.Daemon;
import org.slf4j.Logger;

/**
 * 文件说明：HDFS NameNode 端待恢复块管理类，负责跟踪所有正在进行冗余提升的数据块恢复过程
 * 
 * 核心职责：
 * 1. 记录当前正在进行块恢复操作的数据块信息
 * 2. 通过定时器追踪恢复请求的超时情况
 * 3. 后台线程定期清理未完成的超时恢复请求，将这些块重新加入恢复队列
 */
/***************************************************
 * PendingReconstructionBlocks does the bookkeeping of all
 * blocks that gains stronger redundancy.
 *
 * It does the following:
 * 1)  record blocks that gains stronger redundancy at this instant.
 * 2)  a coarse grain timer to track age of reconstruction request
 * 3)  a thread that periodically identifies reconstruction-requests
 *     that never made it.
 *
 ***************************************************/
class PendingReconstructionBlocks {
  private static final Logger LOG = BlockManager.LOG;

  /** 存储所有待恢复块及其恢复信息，Key为块信息，Value为恢复状态信息 */
  private final Map<BlockInfo, PendingBlockInfo> pendingReconstructions;
  /** 存储已超时的待恢复块列表，等待后续重新处理 */
  private final ArrayList<BlockInfo> timedOutItems;
  /** 后台监控超时的守护线程 */
  Daemon timerThread = null;
  /** 标记文件系统是否正在运行，控制监控线程生命周期 */
  private volatile boolean fsRunning = true;
  /** 累计超时的块恢复请求总数，用于指标统计 */
  private long timedOutCount = 0L;

  //
  // It might take anywhere between 5 to 10 minutes before
  // a request is timed out.
  //
  /** 恢复请求超时时间，超过该时间未完成则判定为失败 */
  private volatile long timeout =
      DFS_NAMENODE_RECONSTRUCTION_PENDING_TIMEOUT_SEC_DEFAULT * 1000;
  /** 默认监控线程检查间隔，5分钟 */
  private final static long DEFAULT_RECHECK_INTERVAL = 5 * 60 * 1000;

  /**
   * 构造待恢复块管理器，可自定义超时时间
   * @param timeoutPeriod 自定义超时时间，单位毫秒，大于0时使用自定义值
   */
  PendingReconstructionBlocks(long timeoutPeriod) {
    if ( timeoutPeriod > 0 ) {
      this.timeout = timeoutPeriod;
    }
    pendingReconstructions = new HashMap<>();
    timedOutItems = new ArrayList<>();
  }

  /**
   * 启动后台监控线程，开始定期检查超时恢复请求
   */
  void start() {
    timerThread = new Daemon(new PendingReconstructionMonitor());
    timerThread.start();
  }

  /**
   * 设置恢复请求超时时间
   * @param timeoutPeriod 新的超时时间，单位毫秒
   */
  public void setTimeout(long timeoutPeriod) {
    this.timeout = timeoutPeriod;
  }

  /**
   * 获取当前恢复请求超时时间
   * @return 超时时间，单位毫秒
   */
  public long getTimeout() {
    return this.timeout;
  }

  /**
   * Add a block to the list of pending reconstructions
   * @param block The corresponding block
   */
  /**
   * 增加待恢复块的待恢复副本计数，添加新的目标数据节点
   * @param block 需要恢复的块信息
   * @param targets 目标数据节点存储信息，恢复出的副本将放置在这些节点上
   */
  void increment(BlockInfo block, DatanodeStorageInfo... targets) {
    synchronized (pendingReconstructions) {
      PendingBlockInfo found = pendingReconstructions.get(block);
      if (found == null) {
        // 新的待恢复块，添加到映射表
        pendingReconstructions.put(block, new PendingBlockInfo(targets));
      } else {
        // 已有恢复记录，增加待恢复副本，更新时间戳
        found.incrementReplicas(targets);
        found.setTimeStamp();
      }
    }
  }

  /**
   * One reconstruction request for this block has finished.
   * Decrement the number of pending reconstruction requests
   * for this block.
   *
   * @param dn The DataNode that finishes the reconstruction
   * @return true if the block is decremented to 0 and got removed.
   */
  /**
   * 完成一个块恢复请求，减少待恢复副本计数，计数归零则移除该块
   * @param block 完成恢复的块
   * @param dn 完成恢复的数据节点存储信息
   * @return true如果待恢复计数归零，块已从待恢复列表移除
   */
  boolean decrement(BlockInfo block, DatanodeStorageInfo dn) {
    boolean removed = false;
    synchronized (pendingReconstructions) {
      PendingBlockInfo found = pendingReconstructions.get(block);
      if (found != null) {
        LOG.debug("Removing pending reconstruction for {}", block);
        // 减少该节点对应的待恢复计数
        found.decrementReplicas(dn);
        if (found.getNumReplicas() <= 0) {
          // 所有待恢复都已完成，移除该块
          pendingReconstructions.remove(block);
          removed = true;
        }
      }
    }
    return removed;
  }

  /**
   * Remove the record about the given block from pending reconstructions.
   *
   * @param block
   *          The given block whose pending reconstruction requests need to be
   *          removed
   */
  /**
   * 直接移除指定块的所有待恢复记录
   * @param block 需要移除的块
   * @return 被移除的块恢复信息，如果不存在则返回null
   */
  PendingBlockInfo remove(BlockInfo block) {
    synchronized (pendingReconstructions) {
      return pendingReconstructions.remove(block);
    }
  }

  /**
   * 清空所有待恢复块和超时记录，重置计数器
   */
  public void clear() {
    synchronized (pendingReconstructions) {
      pendingReconstructions.clear();
      synchronized (timedOutItems) {
        timedOutItems.clear();
      }
      timedOutCount = 0L;
    }
  }

  /**
   * The total number of blocks that are undergoing reconstruction.
   */
  /**
   * 获取当前正在进行恢复的块总数
   * @return 待恢复块数量
   */
  int size() {
    synchronized (pendingReconstructions) {
      return pendingReconstructions.size();
    }
  }

  /**
   * How many copies of this block is pending reconstruction?.
   */
  /**
   * 获取指定块当前待恢复的副本数量
   * @param block 查询的块
   * @return 待恢复副本数，如果块不在待恢复列表则返回0
   */
  int getNumReplicas(BlockInfo block) {
    synchronized (pendingReconstructions) {
      PendingBlockInfo found = pendingReconstructions.get(block);
      if (found != null) {
        return found.getNumReplicas();
      }
    }
    return 0;
  }

  /**
   * Used for metrics.
   * @return The number of timeouts
   */
  /**
   * 获取累计超时的块恢复请求总数，用于指标统计
   * @return 累计超时总数（含已处理和待处理）
   */
  long getNumTimedOuts() {
    synchronized (timedOutItems) {
      return timedOutCount + timedOutItems.size();
    }
  }

  /**
   * Returns a list of blocks that have timed out their
   * reconstruction requests. Returns null if no blocks have
   * timed out.
   */
  /**
   * 获取所有已超时的待恢复块，清空超时列表，供NameNode重新发起恢复
   * @return 超时块数组，如果没有超时块返回null
   */
  BlockInfo[] getTimedOutBlocks() {
    synchronized (timedOutItems) {
      if (timedOutItems.size() <= 0) {
        return null;
      }
      int size = timedOutItems.size();
      BlockInfo[] blockList = timedOutItems.toArray(
          new BlockInfo[size]);
      timedOutItems.clear();
      timedOutCount += size;
      return blockList;
    }
  }

  /**
   * An object that contains information about a block that
   * is being reconstructed. It records the timestamp when the
   * system started reconstructing the most recent copy of this
   * block. It also records the list of Datanodes where the
   * reconstruction requests are in progress.
   */
  /**
   * 单个待恢复块的状态信息类，记录恢复时间戳和正在进行恢复的目标节点列表
   */
  static class PendingBlockInfo {
    /** 最近一次恢复请求发起的时间戳，使用单调时间 */
    private long timeStamp;
    /** 当前正在进行恢复的目标数据节点存储列表 */
    private final List<DatanodeStorageInfo> targets;

    /**
     * 构造待恢复块信息实例
     * @param targets 目标数据节点存储数组
     */
    PendingBlockInfo(DatanodeStorageInfo[] targets) {
      this.timeStamp = monotonicNow();
      this.targets = targets == null ? new ArrayList<DatanodeStorageInfo>()
          : new ArrayList<>(Arrays.asList(targets));
    }

    /**
     * 获取最近一次恢复请求的时间戳
     * @return 时间戳（单调时间，毫秒）
     */
    long getTimeStamp() {
      return timeStamp;
    }

    /**
     * 更新时间戳为当前时间，用于重置超时计时
     */
    void setTimeStamp() {
      timeStamp = monotonicNow();
    }

    /**
     * 增加新的待恢复目标节点，去重处理
     * @param newTargets 新增的目标节点数组
     */
    void incrementReplicas(DatanodeStorageInfo... newTargets) {
      if (newTargets != null) {
        for (DatanodeStorageInfo newTarget : newTargets) {
          // 避免重复添加同一节点
          if (!targets.contains(newTarget)) {
            targets.add(newTarget);
          }
        }
      }
    }

    /**
     * 移除指定数据节点的恢复请求，完成恢复后调用
     * @param dn 完成恢复的数据节点存储信息
     */
    void decrementReplicas(DatanodeStorageInfo dn) {
      Iterator<DatanodeStorageInfo> iterator = targets.iterator();
      while (iterator.hasNext()) {
        DatanodeStorageInfo next = iterator.next();
        // 根据数据节点描述符匹配，同一节点不同存储视为同一个节点
        if (next.getDatanodeDescriptor() == dn.getDatanodeDescriptor()) {
          iterator.remove();
        }
      }
    }

    /**
     * 获取当前待恢复副本数量
     * @return 待恢复副本数等于目标节点数
     */
    int getNumReplicas() {
      return targets.size();
    }

    /**
     * 获取所有待恢复目标节点列表
     * @return 目标节点存储列表
     */
    List<DatanodeStorageInfo> getTargets() {
      return targets;
    }
  }

  /*
   * A periodic thread that scans for blocks that never finished
   * their reconstruction request.
   */
  /**
   * 后台监控线程，定期扫描检测超时的块恢复请求
   */
  class PendingReconstructionMonitor implements Runnable {
    @Override
    public void run() {
      while (fsRunning) {
        // 检查间隔取默认间隔和超时时间中的较小值，避免超时后长时间不检测
        long period = Math.min(DEFAULT_RECHECK_INTERVAL, timeout);
        try {
          // 执行超时检查
          pendingReconstructionCheck();
          Thread.sleep(period);
        } catch (InterruptedException ie) {
          LOG.debug("PendingReconstructionMonitor thread is interrupted.", ie);
        }
      }
    }

    /**
     * Iterate through all items and detect timed-out items
     */
    /**
     * 遍历所有待恢复块，检测并移除超时的恢复请求
     */
    void pendingReconstructionCheck() {
      synchronized (pendingReconstructions) {
        Iterator<Map.Entry<BlockInfo, PendingBlockInfo>> iter =
            pendingReconstructions.entrySet().iterator();
        long now = monotonicNow();
        LOG.debug("PendingReconstructionMonitor checking Q");
        while (iter.hasNext()) {
          Map.Entry<BlockInfo, PendingBlockInfo> entry = iter.next();
          PendingBlockInfo pendingBlock = entry.getValue();
          // 判断是否超过超时时间
          if (now > pendingBlock.getTimeStamp() + timeout) {
            BlockInfo block = entry.getKey();
            // 添加到超时列表，后续会被重新处理
            synchronized (timedOutItems) {
              timedOutItems.add(block);
            }
            LOG.warn("PendingReconstructionMonitor timed out " + block);
            // 增加指标计数
            NameNode.getNameNodeMetrics().incTimeoutReReplications();
            // 从待恢复列表移除
            iter.remove();
          }
        }
      }
    }
  }

  /**
   * @return timer thread.
   */
  @VisibleForTesting
  /**
   * 获取监控线程实例，仅用于测试
   * @return 后台监控守护线程
   */
  public Daemon getTimerThread() {
    return timerThread;
  }
  /*
   * Shuts down the pending reconstruction monitor thread.
   * Waits for the thread to exit.
   */
  /**
   * 停止后台监控线程，等待线程退出
   */
  void stop() {
    fsRunning = false;
    if(timerThread == null) return;
    timerThread.interrupt();
    try {
      // 最多等待3秒让线程退出
      timerThread.join(3000);
    } catch (InterruptedException ie) {
    }
  }

  /**
   * Iterate through all items and print them.
   */
  /**
   * 将当前所有待恢复块信息写入元数据保存文件，用于元数据备份和故障排查
   * @param out 输出打印Writer
   */
  void metaSave(PrintWriter out) {
    synchronized (pendingReconstructions) {
      out.println("Metasave: Blocks being reconstructed: " +
                  pendingReconstructions.size());
      for (Map.Entry<BlockInfo, PendingBlockInfo> entry :
          pendingReconstructions.entrySet()) {
        PendingBlockInfo pendingBlock = entry.getValue();
        Block block = entry.getKey();
        out.println(block +
            " StartTime: " + new Time(pendingBlock.timeStamp) +
            " NumReconstructInProgress: " +
            pendingBlock.getNumReplicas());
      }
    }
  }

  /**
   * 获取指定块的所有待恢复目标节点列表
   * @param block 查询的块
   * @return 目标节点存储列表的副本，如果块不在待恢复列表返回null
   */
  List<DatanodeStorageInfo> getTargets(BlockInfo block) {
    synchronized (pendingReconstructions) {
      PendingBlockInfo found = pendingReconstructions.get(block);
      if (found != null) {
        return new ArrayList<>(found.targets);
      }
    }
    return null;
  }
}