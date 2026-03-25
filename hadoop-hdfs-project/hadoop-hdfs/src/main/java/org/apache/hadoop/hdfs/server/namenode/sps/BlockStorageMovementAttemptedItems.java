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
package org.apache.hadoop.hdfs.server.namenode.sps;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_RECHECK_TIMEOUT_MILLIS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_RECHECK_TIMEOUT_MILLIS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_SELF_RETRY_TIMEOUT_MILLIS_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_SELF_RETRY_TIMEOUT_MILLIS_KEY;
import static org.apache.hadoop.util.Time.monotonicNow;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.server.namenode.sps.StoragePolicySatisfier.AttemptedItemInfo;
import org.apache.hadoop.hdfs.server.namenode.sps.StoragePolicySatisfier.StorageTypeNodePair;
import org.apache.hadoop.util.Daemon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * 文件：存储策略满足器模块中跟踪已发起块存储移动请求的监控类
 * 功能：跟踪已经发送给数据节点的块存储移动请求，检测请求是否完成，超时未完成的自动重试
 * 核心职责：维护所有正在进行的块移动任务状态，处理数据节点上报的完成消息，超时后自动将任务放回待处理队列重试
 */
public class BlockStorageMovementAttemptedItems {
  private static final Logger LOG =
      LoggerFactory.getLogger(BlockStorageMovementAttemptedItems.class);

  /**
   * 存储所有已经发起给数据节点处理的块移动任务信息
   */
  private final List<AttemptedItemInfo> storageMovementAttemptedItems;
  /**
   * 存储每个待移动块对应的目标存储类型和数据节点配对信息，用于匹配数据节点上报的完成消息
   */
  private Map<Block, Set<StorageTypeNodePair>> scheduledBlkLocs;
  // 维护已完成移动的块队列，异步更新待处理任务列表，降低锁竞争
  private final BlockingQueue<Block> movementFinishedBlocks;
  private volatile boolean monitorRunning = true;
  private Daemon timerThread = null;
  private final Context context;
  //
  // 块移动请求超时时间通常在5-10分钟，超时后自动重试
  //
  private long selfRetryTimeout = 5 * 60 * 1000;

  //
  // 监控线程检查间隔的最小超时时间通常在1-2分钟
  //
  private long minCheckTimeout = 1 * 60 * 1000; // minimum value
  /**
   * 用于将超时/已完成任务放回待处理队列的回调接口
   */
  private BlockStorageMovementNeeded blockStorageMovementNeeded;
  private final SPSService service;

  /**
   * 构造函数，初始化跟踪器，加载超时配置
   * @param service 存储策略满足器服务实例，用于获取配置
   * @param unsatisfiedStorageMovementFiles 待处理块移动队列，用于放回需要重试的任务
   * @param context 上下文回调，用于通知SPS服务移动任务尝试结果
   */
  public BlockStorageMovementAttemptedItems(SPSService service,
      BlockStorageMovementNeeded unsatisfiedStorageMovementFiles,
      Context context) {
    this.service = service;
    // 从配置加载监控线程检查间隔超时时间
    long recheckTimeout = this.service.getConf().getLong(
        DFS_STORAGE_POLICY_SATISFIER_RECHECK_TIMEOUT_MILLIS_KEY,
        DFS_STORAGE_POLICY_SATISFIER_RECHECK_TIMEOUT_MILLIS_DEFAULT);
    if (recheckTimeout > 0) {
      this.minCheckTimeout = Math.min(minCheckTimeout, recheckTimeout);
    }

    // 从配置加载任务自动重试超时时间
    this.selfRetryTimeout = this.service.getConf().getLong(
        DFS_STORAGE_POLICY_SATISFIER_SELF_RETRY_TIMEOUT_MILLIS_KEY,
        DFS_STORAGE_POLICY_SATISFIER_SELF_RETRY_TIMEOUT_MILLIS_DEFAULT);
    this.blockStorageMovementNeeded = unsatisfiedStorageMovementFiles;
    storageMovementAttemptedItems = new ArrayList<>();
    scheduledBlkLocs = new HashMap<>();
    movementFinishedBlocks = new LinkedBlockingQueue<>();
    this.context = context;
  }

  /**
   * 添加新发起的块移动任务到跟踪列表
   * @param startPathId 满足器路径起始ID
   * @param fileId 文件ID
   * @param monotonicNow 当前时间戳
   * @param assignedBlocks 需要移动的块及其目标位置信息
   * @param retryCount 当前任务重试次数
   */
  public void add(long startPathId, long fileId, long monotonicNow,
      Map<Block, Set<StorageTypeNodePair>> assignedBlocks, int retryCount) {
    AttemptedItemInfo itemInfo = new AttemptedItemInfo(startPathId, fileId,
        monotonicNow, assignedBlocks.keySet(), retryCount);
    synchronized (storageMovementAttemptedItems) {
      storageMovementAttemptedItems.add(itemInfo);
    }
    synchronized (scheduledBlkLocs) {
      scheduledBlkLocs.putAll(assignedBlocks);
    }
  }

  /**
   * 接收数据节点上报的块移动尝试完成通知
   * @param reportedDn 上报完成的数据节点
   * @param type 目标存储类型
   * @param reportedBlock 完成移动的块
   */
  public void notifyReportedBlock(DatanodeInfo reportedDn, StorageType type,
      Block reportedBlock) {
    synchronized (scheduledBlkLocs) {
      if (scheduledBlkLocs.size() <= 0) {
        return;
      }
      matchesReportedBlock(reportedDn, type, reportedBlock);
    }
  }

  /**
   * 匹配上报的块移动完成消息，更新跟踪状态
   */
  private void matchesReportedBlock(DatanodeInfo reportedDn, StorageType type,
      Block reportedBlock) {
    Set<StorageTypeNodePair> blkLocs = scheduledBlkLocs.get(reportedBlock);
    if (blkLocs == null) {
      return; // 未知块，直接跳过
    }

    for (StorageTypeNodePair dn : blkLocs) {
      boolean foundDn = dn.getDatanodeInfo().compareTo(reportedDn) == 0 ? true
          : false;
      boolean foundType = dn.getStorageType().equals(type);
      if (foundDn && foundType) {
        blkLocs.remove(dn);
        Block[] mFinishedBlocks = new Block[1];
        mFinishedBlocks[0] = reportedBlock;
        context.notifyMovementTriedBlocks(mFinishedBlocks);
        // 当前块所有目标位置都已上报完成
        if (blkLocs.size() <= 0) {
          movementFinishedBlocks.add(reportedBlock);
          scheduledBlkLocs.remove(reportedBlock); // 清理已完成块
        }
        return; // 找到匹配项，返回
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Reported block:{} not found in attempted blocks. Datanode:{}"
          + ", StorageType:{}", reportedBlock, reportedDn, type);
    }
  }

  /**
   * 启动后台监控线程
   */
  public synchronized void start() {
    monitorRunning = true;
    timerThread = new Daemon(new BlocksStorageMovementAttemptMonitor());
    timerThread.setName("BlocksStorageMovementAttemptMonitor");
    timerThread.start();
  }

  /**
   * 停止后台监控线程，清空所有队列
   */
  public synchronized void stop() {
    monitorRunning = false;
    if (timerThread != null) {
      timerThread.interrupt();
    }
    this.clearQueues();
  }

  /**
   * 优雅停止监控线程，等待最多3秒退出
   */
  synchronized void stopGracefully() {
    if (timerThread == null) {
      return;
    }
    if (monitorRunning) {
      stop();
    }
    try {
      timerThread.join(3000);
    } catch (InterruptedException ie) {
    }
  }

  /**
   * 后台监控线程，定期检查块移动任务状态，处理完成和超时任务
   */
  private class BlocksStorageMovementAttemptMonitor implements Runnable {
    @Override
    public void run() {
      while (monitorRunning) {
        try {
          // 处理已完成上报的块，更新任务列表
          blockStorageMovementReportedItemsCheck();
          // 检查未完成的任务，超时则重试
          blocksStorageMovementUnReportedItemsCheck();
          Thread.sleep(minCheckTimeout);
        } catch (InterruptedException ie) {
          LOG.info("BlocksStorageMovementAttemptMonitor thread "
              + "is interrupted.", ie);
        } catch (IOException ie) {
          LOG.warn("BlocksStorageMovementAttemptMonitor thread "
              + "received exception and exiting.", ie);
        }
      }
    }
  }

  /**
   * 检查所有未完成上报的块移动任务，超时未响应的放回待处理队列重试
   */
  @VisibleForTesting
  void blocksStorageMovementUnReportedItemsCheck() {
    synchronized (storageMovementAttemptedItems) {
      Iterator<AttemptedItemInfo> iter = storageMovementAttemptedItems
          .iterator();
      long now = monotonicNow();
      while (iter.hasNext()) {
        AttemptedItemInfo itemInfo = iter.next();
        // 当前时间超过最后尝试时间加上重试超时，触发重试
        if (now > itemInfo.getLastAttemptedOrReportedTime()
            + selfRetryTimeout) {
          long file = itemInfo.getFile();
          ItemInfo candidate = new ItemInfo(itemInfo.getStartPath(), file,
              itemInfo.getRetryCount() + 1);
          blockStorageMovementNeeded.add(candidate);
          iter.remove();
          LOG.info("TrackID: {} becomes timed out and moved to needed "
              + "retries queue for next iteration.", file);
        }
      }
    }
  }

  /**
   * 处理所有已上报完成的块，更新任务列表，任务所有块完成则放回队列触发下一次检查
   * @throws IOException IO异常
   */
  @VisibleForTesting
  void blockStorageMovementReportedItemsCheck() throws IOException {
    // 批量取出所有已完成块处理
    Collection<Block> finishedBlks = new ArrayList<>();
    movementFinishedBlocks.drainTo(finishedBlks);

    // 更新任务列表移除已完成块
    for (Block blk : finishedBlks) {
      synchronized (storageMovementAttemptedItems) {
        Iterator<AttemptedItemInfo> iterator = storageMovementAttemptedItems
            .iterator();
        while (iterator.hasNext()) {
          AttemptedItemInfo attemptedItemInfo = iterator.next();
          attemptedItemInfo.getBlocks().remove(blk);
          // 当前任务所有块都已完成，放回待处理队列重新检查存储策略满足情况
          if (attemptedItemInfo.getBlocks().isEmpty()) {
            blockStorageMovementNeeded.add(new ItemInfo(
                attemptedItemInfo.getStartPath(), attemptedItemInfo.getFile(),
                attemptedItemInfo.getRetryCount() + 1));
            iterator.remove();
          }
        }
      }
    }
  }

  @VisibleForTesting
  public int getMovementFinishedBlocksCount() {
    return movementFinishedBlocks.size();
  }

  @VisibleForTesting
  public int getAttemptedItemsCount() {
    synchronized (storageMovementAttemptedItems) {
      return storageMovementAttemptedItems.size();
    }
  }

  @VisibleForTesting
  public List<AttemptedItemInfo> getStorageMovementAttemptedItems() {
    return storageMovementAttemptedItems;
  }

  @VisibleForTesting
  public BlockingQueue<Block> getMovementFinishedBlocks() {
    return movementFinishedBlocks;
  }

  /**
   * 清空所有跟踪队列
   */
  public void clearQueues() {
    movementFinishedBlocks.clear();
    synchronized (storageMovementAttemptedItems) {
      storageMovementAttemptedItems.clear();
    }
    synchronized (scheduledBlkLocs) {
      scheduledBlkLocs.clear();
    }
  }
}