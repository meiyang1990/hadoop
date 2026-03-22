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

import static org.apache.hadoop.util.ExitUtil.terminate;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.CacheDirective;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor.CachedBlocksList.Type;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.hdfs.server.namenode.CacheManager;
import org.apache.hadoop.hdfs.server.namenode.CachePool;
import org.apache.hadoop.hdfs.server.namenode.CachedBlock;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory.DirOp;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.hdfs.util.ReadOnlyList;
import org.apache.hadoop.hdfs.util.RwLockMode;
import org.apache.hadoop.util.GSet;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.concurrent.SubjectInheritingThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.util.Preconditions;

/**
 * @fileoverview HDFS缓存副本监控线程，负责定期扫描命名系统，根据缓存指令调度块缓存复制。
 * 该监控器在NameNode启动时执行全量扫描，之后按照可配置的间隔定期扫描。
 */
@InterfaceAudience.LimitedPrivate({"HDFS"})
/**
 * 缓存复制监控器，维护HDFS块缓存的副本复制状态，定期更新需要缓存的块并调度DataNode执行缓存操作。
 * 核心职责：定期扫描缓存指令和已缓存块集合，调整各DataNode上的缓存块数量，满足缓存副本配置要求；
 * 处理缓存指令变更，在需要时触发立即重新扫描；管理缓存空间配额，超过缓存池限制的文件不进行缓存。
 */
public class CacheReplicationMonitor extends SubjectInheritingThread implements Closeable {

  private static final Logger LOG =
      LoggerFactory.getLogger(CacheReplicationMonitor.class);

  private final FSNamesystem namesystem;

  private final BlockManager blockManager;

  private final CacheManager cacheManager;

  private final GSet<CachedBlock, CachedBlock> cachedBlocks;

  /**
   * 伪随机数生成源，用于随机选择DataNode进行缓存。
   */
  private static final Random random = new Random();

  /**
   * 定期扫描命名系统缓存变更的时间间隔（毫秒）。
   */
  private final long intervalMs;

  /**
   * CacheReplicationMonitor锁，用于同步启动和等待重新扫描操作。
   */
  private final ReentrantLock lock;

  /**
   * 条件变量，通知扫描线程需要立即执行重新扫描。
   */
  private final Condition doRescan;

  /**
   * 条件变量，通知等待线程重新扫描已完成。
   */
  private final Condition scanFinished;

  /**
   * 已完成的扫描次数，用于等待扫描完成，由CacheReplicationMonitor锁保护。
   */
  private long completedScanCount = 0;

  /**
   * 当前正在执行的扫描序号，无扫描进行时为-1，由CacheReplicationMonitor锁保护。
   */
  private long curScanCount = -1;

  /**
   * 需要完成的扫描次数，由CacheReplicationMonitor锁保护。
   */
  private long neededScanCount = 0;

  /**
   * 监控线程是否应该终止，由CacheReplicationMonitor锁保护。
   */
  private boolean shutdown = false;

  /**
   * 当前扫描的标记位，用于标记哪些块在本轮扫描中被访问过。
   */
  private boolean mark = false;

  /**
   * 上一次扫描处理的缓存指令数量。
   */
  private int scannedDirectives;

  /**
   * 上一次扫描处理的块数量。
   */
  private long scannedBlocks;

  /**
   * 上次扫描开始时间，用于避免长时间持有全局锁。
   */
  private long lastScanTimeMs;

  /**
   * 构造CacheReplicationMonitor实例。
   * @param namesystem NameNode的FSNamesystem对象
   * @param cacheManager 缓存管理器实例
   * @param intervalMs 定期扫描间隔（毫秒）
   * @param lock 同步锁对象
   */
  public CacheReplicationMonitor(FSNamesystem namesystem,
      CacheManager cacheManager, long intervalMs, ReentrantLock lock) {
    this.namesystem = namesystem;
    this.blockManager = namesystem.getBlockManager();
    this.cacheManager = cacheManager;
    this.cachedBlocks = cacheManager.getCachedBlocks();
    this.intervalMs = intervalMs;
    this.lock = lock;
    this.doRescan = this.lock.newCondition();
    this.scanFinished = this.lock.newCondition();
  }

  @Override
  public void work() {
    long startTimeMs = 0;
    Thread.currentThread().setName("CacheReplicationMonitor(" +
        System.identityHashCode(this) + ")");
    LOG.info("Starting CacheReplicationMonitor with interval " +
             intervalMs + " milliseconds");
    try {
      long curTimeMs = Time.monotonicNow();
      while (true) {
        lock.lock();
        try {
          while (true) {
            // 检查是否需要关闭线程
            if (shutdown) {
              LOG.info("Shutting down CacheReplicationMonitor");
              return;
            }
            // 有未完成的扫描请求，需要立即执行
            if (completedScanCount < neededScanCount) {
              LOG.debug("Rescanning because of pending operations");
              break;
            }
            // 计算距离下一次定期扫描剩余时间
            long delta = (startTimeMs + intervalMs) - curTimeMs;
            // 到预定扫描时间，触发扫描
            if (delta <= 0) {
              LOG.debug("Rescanning after {} milliseconds", (curTimeMs - startTimeMs));
              break;
            }
            // 等待唤醒或超时
            doRescan.await(delta, TimeUnit.MILLISECONDS);
            curTimeMs = Time.monotonicNow();
          }
        } finally {
          lock.unlock();
        }
        startTimeMs = curTimeMs;
        // 翻转扫描标记位，本轮扫描标记和上一轮区分
        mark = !mark;
        // 执行重新扫描
        rescan();
        curTimeMs = Time.monotonicNow();
        // 更新同步计数并通知等待线程
        lock.lock();
        try {
          completedScanCount = curScanCount;
          curScanCount = -1;
          scanFinished.signalAll();
        } finally {
          lock.unlock();
        }
        LOG.debug("Scanned {} directive(s) and {} block(s) in {} millisecond(s).",
            scannedDirectives, scannedBlocks, (curTimeMs - startTimeMs));
      }
    } catch (InterruptedException e) {
      LOG.info("Shutting down CacheReplicationMonitor.");
      return;
    } catch (Throwable t) {
      LOG.error("Thread exiting", t);
      terminate(1, t);
    }
  }

  /**
   * 等待当前待处理的重新扫描完成，不强制触发新扫描。
   * 调用该方法必须持有CRM锁，且不能持有FSN写锁。
   */
  public void waitForRescanIfNeeded() {
    Preconditions.checkArgument(!namesystem.hasWriteLock(RwLockMode.FS),
        "Must not hold the FSN write lock when waiting for a rescan.");
    Preconditions.checkArgument(lock.isHeldByCurrentThread(),
        "Must hold the CRM lock when waiting for a rescan.");
    if (neededScanCount <= completedScanCount) {
      return;
    }
    // 如果当前没有扫描进行，触发扫描
    if (curScanCount < 0) {
      doRescan.signal();
    }
    // 等待扫描完成
    while ((!shutdown) && (completedScanCount < neededScanCount)) {
      try {
        scanFinished.await();
      } catch (InterruptedException e) {
        LOG.warn("Interrupted while waiting for CacheReplicationMonitor"
            + " rescan", e);
        break;
      }
    }
  }

  /**
   * 标记CacheManager发生变更，需要执行重新扫描。
   * 调用该方法必须持有CRM锁。
   */
  public void setNeedsRescan() {
    Preconditions.checkArgument(lock.isHeldByCurrentThread(),
        "Must hold the CRM lock when setting the needsRescan bit.");
    if (curScanCount >= 0) {
      // 当前已有扫描进行，需要等待下一次扫描
      neededScanCount = curScanCount + 1;
    } else {
      // 当前无扫描，需要在本次完成后新增一次扫描
      neededScanCount = completedScanCount + 1;
    }
  }

  /**
   * 关闭监控线程，终止扫描循环。
   */
  @Override
  public void close() throws IOException {
    Preconditions.checkArgument(namesystem.hasWriteLock(RwLockMode.GLOBAL));
    lock.lock();
    try {
      if (shutdown) return;
      shutdown = true;
      // 唤醒所有等待线程
      doRescan.signalAll();
      scanFinished.signalAll();
    } finally {
      lock.unlock();
    }
  }

  /**
   * 执行一次完整重新扫描，更新缓存块状态并生成新的缓存/移除指令。
   */
  private void rescan() throws InterruptedException {
    scannedDirectives = 0;
    scannedBlocks = 0;
    lastScanTimeMs = Time.monotonicNow();
    try {
      // 获取FS全局写锁
      namesystem.writeLock(RwLockMode.GLOBAL);
      try {
        lock.lock();
        // 关闭检查
        if (shutdown) {
          throw new InterruptedException("CacheReplicationMonitor was " +
              "shut down.");
        }
        // 更新当前扫描序号
        curScanCount = completedScanCount + 1;
      } finally {
        lock.unlock();
      }

      resetStatistics();
      rescanCacheDirectives();
      rescanCachedBlockMap();
      // 重置DataNode上次缓存指令发送时间
      blockManager.getDatanodeManager().resetLastCachingDirectiveSentTime();
    } finally {
      // 释放FS全局写锁
      namesystem.writeUnlock(RwLockMode.GLOBAL, "cacheReplicationMonitorRescan");
    }
  }

  /**
   * 重置所有缓存池和缓存指令的统计信息。
   */
  private void resetStatistics() {
    for (CachePool pool: cacheManager.getCachePools()) {
      pool.resetStatistics();
    }
    for (CacheDirective directive: cacheManager.getCacheDirectives()) {
      directive.resetStatistics();
    }
  }

  /**
   * 如果长时间持有锁，则先释放再重新获取，避免阻塞其他操作。
   * @param last 上次释放锁的时间
   */
  private void reacquireLock(long last) {
    long now = Time.monotonicNow();
    if (now - last > cacheManager.getMaxLockTimeMs()) {
      try {
        // 临时释放全局写锁
        namesystem.writeUnlock(RwLockMode.GLOBAL, "cacheReplicationMonitorRescan");
        // 等待一段时间让其他操作执行
        Thread.sleep(cacheManager.getSleepTimeMs());
      } catch (InterruptedException e) {
      } finally {
        // 重新获取全局写锁
        namesystem.writeLock(RwLockMode.GLOBAL);
      }
    }
  }

  /**
   * 扫描所有缓存指令，计算每个块需要的缓存副本数量。
   */
  private void rescanCacheDirectives() {
    FSDirectory fsDir = namesystem.getFSDirectory();
    final long now = new Date().getTime();
    for (CacheDirective directive : cacheManager.getCacheDirectives()) {
      scannedDirectives++;
      // 跳过已过期的缓存指令
      if (directive.getExpiryTime() > 0 && directive.getExpiryTime() <= now) {
        LOG.debug("Directive {}: the directive expired at {} (now = {})",
             directive.getId(), directive.getExpiryTime(), now);
        continue;
      }
      String path = directive.getPath();
      INode node;
      try {
        // 解析缓存路径对应的INode
        node = fsDir.getINode(path, DirOp.READ);
      } catch (IOException e) {
        // 路径解析失败，跳过该指令
        LOG.debug("Directive {}: Failed to resolve path {} ({})",
            directive.getId(), path, e.getMessage());
        continue;
      }
      if (node == null)  {
        LOG.debug("Directive {}: No inode found at {}", directive.getId(),
            path);
      } else if (node.isDirectory()) {
        // 处理目录，递归缓存目录下所有文件
        INodeDirectory dir = node.asDirectory();
        ReadOnlyList<INode> children = dir
            .getChildrenList(Snapshot.CURRENT_STATE_ID);
        for (INode child : children) {
          if (child.isFile()) {
            rescanFile(directive, child.asFile());
          }
        }
      } else if (node.isFile()) {
        // 处理单个文件
        rescanFile(directive, node.asFile());
      } else {
        LOG.debug("Directive {}: ignoring non-directive, non-file inode {} ",
            directive.getId(), node);
      }
    }
  }
  
  /**
   * 根据缓存指令处理单个文件，更新文件中所有块的缓存需求。
   * @param directive 缓存指令
   * @param file 需要缓存的文件
   */
  private void rescanFile(CacheDirective directive, INodeFile file) {
    BlockInfo[] blockInfos = file.getBlocks();

    // 更新统计信息：需要缓存的文件数
    directive.addFilesNeeded(1);
    // 计算需要缓存的总字节数，不包含未完成构建的块
    long neededTotal = file.computeFileSizeNotIncludingLastUcBlock() *
        directive.getReplication();
    directive.addBytesNeeded(neededTotal);

    // 检查缓存池配额，如果超过限制则不缓存该文件
    CachePool pool = directive.getPool();
    if (pool.getBytesNeeded() > pool.getLimit()) {
      LOG.debug("Directive {}: not scanning file {} because " +
          "bytesNeeded for pool {} is {}, but the pool's limit is {}",
          directive.getId(),
          file.getFullPathName(),
          pool.getPoolName(),
          pool.getBytesNeeded(),
          pool.getLimit());
      return;
    }

    long cachedTotal = 0;
    for (BlockInfo blockInfo : blockInfos) {
      // 跳过未完成构建的块
      if (!blockInfo.getBlockUCState().equals(BlockUCState.COMPLETE)) {
        LOG.trace("Directive {}: can't cache block {} because it is in state "
                + "{}, not COMPLETE.", directive.getId(), blockInfo,
            blockInfo.getBlockUCState()
        );
        continue;
      }
      Block block = new Block(blockInfo.getBlockId());
      CachedBlock ncblock = new CachedBlock(block.getBlockId(),
          directive.getReplication(), mark);
      CachedBlock ocblock = cachedBlocks.get(ncblock);
      if (ocblock == null) {
        // 新块，添加到已缓存块集合
        cachedBlocks.put(ncblock);
        ocblock = ncblock;
      } else {
        // 已有块，计算当前已缓存字节数
        List<DatanodeDescriptor> cachedOn =
            ocblock.getDatanodes(Type.CACHED);
        long cachedByBlock = Math.min(cachedOn.size(),
            directive.getReplication()) * blockInfo.getNumBytes();
        cachedTotal += cachedByBlock;

        // 在两种情况下更新块的