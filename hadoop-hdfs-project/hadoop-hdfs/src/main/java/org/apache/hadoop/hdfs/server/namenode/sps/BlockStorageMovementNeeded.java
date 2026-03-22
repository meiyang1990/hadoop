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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.util.Daemon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;

/**
 * 文件路径：hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/sps/BlockStorageMovementNeeded.java
 * <p>
 * 存储策略满足器(SPS)中，跟踪需要进行块存储移动的块集合ID（Inode ID）的核心管理器。
 * 根据命名空间信息和DataNode存储报告，扫描待处理目录，收集需要移动的块集合，
 * 跟踪处理进度，移动完成后清除目录/文件上的SPS扩展属性。
 */
@InterfaceAudience.Private
public class BlockStorageMovementNeeded {

  public static final Logger LOG =
      LoggerFactory.getLogger(BlockStorageMovementNeeded.class);

  // 待执行存储移动任务的队列
  private final Queue<ItemInfo> storageMovementNeeded =
      new LinkedList<ItemInfo>();

  /**
   * 目录待处理工作统计：key为起始目录INode ID，value为该目录下待处理文件数量统计信息。
   * 待处理数量表示目录下还有多少个文件需要完成存储策略满足。
   */
  private final Map<Long, DirPendingWorkInfo> pendingWorkForDirectory =
      new HashMap<>();

  // SPS上下文对象，提供扫描、移除SPS标记等核心操作
  private final Context ctxt;

  // 路径ID收集后台线程
  private Daemon pathIdCollector;

  // 路径ID处理任务实例
  private SPSPathIdProcessor pathIDProcessor;

  // 路径成功状态缓存过期时间，过期后转为NOT_AVAILABLE状态，单位毫秒
  private static long statusClearanceElapsedTimeMs = 300000;

  /**
   * 构造方法，初始化块存储移动任务管理器。
   * @param context SPS上下文对象，提供核心操作接口
   */
  public BlockStorageMovementNeeded(Context context) {
    this.ctxt = context;
    pathIDProcessor = new SPSPathIdProcessor();
  }

  /**
   * 添加单个需要存储移动的任务到跟踪队列。
   * @param trackInfo 待满足存储策略的任务信息
   */
  public synchronized void add(ItemInfo trackInfo) {
    if (trackInfo != null) {
      storageMovementNeeded.add(trackInfo);
    }
  }

  /**
   * 批量添加目录下的多个待处理任务到跟踪队列，更新目录扫描统计。
   * @param startPath 起始目录INode ID
   * @param itemInfoList 目录下待处理任务列表
   * @param scanCompleted 标记该目录是否已经扫描完成
   */
  @VisibleForTesting
  public synchronized void addAll(long startPath, List<ItemInfo> itemInfoList,
      boolean scanCompleted) {
    storageMovementNeeded.addAll(itemInfoList);
    updatePendingDirScanStats(startPath, itemInfoList.size(), scanCompleted);
  }

  /**
   * 添加单个待处理任务，并更新对应起始目录的扫描统计。
   * @param itemInfo 待处理任务信息
   * @param scanCompleted 标记对应目录是否已经扫描完成
   */
  @VisibleForTesting
  public synchronized void add(ItemInfo itemInfo, boolean scanCompleted) {
    storageMovementNeeded.add(itemInfo);
    // 起始路径就是当前文件本身，无需更新目录扫描统计
    if (itemInfo.getStartPath() == itemInfo.getFile()) {
      return;
    }
    updatePendingDirScanStats(itemInfo.getStartPath(), 1, scanCompleted);
  }

  /**
   * 更新指定目录的扫描统计信息，增加待处理文件数量，标记扫描完成状态。
   * @param startPath 起始目录INode ID
   * @param numScannedFiles 本次新增扫描到的文件数量
   * @param scanCompleted 目录是否已经扫描完成
   */
  private void updatePendingDirScanStats(long startPath, int numScannedFiles,
      boolean scanCompleted) {
    DirPendingWorkInfo pendingWork = pendingWorkForDirectory.get(startPath);
    if (pendingWork == null) {
      pendingWork = new DirPendingWorkInfo();
      pendingWorkForDirectory.put(startPath, pendingWork);
    }
    pendingWork.addPendingWorkCount(numScannedFiles);
    if (scanCompleted) {
      pendingWork.markScanCompleted();
    }
  }

  /**
   * 从待处理队列取出一个需要块存储移动检查的任务。
   * @return 待处理任务信息
   */
  public synchronized ItemInfo get() {
    return storageMovementNeeded.poll();
  }

  /**
   * 获取待处理任务队列的长度。
   * @return 队列中待处理任务数量
   */
  public synchronized int size() {
    return storageMovementNeeded.size();
  }

  /**
   * 清空所有待处理任务和目录统计信息。
   */
  public synchronized void clearAll() {
    storageMovementNeeded.clear();
    pendingWorkForDirectory.clear();
  }

  /**
   * 任务完成后，减少对应目录的待处理计数，计数清零时移除SPS扩展属性。
   * @param trackInfo 已完成的任务信息
   * @param isSuccess 移动是否成功
   * @throws IOException 移除扩展属性时可能抛出IO异常
   */
  public synchronized void removeItemTrackInfo(ItemInfo trackInfo,
      boolean isSuccess) throws IOException {
    if (trackInfo.isDir()) {
      // 如果任务属于某个起始INode目录，减少目录的待处理计数
      long startId = trackInfo.getStartPath();
      if (!ctxt.isFileExist(startId)) {
        // 目录已被删除，直接移除统计信息
        this.pendingWorkForDirectory.remove(startId);
      } else {
        DirPendingWorkInfo pendingWork = pendingWorkForDirectory.get(startId);
        if (pendingWork != null) {
          pendingWork.decrementPendingWorkCount();
          if (pendingWork.isDirWorkDone()) {
            // 目录所有任务完成，移除SPS标记
            ctxt.removeSPSHint(startId);
            pendingWorkForDirectory.remove(startId);
          }
        }
      }
    } else {
      // 文件任务，直接移除SPS扩展属性
      ctxt.removeSPSHint(trackInfo.getFile());
    }
  }

  /**
   * 清空所有待处理队列中的任务，移除所有任务的SPS扩展属性，通知清理资源。
   */
  public synchronized void clearQueuesWithNotification() {
    // 移除待扫描目录的SPS扩展属性
    Long trackId;
    while ((trackId = ctxt.getNextSPSPath()) != null) {
      try {
        ctxt.removeSPSHint(trackId);
      } catch (IOException ie) {
        LOG.warn("Failed to remove SPS xattr for track id " + trackId, ie);
      }
    }

    // 移除待处理文件任务的SPS扩展属性
    ItemInfo itemInfo;
    while ((itemInfo = get()) != null) {
      try {
        if (!itemInfo.isDir()) {
          ctxt.removeSPSHint(itemInfo.getFile());
        }
      } catch (IOException ie) {
        LOG.warn(
            "Failed to remove SPS xattr for track id "
                + itemInfo.getFile(), ie);
      }
    }
    this.clearAll();
  }

  /**
   * 后台路径ID处理线程，从待扫描目录队列取出目录，递归收集目录下所有文件ID，交给存储移动任务处理。
   */
  private class SPSPathIdProcessor implements Runnable {
    private static final int MAX_RETRY_COUNT = 3;

    @Override
    public void run() {
      LOG.info("Starting SPSPathIdProcessor!.");
      Long startINode = null;
      int retryCount = 0;
      while (ctxt.isRunning()) {
        try {
          // 安全模式下不进行扫描
          if (!ctxt.isInSafeMode()) {
            if (startINode == null) {
              retryCount = 0;
              // 取出下一个待扫描目录
              startINode = ctxt.getNextSPSPath();
            } // else 重试当前处理失败的INode
            if (startINode == null) {
              // 没有待处理路径，等待3秒后重试
              Thread.sleep(3000);
            } else {
              // 扫描目录，收集所有需要处理的文件
              ctxt.scanAndCollectFiles(startINode);
              // 检查目录是否已经扫描完成且所有任务处理完毕
              DirPendingWorkInfo dirPendingWorkInfo =
                  pendingWorkForDirectory.get(startINode);
              if (dirPendingWorkInfo != null
                  && dirPendingWorkInfo.isDirWorkDone()) {
                try {
                  // 移除目录上的SPS扩展属性
                  ctxt.removeSPSHint(startINode);
                } catch (FileNotFoundException e) {
                  // 文件不存在则忽略
                  startINode = null;
                }
                pendingWorkForDirectory.remove(startINode);
              }
            }
            startINode = null; // 当前INode扫描完成
          }
        } catch (Throwable t) {
          String reClass = t.getClass().getName();
          if (InterruptedException.class.getName().equals(reClass)) {
            LOG.info("SPSPathIdProcessor thread is interrupted. Stopping..");
            break;
          }
          LOG.warn("Exception while scanning file inodes to satisfy the policy",
              t);
          try {
            Thread.sleep(3000);
          } catch (InterruptedException e) {
            LOG.info("Interrupted while waiting in SPSPathIdProcessor", t);
            break;
          }
          retryCount++;
          if (retryCount >= MAX_RETRY_COUNT) {
            // 重试次数超过上限，跳过当前INode
            LOG.warn("Skipping this inode {} due to too many retries.", startINode);
            startINode = null;
          }
        }
      }
    }
  }

  /**
   * 目录递归扫描的待处理工作统计信息，记录待完成任务数量和扫描完成状态。
   */
  public static class DirPendingWorkInfo {

    private int pendingWorkCount = 0;
    private boolean fullyScanned = false;

    /**
     * 增加目录待处理任务计数。
     * @param count 新增的待处理任务数量
     */
    public synchronized void addPendingWorkCount(int count) {
      this.pendingWorkCount = this.pendingWorkCount + count;
    }

    /**
     * 完成一个任务，减少目录待处理任务计数。
     */
    public synchronized void decrementPendingWorkCount() {
      this.pendingWorkCount--;
    }

    /**
     * 检查目录所有任务是否已经完成：扫描已完成且待处理计数清零。
     * @return  true表示目录所有工作已完成，false表示还有待处理任务
     */
    public synchronized boolean isDirWorkDone() {
      return (pendingWorkCount <= 0 && fullyScanned);
    }

    /**
     * 标记目录已经扫描完成，不会再新增待处理任务。
     */
    public synchronized void markScanCompleted() {
      this.fullyScanned = true;
    }
  }

  /**
   * 激活后台路径扫描处理线程，开始处理待移动任务。
   */
  public void activate() {
    pathIdCollector = new Daemon(pathIDProcessor);
    pathIdCollector.setName("SPSPathIdProcessor");
    pathIdCollector.start();
  }

  /**
   * 关闭后台扫描处理线程，停止处理任务。
   */
  public void close() {
    if (pathIdCollector != null) {
      pathIdCollector.interrupt();
    }
  }

  /**
   * 设置状态缓存过期时间，仅用于单元测试。
   * @param statusClearanceElapsedTimeMs 过期时间，单位毫秒
   */
  @VisibleForTesting
  public static void setStatusClearanceElapsedTimeMs(
      long statusClearanceElapsedTimeMs) {
    BlockStorageMovementNeeded.statusClearanceElapsedTimeMs =
        statusClearanceElapsedTimeMs;
  }

  /**
   * 获取状态缓存过期时间，仅用于单元测试。
   * @return 过期时间，单位毫秒
   */
  @VisibleForTesting
  public static long getStatusClearanceElapsedTimeMs() {
    return statusClearanceElapsedTimeMs;
  }

  /**
   * 标记指定INode对应的目录扫描已完成。
   * @param inode 目录INode ID
   */
  public void markScanCompletedForDir(long inode) {
    DirPendingWorkInfo pendingWork = pendingWorkForDirectory.get(inode);
    if (pendingWork != null) {
      pendingWork.markScanCompleted();
    }
  }
}