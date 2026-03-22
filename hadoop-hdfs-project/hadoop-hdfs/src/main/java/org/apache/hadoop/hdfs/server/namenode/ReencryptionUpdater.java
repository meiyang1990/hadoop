// 这个文件已经全部加上中文注释
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension.EncryptedKeyVersion;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.hdfs.protocol.ZoneReencryptionStatus;
import org.apache.hadoop.hdfs.server.namenode.ReencryptionHandler.ReencryptionBatch;
import org.apache.hadoop.hdfs.util.RwLockMode;
import org.apache.hadoop.ipc.RetriableException;
import org.apache.hadoop.util.Lists;
import org.apache.hadoop.util.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Preconditions;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_REENCRYPT_THROTTLE_LIMIT_UPDATER_RATIO_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_REENCRYPT_THROTTLE_LIMIT_UPDATER_RATIO_KEY;

/**
 * 文件重加密EDEK操作收尾处理类，通过使用重加密生成的新EDEK更新文件扩展属性完成重加密流程
 * <p>
 * 任务由{@link ReencryptionHandler}提交
 * <p>
 * 设计假设仅运行一个更新线程，因为更新文件扩展属性需要占用命名空间写锁，多线程性能收益有限
 */
@InterfaceAudience.Private
public final class ReencryptionUpdater implements Runnable {

  public static final Logger LOG =
      LoggerFactory.getLogger(ReencryptionUpdater.class);

  private volatile boolean shouldPauseForTesting = false;
  private volatile int pauseAfterNthCheckpoint = 0;
  private volatile long pauseZoneId = 0;

  private double throttleLimitRatio;
  private final StopWatch throttleTimerAll = new StopWatch();
  private final StopWatch throttleTimerLocked = new StopWatch();

  private volatile long faultRetryInterval = 60000;
  private volatile boolean isRunning = false;

  /**
   * 单个加密区域重加密任务提交追踪器，维护所有已提交任务的Future对象，并跟踪任务处理进度
   */
  static final class ZoneSubmissionTracker {
    private boolean submissionDone;
    private LinkedList<Future> tasks;
    private int numCheckpointed;
    private int numFutureDone;

    ZoneSubmissionTracker() {
      submissionDone = false;
      tasks = new LinkedList<>();
      numCheckpointed = 0;
      numFutureDone = 0;
    }

    void reset() {
      submissionDone = false;
      tasks.clear();
      numCheckpointed = 0;
      numFutureDone = 0;
    }

    LinkedList<Future> getTasks() {
      return tasks;
    }

    void cancelAllTasks() {
      if (!tasks.isEmpty()) {
        LOG.info("Cancelling {} re-encryption tasks", tasks.size());
        for (Future f : tasks) {
          f.cancel(true);
        }
      }
    }

    void addTask(final Future task) {
      tasks.add(task);
    }

    private boolean isCompleted() {
      return submissionDone && tasks.isEmpty();
    }

    void setSubmissionDone() {
      submissionDone = true;
    }
  }

  /**
   * 代表一批重加密命令的任务，维护单批次任务的执行进度统计信息
   */
  static final class ReencryptionTask {
    private final long zoneId;
    private boolean processed = false;
    private int numFilesUpdated = 0;
    private int numFailures = 0;
    private String lastFile = null;
    private final ReencryptionBatch batch;

    ReencryptionTask(final long id, final int failures,
        final ReencryptionBatch theBatch) {
      zoneId = id;
      numFailures = failures;
      batch = theBatch;
    }
  }

  /**
   * 封装单个文件重加密信息类，包含文件inode、原始EDEK和重加密生成的新EDEK
   * <p>
   * 假设对象初始化时已经持有目录锁，初始化过程中inode有效且已加密
   * <p>
   * 重加密过程中可能发生命名空间变更，如果inode已改变则跳过该文件重加密
   */
  static final class FileEdekInfo {
    private final long inodeId;
    private final EncryptedKeyVersion existingEdek;
    private EncryptedKeyVersion edek = null;

    FileEdekInfo(FSDirectory dir, INodeFile inode) throws IOException {
      assert dir.hasReadLock();
      Preconditions.checkNotNull(inode, "INodeFile is null");
      inodeId = inode.getId();
      final FileEncryptionInfo fei = FSDirEncryptionZoneOp
          .getFileEncryptionInfo(dir, INodesInPath.fromINode(inode));
      Preconditions.checkNotNull(fei,
          "FileEncryptionInfo is null for " + inodeId);
      existingEdek = EncryptedKeyVersion
          .createForDecryption(fei.getKeyName(), fei.getEzKeyVersionName(),
              fei.getIV(), fei.getEncryptedDataEncryptionKey());
    }

    long getInodeId() {
      return inodeId;
    }

    EncryptedKeyVersion getExistingEdek() {
      return existingEdek;
    }

    void setEdek(final EncryptedKeyVersion ekv) {
      assert ekv != null;
      edek = ekv;
    }
  }

  @VisibleForTesting
  synchronized void pauseForTesting() {
    shouldPauseForTesting = true;
    LOG.info("Pausing re-encrypt updater for testing.");
    notify();
  }

  @VisibleForTesting
  synchronized void resumeForTesting() {
    shouldPauseForTesting = false;
    LOG.info("Resuming re-encrypt updater for testing.");
    notify();
  }

  @VisibleForTesting
  void pauseForTestingAfterNthCheckpoint(final long zoneId, final int count) {
    assert pauseAfterNthCheckpoint == 0;
    pauseAfterNthCheckpoint = count;
    pauseZoneId = zoneId;
  }

  @VisibleForTesting
  boolean isRunning() {
    return isRunning;
  }

  private final FSDirectory dir;
  private final CompletionService<ReencryptionTask> batchService;
  private final ReencryptionHandler handler;

  /**
   * 构造重加密更新器实例
   * @param fsd 文件系统目录对象
   * @param service 批量任务完成服务
   * @param rh 重加密处理器
   * @param conf Hadoop配置
   */
  ReencryptionUpdater(final FSDirectory fsd,
      final CompletionService<ReencryptionTask> service,
      final ReencryptionHandler rh, final Configuration conf) {
    dir = fsd;
    batchService = service;
    handler = rh;
    this.throttleLimitRatio =
        conf.getDouble(DFS_NAMENODE_REENCRYPT_THROTTLE_LIMIT_UPDATER_RATIO_KEY,
            DFS_NAMENODE_REENCRYPT_THROTTLE_LIMIT_UPDATER_RATIO_DEFAULT);
    Preconditions.checkArgument(throttleLimitRatio > 0.0f,
        DFS_NAMENODE_REENCRYPT_THROTTLE_LIMIT_UPDATER_RATIO_KEY
            + " is not positive.");
  }

  /**
   * 由提交线程调用，标记指定加密区域所有任务已提交完成
   * 如果调用时没有任务已提交，则直接认为该区域重加密完成
   *
   * @param zoneId 加密区域inode ID
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  void markZoneSubmissionDone(final long zoneId)
      throws IOException, InterruptedException {
    final ZoneSubmissionTracker tracker = handler.getTracker(zoneId);
    if (tracker != null && !tracker.getTasks().isEmpty()) {
      tracker.submissionDone = true;
    } else {
      // Caller thinks submission is done, but no tasks submitted - meaning
      // no files in the EZ need to be re-encrypted. Complete directly.
      handler.addDummyTracker(zoneId, tracker);
    }
  }

  @Override
  public void run() {
    isRunning = true;
    throttleTimerAll.start();
    while (true) {
      try {
        // 获取并处理已完成的重加密任务
        takeAndProcessTasks();
      } catch (InterruptedException ie) {
        LOG.warn("Re-encryption updater thread interrupted. Exiting.");
        Thread.currentThread().interrupt();
        isRunning = false;
        return;
      } catch (IOException | CancellationException e) {
        LOG.warn("Re-encryption updater thread exception.", e);
      } catch (Throwable t) {
        LOG.error("Re-encryption updater thread exiting.", t);
        isRunning = false;
        return;
      }
    }
  }

  /**
   * 处理单个已完成的重加密任务，将每个inode ID解析为INode对象，如果inode已删除则跳过更新
   * <p>
   * 本方法仅更新文件扩展属性，不更新重加密进度
   *
   * @param zoneNodePath 加密区域inode完整路径
   * @param task 已完成的重加密任务
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  private void processTaskEntries(final String zoneNodePath,
      final ReencryptionTask task) throws IOException, InterruptedException {
    assert dir.hasWriteLock();
    if (!task.batch.isEmpty() && task.numFailures == 0) {
      LOG.debug(
          "Updating file xattrs for re-encrypting zone {}," + " starting at {}",
          zoneNodePath, task.batch.getFirstFilePath());
      final int batchSize = task.batch.size();
      // 遍历当前批次中所有文件重加密信息
      for (Iterator<FileEdekInfo> it = task.batch.getBatch().iterator();
           it.hasNext();) {
        FileEdekInfo entry = it.next();
        // 重新解析inode，inode不存在则跳过
        LOG.trace("Updating {} for re-encryption.", entry.getInodeId());
        final INode inode = dir.getInode(entry.getInodeId());
        if (inode == null) {
          LOG.debug("INode {} doesn't exist, skipping re-encrypt.",
              entry.getInodeId());
          // 从批次中移除，避免后续保存
          it.remove();
          continue;
        }

        // 谨慎检查文件加密信息，只有确认仍使用原有EDEK才执行更新
        Preconditions.checkNotNull(entry.edek);
        final FileEncryptionInfo fei = FSDirEncryptionZoneOp
            .getFileEncryptionInfo(dir, INodesInPath.fromINode(inode));
        // 加密区域密钥名称已变更，跳过更新
        if (!fei.getKeyName().equals(entry.edek.getEncryptionKeyName())) {
          LOG.debug("Inode {} EZ key changed, skipping re-encryption.",
              entry.getInodeId());
          it.remove();
          continue;
        }
        // 加密区域密钥版本未变更，无需重加密
        if (fei.getEzKeyVersionName()
            .equals(entry.edek.getEncryptionKeyVersionName())) {
          LOG.debug(
              "Inode {} EZ key version unchanged, skipping re-encryption.",
              entry.getInodeId());
          it.remove();
          continue;
        }
        // 已有EDEK已变更，跳过更新避免覆盖
        if (!Arrays.equals(fei.getEncryptedDataEncryptionKey(),
            entry.existingEdek.getEncryptedKeyVersion().getMaterial())) {
          LOG.debug("Inode {} existing edek changed, skipping re-encryption",
              entry.getInodeId());
          it.remove();
          continue;
        }
        // 构造新的文件加密信息
        FileEncryptionInfo newFei = new FileEncryptionInfo(fei.getCipherSuite(),
            fei.getCryptoProtocolVersion(),
            entry.edek.getEncryptedKeyVersion().getMaterial(),
            entry.edek.getEncryptedKeyIv(), fei.getKeyName(),
            entry.edek.getEncryptionKeyVersionName());
        final INodesInPath iip = INodesInPath.fromINode(inode);
        // 更新文件加密信息扩展属性
        FSDirEncryptionZoneOp
            .setFileEncryptionInfo(dir, iip, newFei, XAttrSetFlag.REPLACE);
        task.lastFile = iip.getPath();
        ++task.numFilesUpdated;
      }

      LOG.info("Updated xattrs on {}({}) files in zone {} for re-encryption,"
              + " starting:{}.", task.numFilesUpdated, batchSize,
          zoneNodePath, task.batch.getFirstFilePath());
    }
    task.processed = true;
  }

  /**
   * 遍历指定加密区域的任务，更新重加密检查点进度
   * 检查点表示其之前所有文件都已完成重加密，方法会将检查点更新到所有前置任务都已完成的位置
   *
   * @param zoneNode 加密区域inode
   * @param tracker 加密区域任务提交追踪器
   * @return 包含最后一次检查点扩展属性的列表，如果没有检查点则返回空列表
   * @throws ExecutionException 执行异常
   * @throws IOException IO异常
   * @throws InterruptedException 中断异常
   */
  private List<XAttr> processCheckpoints(final INode zoneNode,
      final ZoneSubmissionTracker tracker)
      throws ExecutionException, IOException, InterruptedException {
    assert dir.hasWriteLock();
    final long zoneId = zoneNode.getId();
    final String zonePath = zoneNode.getFullPathName();
    final ZoneReencryptionStatus status =
        handler.getReencryptionStatus().getZoneStatus(zoneId);
    assert status != null;
    // 始终从头开始遍历，因为检查点代表其之前所有文件都已完成重加密
    final LinkedList<Future> tasks = tracker.getTasks();
    final List<XAttr> xAttrs = Lists.newArrayListWithCapacity(1);
    ListIterator<Future> iter = tasks.listIterator();
    synchronized (handler) {
      while (iter.hasNext()) {
        Future<ReencryptionTask> curr = iter.next();
        if (curr.isCancelled()) {
          break;
        }
        if (!curr.isDone() || !curr.get().processed) {
          // 仍有更早任务未完成，终止遍历
          break;
        }
        ReencryptionTask task = curr.get();
        LOG.debug("Updating re-encryption checkpoint with completed task."
            + " last: {} size:{}.", task.lastFile, task.batch.size());
        assert zoneId == task.zoneId;
        try {
          // 更新加密区域重加密进度扩展属性
          final XAttr xattr = FSDirEncryptionZoneOp
              .updateReencryptionProgress(dir, zoneNode, status, task.lastFile,
                  task.numFilesUpdated, task.numFailures);
          xAttrs.clear();
          xAttrs.add(xattr);
        } catch (IOException ie) {
          LOG.warn("Failed to update re-encrypted progress to xattr" +
                  " for zone {}", zonePath, ie);
          ++task.numFailures;
        }
        ++tracker.numCheckpointed;
        iter.remove();
      }
    }
    if (tracker.isCompleted()) {
      // 所有任务完成，移除追踪器并完成重加密流程
      LOG.debug("Removed re-encryption tracker for zone {} because it completed"
              + " with {} tasks.", zonePath, tracker.numCheckpointed);
      return handler.completeReencryption(zoneNode);
    }
    return xAttrs;
  }

  /**
   * 获取并处理已完成的重加密任务
   * @throws Exception 各类异常
   */
  private void takeAndProcessTasks() throws Exception {
    // 从完成服务获取下一个已完成任务
    final Future<ReencryptionTask> completed = batchService.take();
    // 执行限流控制，避免占用过多写锁
    throttle();
    // 检查测试暂停条件
    checkPauseForTesting();
    if (completed.isCancelled()) {
      // 忽略已取消任务，取消操作已由处理器记录