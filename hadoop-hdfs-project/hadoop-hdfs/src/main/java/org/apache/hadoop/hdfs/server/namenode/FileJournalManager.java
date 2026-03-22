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
package org.apache.hadoop.hdfs.server.namenode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Comparator;
import java.util.Collections;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NodeType;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.common.StorageErrorReporter;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.namenode.NNStorageRetentionManager.StoragePurger;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogLoader.EditLogValidation;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeFile;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLog;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.util.Lists;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.collect.ComparisonChain;

/**
 * 文件系统日志管理器，负责将NameNode编辑日志写入本地存储目录，提供日志管理、清理、恢复功能
 * 实现基于文件存储的JournalManager接口，支持HDFS元数据变更日志的持久化存储
 *
 * Note: this class is not thread-safe and should be externally
 * synchronized.
 */
@InterfaceAudience.Private
public class FileJournalManager implements JournalManager {
  private static final Logger LOG =
      LoggerFactory.getLogger(FileJournalManager.class);

  private final Configuration conf;
  private final StorageDirectory sd;
  private final StorageErrorReporter errorReporter;
  private int outputBufferCapacity = 512*1024;

  // 已 finalized 的编辑日志文件名正则表达式：edits_start-end
  private static final Pattern EDITS_REGEX = Pattern.compile(
    NameNodeFile.EDITS.getName() + "_(\\d+)-(\\d+)");
  // 进行中的编辑日志文件名正则表达式：edits_inprogress_start
  private static final Pattern EDITS_INPROGRESS_REGEX = Pattern.compile(
    NameNodeFile.EDITS_INPROGRESS.getName() + "_(\\d+)");
  // 过时的进行中编辑日志文件名正则表达式（带后缀标记）
  private static final Pattern EDITS_INPROGRESS_STALE_REGEX = Pattern.compile(
      NameNodeFile.EDITS_INPROGRESS.getName() + "_(\\d+).*(\\S+)");

  @VisibleForTesting
  File currentInProgress = null;

  /**
   * A FileJournalManager should maintain the largest Tx ID that has been
   * safely written to its edit log files.
   * It should limit readers to read beyond this ID to avoid potential race
   * with ongoing writers.
   * Initial value indicates that all transactions can be read.
   */
  // 当前所有已安全写入日志的最大事务ID，限制读取范围避免和写入竞态
  private long lastReadableTxId = Long.MAX_VALUE;

  @VisibleForTesting
  StoragePurger
    = new NNStorageRetentionManager.DeletionStoragePurger();

  /**
   * 构造文件日志管理器，绑定指定的存储目录
   * @param conf Hadoop配置对象
   * @param sd 存储目录对象
   * @param errorReporter 存储错误报告器
   */
  public FileJournalManager(Configuration conf, StorageDirectory sd,
      StorageErrorReporter errorReporter) {
    this.conf = conf;
    this.sd = sd;
    this.errorReporter = errorReporter;
  }

  @Override 
  public void close() throws IOException {}
  
  @Override
  public void format(NamespaceInfo ns, boolean force) throws IOException {
    // Formatting file journals is done by the StorageDirectory
    // format code, since they may share their directory with
    // checkpoints, etc.
    throw new UnsupportedOperationException();
  }
  
  @Override
  public boolean hasSomeData() {
    // Formatting file journals is done by the StorageDirectory
    // format code, since they may share their directory with
    // checkpoints, etc.
    throw new UnsupportedOperationException();
  }

  /**
   * 启动一个新的编辑日志段，用于写入从指定事务ID开始的日志
   * @param txid 新日志段起始事务ID
   * @param layoutVersion HDFS存储布局版本
   * @return 编辑日志输出流
   * @throws IOException 启动日志段失败时抛出
   */
  @Override
  synchronized public EditLogOutputStream startLogSegment(long txid,
      int layoutVersion) throws IOException {
    try {
      // 获取进行中的编辑日志文件对象
      currentInProgress = NNStorage.getInProgressEditsFile(sd, txid);
      // 创建文件输出流并初始化
      EditLogOutputStream stm = new EditLogFileOutputStream(conf,
          currentInProgress, outputBufferCapacity);
      stm.create(layoutVersion);
      return stm;
    } catch (IOException e) {
      LOG.warn("Unable to start log segment " + txid +
          " at " + currentInProgress + ": " +
          e.getLocalizedMessage());
      // 上报文件错误
      errorReporter.reportErrorOnFile(currentInProgress);
      throw e;
    }
  }

  /**
   * 完成指定日志段，将进行中的文件重命名为已 finalized 文件
   * @param firstTxId 日志段起始事务ID
   * @param lastTxId 日志段结束事务ID
   * @throws IOException 完成日志段失败时抛出
   */
  @Override
  synchronized public void finalizeLogSegment(long firstTxId, long lastTxId)
      throws IOException {
    File inprogressFile = NNStorage.getInProgressEditsFile(sd, firstTxId);

    // 生成最终化后的目标文件名
    File dstFile = NNStorage.getFinalizedEditsFile(
        sd, firstTxId, lastTxId);
    LOG.info("Finalizing edits file " + inprogressFile + " -> " + dstFile);
    
    // 检查目标文件不存在
    Preconditions.checkState(!dstFile.exists(),
        "Can't finalize edits file " + inprogressFile + " since finalized file " +
        "already exists");

    try {
      // 重命名文件完成最终化
      NativeIO.renameTo(inprogressFile, dstFile);
    } catch (IOException e) {
      errorReporter.reportErrorOnFile(dstFile);
      throw new IllegalStateException("Unable to finalize edits file " + inprogressFile, e);
    }

    // 清空当前进行中引用
    if (inprogressFile.equals(currentInProgress)) {
      currentInProgress = null;
    }
  }

  @VisibleForTesting
  public StorageDirectory getStorageDirectory() {
    return sd;
  }

  /**
   * 设置日志输出缓冲区容量
   * @param size 缓冲区容量大小（字节）
   */
  @Override
  synchronized public void setOutputBufferCapacity(int size) {
    this.outputBufferCapacity = size;
  }


  public long getLastReadableTxId() {
    return lastReadableTxId;
  }

  public void setLastReadableTxId(long id) {
    this.lastReadableTxId = id;
  }

  /**
   * Purges the unnecessary edits and edits_inprogress files.
   *
   * Edits files that are ending before the minTxIdToKeep are purged.
   * Edits in progress files that are starting before minTxIdToKeep are purged.
   * Edits in progress files that are marked as empty, trash, corrupted or
   * stale by file extension and starting before minTxIdToKeep are purged.
   * Edits in progress files that are after minTxIdToKeep, but before the
   * current edits in progress files are marked as stale for clarity.
   *
   * In case file removal or rename is failing a warning is logged, but that
   * does not fail the operation.
   *
   * @param minTxIdToKeep the lowest transaction ID that should be retained
   * @throws IOException if listing the storage directory fails.
   */
  /**
   * 清理早于指定最小保留事务ID的旧日志文件
   * @param minTxIdToKeep 需要保留的最小事务ID，早于该ID的日志会被清理
   * @throws IOException 列出存储目录文件失败时抛出
   */
  @Override
  public void purgeLogsOlderThan(long minTxIdToKeep)
      throws IOException {
    LOG.info("Purging logs older than " + minTxIdToKeep);
    // 列出当前目录所有文件
    File[] files = FileUtil.listFiles(sd.getCurrentDir());
    // 匹配所有编辑日志文件
    List<EditLogFile> editLogs = matchEditLogs(files, true);
    synchronized (this) {
      // 遍历所有日志，判断是否需要清理或标记过期
      for (EditLogFile log : editLogs) {
        if (log.getFirstTxId() < minTxIdToKeep &&
            log.getLastTxId() < minTxIdToKeep) {
          // 日志完全早于保留阈值，清理
          purger.purgeLog(log);
        } else if (isStaleInProgressLog(minTxIdToKeep, log)) {
          // 非当前进行中的旧进行中日志，标记为过期
          purger.markStale(log);
        }
      }
    }
  }

  /**
   * 判断给定日志是否为过期的进行中日志
   * @param minTxIdToKeep 最小保留事务ID
   * @param log 待判断的日志文件对象
   * @return 是否为过期进行中日志
   */
  private boolean isStaleInProgressLog(long minTxIdToKeep, EditLogFile log) {
    return log.isInProgress() &&
        !log.getFile().equals(currentInProgress) &&
        log.getFirstTxId() >= minTxIdToKeep &&
        // 仅对未标记过的文件处理，已经是过期/垃圾/损坏的文件会被直接清理
        EDITS_INPROGRESS_REGEX.matcher(log.getFile().getName()).matches();
  }

  /**
   * Find all editlog segments starting at or above the given txid.
   * @param firstTxId the txnid which to start looking
   * @param inProgressOk whether or not to include the in-progress edit log 
   *        segment       
   * @return a list of remote edit logs
   * @throws IOException if edit logs cannot be listed.
   */
  /**
   * 获取起始事务ID大于等于指定值的所有远程编辑日志
   * @param firstTxId 起始查找事务ID
   * @param inProgressOk 是否包含进行中的日志段
   * @return 符合条件的远程编辑日志列表
   * @throws IOException 列出日志文件失败时抛出
   */
  public List<RemoteEditLog> getRemoteEditLogs(long firstTxId,
      boolean inProgressOk) throws IOException {
    File currentDir = sd.getCurrentDir();
    List<EditLogFile> allLogFiles = matchEditLogs(currentDir);
    List<RemoteEditLog> ret = Lists.newArrayListWithCapacity(
        allLogFiles.size());
    // 遍历所有日志文件，筛选符合条件的日志
    for (EditLogFile elf : allLogFiles) {
      if (elf.hasCorruptHeader() || (!inProgressOk && elf.isInProgress())) {
        // 跳过损坏头文件或不包含进行中日志时跳过进行中文件
        continue;
      }
      if (elf.isInProgress()) {
        try {
          // 扫描进行中日志验证头信息，获取实际结束事务ID
          elf.scanLog(getLastReadableTxId(), true);
        } catch (IOException e) {
          LOG.error("got IOException while trying to validate header of " +
              elf + ".  Skipping.", e);
          continue;
        }
      }
      if (elf.getFirstTxId() >= firstTxId) {
        // 起始ID满足要求，添加结果
        ret.add(new RemoteEditLog(elf.firstTxId, elf.lastTxId,
            elf.isInProgress()));
      } else if (elf.getFirstTxId() < firstTxId && firstTxId <= elf.getLastTxId()) {
        // firstTxId落在当前日志段中间，仍然返回由调用方处理
        ret.add(new RemoteEditLog(elf.firstTxId, elf.lastTxId,
            elf.isInProgress()));
      }
    }
    
    // 按事务ID排序返回
    Collections.sort(ret);
    
    return ret;
  }
  
  /**
   * Discard all editlog segments whose first txid is greater than or equal to
   * the given txid, by renaming them with suffix ".trash".
   */
  /**
   * 丢弃所有起始事务ID大于等于指定值的日志段，通过重命名为.trash后缀实现
   * @param startTxId 起始丢弃事务ID
   * @throws IOException 列出文件或重命名失败时抛出
   */
  private void discardEditLogSegments(long startTxId) throws IOException {
    File currentDir = sd.getCurrentDir();
    List<EditLogFile> allLogFiles = matchEditLogs(currentDir);
    List<EditLogFile> toTrash = Lists.newArrayList();
    LOG.info("Discard the EditLog files, the given start txid is " + startTxId);
    // 遍历日志，收集需要丢弃的日志段
    for (EditLogFile elf : allLogFiles) {
      if (elf.getFirstTxId() >= startTxId) {
        toTrash.add(elf);
      } else {
        // 保证所有起始ID小于startTxId的日志，结束ID也一定小于startTxId，即事务ID边界对齐
        Preconditions.checkState(elf.getLastTxId() < startTxId);
      }
    }

    // 将所有需要丢弃的日志重命名为.trash后缀
    for (EditLogFile elf : toTrash) {
      elf.moveAsideTrashFile(startTxId);
      LOG.info("Trash the EditLog file " + elf);
    }
  }

  /**
   * returns matching edit logs via the log directory. Simple helper function
   * that lists the files in the logDir and calls matchEditLogs(File[])
   * 
   * @param logDir
   *          directory to match edit logs in
   * @return matched edit logs
   * @throws IOException
   *           IOException thrown for invalid logDir
   */
  public static List<EditLogFile> matchEditLogs(File logDir) throws IOException {
    return matchEditLogs(FileUtil.listFiles(logDir));
  }

  static List<EditLogFile> matchEditLogs(File[] filesInStorage) {
    return matchEditLogs(filesInStorage, false);
  }

  /**
   * 匹配文件数组中所有符合编辑日志命名规则的文件，转换为EditLogFile对象
   * @param filesInStorage 待匹配的文件数组
   * @param forPurging 是否用于清理，为true时额外匹配已标记过期的进行中日志
   * @return 匹配到的编辑日志对象列表
   */
  private static List<EditLogFile> matchEditLogs(File[] filesInStorage,
      boolean forPurging) {
    List<EditLogFile> ret = Lists.newArrayList();
    // 遍历所有文件，匹配不同类型日志
    for (File f : filesInStorage) {
      String name = f.getName();
      // 匹配已 finalized 日志
      Matcher editsMatch = EDITS_REGEX.matcher(name);
      if (editsMatch.matches()) {
        try {
          long startTxId = Long.parseLong(editsMatch.group(1));
          long endTxId = Long.parseLong(editsMatch.group(2));
          ret.add(new EditLogFile(f, startTxId, endTxId));
          continue;
        } catch (NumberFormatException nfe) {
          LOG.error("Edits file " + f + " has improperly formatted " +
                    "transaction ID");
          // skip
        }
      }
      
      // 匹配正常命名的进行中日志
      Matcher inProgressEditsMatch = EDITS_INPROGRESS_REGEX.matcher(name);
      if (inProgressEditsMatch.matches()) {
        try {
          long startTxId = Long.parseLong(inProgressEditsMatch.group(1));
          ret.add(
              new EditLogFile(f, startTxId, HdfsServerConstants.IN