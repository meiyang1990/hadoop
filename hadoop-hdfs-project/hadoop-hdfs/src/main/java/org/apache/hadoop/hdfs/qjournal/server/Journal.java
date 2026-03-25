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
package org.apache.hadoop.hdfs.qjournal.server;

import org.apache.hadoop.thirdparty.protobuf.ByteString;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.Range;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.qjournal.protocol.JournalNotFormattedException;
import org.apache.hadoop.hdfs.qjournal.protocol.JournalOutOfSyncException;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocol;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetJournaledEditsResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.NewEpochResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.PersistedRecoveryPaxosData;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.PrepareRecoveryResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.SegmentStateProto;
import org.apache.hadoop.hdfs.qjournal.protocol.RequestInfo;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.StorageErrorReporter;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.namenode.EditLogOutputStream;
import org.apache.hadoop.hdfs.server.namenode.FileJournalManager;
import org.apache.hadoop.hdfs.server.namenode.FileJournalManager.EditLogFile;
import org.apache.hadoop.hdfs.server.namenode.JournalManager;
import org.apache.hadoop.hdfs.server.namenode.TransferFsImage;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLog;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;
import org.apache.hadoop.hdfs.util.AtomicFileOutputStream;
import org.apache.hadoop.hdfs.util.BestEffortLongFile;
import org.apache.hadoop.hdfs.util.PersistentLongFile;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StopWatch;
import org.apache.hadoop.util.Time;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.hadoop.thirdparty.protobuf.TextFormat;

/**
 * 文件说明：QJournal JournalNode模块的Journal实体类，对应一个命名空间的日志管理
 * 核心职责：维护单个命名空间的edits日志存储，处理日志写入、分段、恢复等操作，支持HA场景下的Paxos共识日志同步
 */
/**
 * A JournalNode can manage journals for several clusters at once.
 * Each such journal is entirely independent despite being hosted by
 * the same JVM.
 */
public class Journal implements Closeable {
  static final Logger LOG = LoggerFactory.getLogger(Journal.class);


  // Current writing state
  private EditLogOutputStream curSegment;
  private long curSegmentTxId = HdfsServerConstants.INVALID_TXID;
  private int curSegmentLayoutVersion = 0;
  private long nextTxId = HdfsServerConstants.INVALID_TXID;
  private long highestWrittenTxId = 0;
  
  private final String journalId;
  
  private final JNStorage storage;

  /**
   * When a new writer comes along, it asks each node to promise
   * to ignore requests from any previous writer, as identified
   * by epoch number. In order to make such a promise, the epoch
   * number of that writer is stored persistently on disk.
   */
  private PersistentLongFile lastPromisedEpoch;

  /**
   * Each IPC that comes from a given client contains a serial number
   * which only increases from the client's perspective. Whenever
   * we switch epochs, we reset this back to -1. Whenever an IPC
   * comes from a client, we ensure that it is strictly higher
   * than any previous IPC. This guards against any bugs in the IPC
   * layer that would re-order IPCs or cause a stale retry from an old
   * request to resurface and confuse things.
   */
  private long currentEpochIpcSerial = -1;
  
  /**
   * The epoch number of the last writer to actually write a transaction.
   * This is used to differentiate log segments after a crash at the very
   * beginning of a segment. See the the 'testNewerVersionOfSegmentWins'
   * test case.
   */
  private PersistentLongFile lastWriterEpoch;
  
  /**
   * Lower-bound on the last committed transaction ID. This is not
   * depended upon for correctness, but acts as a sanity check
   * during the recovery procedures, and as a visibility mark
   * for clients reading in-progress logs.
   */
  private BestEffortLongFile committedTxnId;
  
  public static final String LAST_PROMISED_FILENAME = "last-promised-epoch";
  public static final String LAST_WRITER_EPOCH = "last-writer-epoch";
  private static final String COMMITTED_TXID_FILENAME = "committed-txid";
  
  private final FileJournalManager fjm;

  private JournaledEditsCache cache;

  private final JournalMetrics metrics;

  private long lastJournalTimestamp = 0;

  private Configuration conf = null;

  // This variable tracks, have we tried to start journalsyncer
  // with nameServiceId. This will help not to start the journalsyncer
  // on each rpc call, if it has failed to start
  private boolean triedJournalSyncerStartedwithnsId = false;

  /**
   * Time threshold for sync calls, beyond which a warning should be logged to the console.
   */
  private static final int WARN_SYNC_MILLIS_THRESHOLD = 1000;

  /**
   * 构造Journal实例，初始化存储、日志管理器、缓存和指标，并扫描已有日志获取最新事务ID
   * @param conf Hadoop配置对象
   * @param logDir 日志存储目录
   * @param journalId 日志ID，对应一个命名空间
   * @param startOpt 启动选项
   * @param errorReporter 存储错误报告器
   * @throws IOException 初始化或IO操作异常
   */
  Journal(Configuration conf, File logDir, String journalId,
      StartupOption startOpt, StorageErrorReporter errorReporter)
      throws IOException {
    this.conf = conf;
    storage = new JNStorage(conf, logDir, startOpt, errorReporter);
    this.journalId = journalId;

    refreshCachedData();
    
    this.fjm = storage.getJournalManager();

    this.cache = createCache();

    this.metrics = JournalMetrics.create(this);
    
    EditLogFile latest = scanStorageForLatestEdits();
    if (latest != null) {
      updateHighestWrittenTxId(latest.getLastTxId());
    }
  }

  /**
   * 根据配置创建或禁用增量edits缓存，用于支持HA NameNode快速尾追日志
   * @return 返回创建的缓存实例，禁用则返回null
   */
  private JournaledEditsCache createCache() {
    if (conf.getBoolean(DFSConfigKeys.DFS_HA_TAILEDITS_INPROGRESS_KEY,
        DFSConfigKeys.DFS_HA_TAILEDITS_INPROGRESS_DEFAULT)) {
      return new JournaledEditsCache(conf);
    } else {
      return null;
    }
  }

  /**
   * 设置是否已尝试按nameserviceId启动日志同步器
   * @param started 是否已尝试启动
   */
  public void setTriedJournalSyncerStartedwithnsId(boolean started) {
    this.triedJournalSyncerStartedwithnsId = started;
  }

  /**
   * 获取是否已尝试按nameserviceId启动日志同步器
   * @return 是否已尝试启动
   */
  public boolean getTriedJournalSyncerStartedwithnsId() {
    return triedJournalSyncerStartedwithnsId;
  }

  /**
   * 重新加载持久化元数据缓存，首次加载或格式化后需要调用
   */
  private synchronized void refreshCachedData() {
    IOUtils.closeStream(committedTxnId);
    
    File currentDir = storage.getSingularStorageDir().getCurrentDir();
    this.lastPromisedEpoch = new PersistentLongFile(
        new File(currentDir, LAST_PROMISED_FILENAME), 0);
    this.lastWriterEpoch = new PersistentLongFile(
        new File(currentDir, LAST_WRITER_EPOCH), 0);
    this.committedTxnId = new BestEffortLongFile(
        new File(currentDir, COMMITTED_TXID_FILENAME),
        HdfsServerConstants.INVALID_TXID);
  }
  
  /**
   * 扫描本地存储目录，找到包含最高事务ID的日志段
   * @return 包含最高事务的日志文件，无日志文件返回null
   * @throws IOException IO扫描异常
   */
  private synchronized EditLogFile scanStorageForLatestEdits() throws IOException {
    if (!fjm.getStorageDirectory().getCurrentDir().exists()) {
      return null;
    }
    
    LOG.info("Scanning storage " + fjm);
    List<EditLogFile> files = fjm.getLogFiles(0);
    
    while (!files.isEmpty()) {
      EditLogFile latestLog = files.remove(files.size() - 1);
      latestLog.scanLog(Long.MAX_VALUE, false);
      LOG.info("Latest log is " + latestLog + " ; journal id: " + journalId);
      if (latestLog.getLastTxId() == HdfsServerConstants.INVALID_TXID) {
        // the log contains no transactions
        LOG.warn("Latest log " + latestLog + " has no transactions. " +
            "moving it aside and looking for previous log"
            + " ; journal id: " + journalId);
        latestLog.moveAsideEmptyFile();
      } else {
        return latestLog;
      }
    }
    
    LOG.info("No files in " + fjm);
    return null;
  }

  /**
   * 格式化日志存储，写入命名空间信息
   * @param nsInfo 命名空间信息
   * @param force 是否强制格式化
   * @throws IOException 格式化IO异常
   */
  void format(NamespaceInfo nsInfo, boolean force) throws IOException {
    Preconditions.checkState(nsInfo.getNamespaceID() != 0,
        "can't format with uninitialized namespace info: %s",
        nsInfo);
    LOG.info("Formatting journal id : " + journalId + " with namespace info: " +
        nsInfo + " and force: " + force);
    storage.format(nsInfo, force);
    this.cache = createCache();
    refreshCachedData();
  }

  /**
   * 关闭日志，释放所有持有的资源
   * @throws IOException 关闭IO异常
   */
  @Override // Closeable
  public void close() throws IOException {
    IOUtils.closeStream(committedTxnId);
    IOUtils.closeStream(curSegment);
    storage.close();
  }
  
  /**
   * 获取当前Journal的存储对象
   * @return JN存储实例
   */
  JNStorage getStorage() {
    return storage;
  }
  
  /**
   * 获取当前Journal的ID
   * @return 日志ID
   */
  String getJournalId() {
    return journalId;
  }

  /**
   * 获取当前节点承诺接受的最新epoch，所有小于该epoch的写请求都会被拒绝
   * @return 最新承诺epoch，无承诺返回0
   * @throws IOException 读取持久化文件异常
   */
  synchronized long getLastPromisedEpoch() throws IOException {
    checkFormatted();
    return lastPromisedEpoch.get();
  }

  /**
   * 获取当前实际写入数据的最新writer epoch
   * @return 最新写入epoch
   * @throws IOException 读取持久化文件异常
   */
  synchronized public long getLastWriterEpoch() throws IOException {
    checkFormatted();
    return lastWriterEpoch.get();
  }

  /**
   * 获取当前已提交的最大事务ID
   * @return 已提交事务ID
   * @throws IOException 读取持久化文件异常
   */
  synchronized long getCommittedTxnId() throws IOException {
    return committedTxnId.get();
  }

  /**
   * 获取最后一次日志写入的时间戳
   * @return 最后写入时间戳
   */
  synchronized long getLastJournalTimestamp() {
    return lastJournalTimestamp;
  }

  /**
   * 获取当前日志的滞后事务数，即已提交最高事务与已写入最高事务的差值
   * @return 滞后事务数
   * @throws IOException 读取已提交事务ID异常
   */
  synchronized long getCurrentLagTxns() throws IOException {
    long committed = committedTxnId.get();
    if (committed == 0) {
      return 0;
    }
    
    return Math.max(committed - highestWrittenTxId, 0L);
  }
  
  /**
   * 获取已写入到日志的最高事务ID
   * @return 最高写入事务ID
   */
  synchronized long getHighestWrittenTxId() {
    return highestWrittenTxId;
  }

  /**
   * 更新已写入最高事务ID，同时更新底层日志管理器的lastReadableTxId
   * @param val 新的最高事务ID
   */
  private void updateHighestWrittenTxId(long val) {
    highestWrittenTxId = val;
    fjm.setLastReadableTxId(val);
  }

  /**
   * 获取当前Journal的指标对象
   * @return 指标实例
   */
  JournalMetrics getMetrics() {
    return metrics;
  }

  /**
   * 启动新的epoch，检查一致性并更新承诺epoch，返回现有日志信息供新writer恢复
   * @param nsInfo 命名空间信息，用于一致性校验
   * @param epoch 新epoch编号
   * @return 包含最新日志段信息的响应proto
   * @throws IOException 校验失败或IO异常
   */
  synchronized NewEpochResponseProto newEpoch(
      NamespaceInfo nsInfo, long epoch) throws IOException {

    checkFormatted();
    storage.checkConsistentNamespace(nsInfo);

    // Check that the new epoch being proposed is in fact newer than
    // any other that we've promised. 
    if (epoch <= getLastPromisedEpoch()) {
      throw new IOException("Proposed epoch " + epoch + " <= last promise " +
          getLastPromisedEpoch() + " ; journal id: " + journalId);
    }
    
    updateLastPromisedEpoch(epoch);
    abortCurSegment();
    
    NewEpochResponseProto.Builder builder =
        NewEpochResponseProto.newBuilder();

    EditLogFile latestFile = scanStorageForLatestEdits();

    if (latestFile != null) {
      builder.setLastSegmentTxId(latestFile.getFirstTxId());
    }
    
    return builder.build();
  }

  /**
   * 更新承诺的最新epoch，重置当前epoch的IPC序列号
   * @param newEpoch 新的epoch编号
   * @throws IOException 持久化更新异常
   */
  private void updateLastPromisedEpoch(long newEpoch) throws IOException {
    LOG.info("Updating lastPromisedEpoch from " + lastPromisedEpoch.get() +
        " to " + newEpoch + " for client " + Server.getRemoteIp() +
        " ; journal id: " + journalId);
    lastPromisedEpoch.set(newEpoch);
    
    // Since we have a new writer, reset the IPC serial - it will start
    // counting again from 0 for this writer.
    currentEpochIpcSerial = -1;
  }

  /**
   * 终止当前打开的日志段，清空当前段状态
   * @throws IOException 终止IO异常
   */
  private void abortCurSegment() throws IOException {
    if (curSegment == null) {
      return;
    }
    
    curSegment.abort();
    curSegment = null;
    curSegmentTxId = HdfsServerConstants.INVALID_TXID;
    curSegmentLayoutVersion = 0;
  }

  /**
   * 批量写入edits事务到当前日志段，完成flush