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
package org.apache.hadoop.hdfs.qjournal.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.util.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetJournaledEditsResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetJournalStateResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.NewEpochResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.PrepareRecoveryResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.SegmentStateProto;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.common.Util;
import org.apache.hadoop.hdfs.server.namenode.EditLogFileInputStream;
import org.apache.hadoop.hdfs.server.namenode.EditLogInputStream;
import org.apache.hadoop.hdfs.server.namenode.EditLogOutputStream;
import org.apache.hadoop.hdfs.server.namenode.JournalManager;
import org.apache.hadoop.hdfs.server.namenode.JournalSet;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLog;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;
import org.apache.hadoop.hdfs.web.URLConnectionFactory;
import org.apache.hadoop.log.LogThrottlingHelper;
import org.apache.hadoop.log.LogThrottlingHelper.LogAction;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.thirdparty.protobuf.TextFormat;

/**
 * 基于观察者日志节点集群的JournalManager实现，写操作需要获得集群中大多数节点的确认才成功。
 * 用于HDFS高可用场景中，共享NameNode的编辑日志。
 */
@InterfaceAudience.Private
public class QuorumJournalManager implements JournalManager {
  static final Logger LOG = LoggerFactory.getLogger(QuorumJournalManager.class);

  // This config is not publicly exposed
  public static final String QJM_RPC_MAX_TXNS_KEY =
      "dfs.ha.tail-edits.qjm.rpc.max-txns";
  public static final int QJM_RPC_MAX_TXNS_DEFAULT = 5000;

  // 单次RPC获取编辑日志事务的最大数量
  private final int maxTxnsPerRpc;
  // 是否开启对未完成编辑日志的尾部读取
  private final boolean inProgressTailingEnabled;
  // QJM各操作的超时时间配置
  private final int startSegmentTimeoutMs;
  private final int prepareRecoveryTimeoutMs;
  private final int acceptRecoveryTimeoutMs;
  private final int finalizeSegmentTimeoutMs;
  private final int selectInputStreamsTimeoutMs;
  private final int getJournalStateTimeoutMs;
  private final int newEpochTimeoutMs;
  private final int writeTxnsTimeoutMs;

  // This timeout is used for calls that don't occur during normal operation
  // e.g. format, upgrade operations and a few others. So we can use rather
  // lengthy timeouts by default.
  // 非常规操作（格式化、升级等）的超时时间，默认设置较长
  private final int timeoutMs;
  
  private final Configuration conf;
  private final URI uri;
  private final NamespaceInfo nsInfo;
  private final String nameServiceId;
  private boolean isActiveWriter;
  
  private final AsyncLoggerSet loggers;

  private static final int OUTPUT_BUFFER_CAPACITY_DEFAULT = 512 * 1024;
  private int outputBufferCapacity;
  private final URLConnectionFactory connectionFactory;

  /** Limit logging about input stream selection to every 5 seconds max. */
  // 输入流选择日志限流，最多每5秒输出一次
  private static final long SELECT_INPUT_STREAM_LOG_INTERVAL_MS = 5000;
  private final LogThrottlingHelper selectInputStreamLogHelper =
      new LogThrottlingHelper(SELECT_INPUT_STREAM_LOG_INTERVAL_MS);

  @VisibleForTesting
  public QuorumJournalManager(Configuration conf,
                              URI uri,
                              NamespaceInfo nsInfo) throws IOException {
    this(conf, uri, nsInfo, null, IPCLoggerChannel.FACTORY);
  }
  
  public QuorumJournalManager(Configuration conf,
      URI uri, NamespaceInfo nsInfo, String nameServiceId) throws IOException {
    this(conf, uri, nsInfo, nameServiceId, IPCLoggerChannel.FACTORY);
  }

  @VisibleForTesting
  QuorumJournalManager(Configuration conf,
                       URI uri, NamespaceInfo nsInfo,
                       AsyncLogger.Factory loggerFactory) throws IOException {
    this(conf, uri, nsInfo, null, loggerFactory);

  }

  
  /**
   * 构造QuorumJournalManager实例，初始化配置和所有JournalNode连接
   */
  QuorumJournalManager(Configuration conf,
      URI uri, NamespaceInfo nsInfo, String nameServiceId,
      AsyncLogger.Factory loggerFactory) throws IOException {
    Preconditions.checkArgument(conf != null, "must be configured");

    this.conf = conf;
    this.uri = uri;
    this.nsInfo = nsInfo;
    this.nameServiceId = nameServiceId;
    this.loggers = new AsyncLoggerSet(createLoggers(loggerFactory));

    this.maxTxnsPerRpc =
        conf.getInt(QJM_RPC_MAX_TXNS_KEY, QJM_RPC_MAX_TXNS_DEFAULT);
    Preconditions.checkArgument(maxTxnsPerRpc > 0,
        "Must specify %s greater than 0!", QJM_RPC_MAX_TXNS_KEY);
    this.inProgressTailingEnabled = conf.getBoolean(
        DFSConfigKeys.DFS_HA_TAILEDITS_INPROGRESS_KEY,
        DFSConfigKeys.DFS_HA_TAILEDITS_INPROGRESS_DEFAULT);
    // 从配置加载各操作超时时间
    this.startSegmentTimeoutMs = conf.getInt(
        DFSConfigKeys.DFS_QJOURNAL_START_SEGMENT_TIMEOUT_KEY,
        DFSConfigKeys.DFS_QJOURNAL_START_SEGMENT_TIMEOUT_DEFAULT);
    this.prepareRecoveryTimeoutMs = conf.getInt(
        DFSConfigKeys.DFS_QJOURNAL_PREPARE_RECOVERY_TIMEOUT_KEY,
        DFSConfigKeys.DFS_QJOURNAL_PREPARE_RECOVERY_TIMEOUT_DEFAULT);
    this.acceptRecoveryTimeoutMs = conf.getInt(
        DFSConfigKeys.DFS_QJOURNAL_ACCEPT_RECOVERY_TIMEOUT_KEY,
        DFSConfigKeys.DFS_QJOURNAL_ACCEPT_RECOVERY_TIMEOUT_DEFAULT);
    this.finalizeSegmentTimeoutMs = conf.getInt(
        DFSConfigKeys.DFS_QJOURNAL_FINALIZE_SEGMENT_TIMEOUT_KEY,
        DFSConfigKeys.DFS_QJOURNAL_FINALIZE_SEGMENT_TIMEOUT_DEFAULT);
    this.selectInputStreamsTimeoutMs = conf.getInt(
        DFSConfigKeys.DFS_QJOURNAL_SELECT_INPUT_STREAMS_TIMEOUT_KEY,
        DFSConfigKeys.DFS_QJOURNAL_SELECT_INPUT_STREAMS_TIMEOUT_DEFAULT);
    this.getJournalStateTimeoutMs = conf.getInt(
        DFSConfigKeys.DFS_QJOURNAL_GET_JOURNAL_STATE_TIMEOUT_KEY,
        DFSConfigKeys.DFS_QJOURNAL_GET_JOURNAL_STATE_TIMEOUT_DEFAULT);
    this.newEpochTimeoutMs = conf.getInt(
        DFSConfigKeys.DFS_QJOURNAL_NEW_EPOCH_TIMEOUT_KEY,
        DFSConfigKeys.DFS_QJOURNAL_NEW_EPOCH_TIMEOUT_DEFAULT);
    this.writeTxnsTimeoutMs = conf.getInt(
        DFSConfigKeys.DFS_QJOURNAL_WRITE_TXNS_TIMEOUT_KEY,
        DFSConfigKeys.DFS_QJOURNAL_WRITE_TXNS_TIMEOUT_DEFAULT);
    this.timeoutMs = (int) conf.getTimeDuration(DFSConfigKeys
            .DFS_QJM_OPERATIONS_TIMEOUT,
        DFSConfigKeys.DFS_QJM_OPERATIONS_TIMEOUT_DEFAULT, TimeUnit
            .MILLISECONDS);

    int connectTimeoutMs = conf.getInt(
        DFSConfigKeys.DFS_QJOURNAL_HTTP_OPEN_TIMEOUT_KEY,
        DFSConfigKeys.DFS_QJOURNAL_HTTP_OPEN_TIMEOUT_DEFAULT);
    int readTimeoutMs = conf.getInt(
        DFSConfigKeys.DFS_QJOURNAL_HTTP_READ_TIMEOUT_KEY,
        DFSConfigKeys.DFS_QJOURNAL_HTTP_READ_TIMEOUT_DEFAULT);
    // 创建HTTP连接工厂，用于流式拉取编辑日志
    this.connectionFactory = URLConnectionFactory
        .newDefaultURLConnectionFactory(connectTimeoutMs, readTimeoutMs, conf);
    setOutputBufferCapacity(OUTPUT_BUFFER_CAPACITY_DEFAULT);
  }
  
  protected List<AsyncLogger> createLoggers(
      AsyncLogger.Factory factory) throws IOException {
    return createLoggers(conf, uri, nsInfo, factory, nameServiceId);
  }

  /**
   * 从URI解析日志ID，检查格式合法性
   * @param uri QJM连接URI
   * @return 解析得到的日志ID
   */
  static String parseJournalId(URI uri) {
    String path = uri.getPath();
    Preconditions.checkArgument(path != null && !path.isEmpty(),
        "Bad URI '%s': must identify journal in path component",
        uri);
    String journalId = path.substring(1);
    checkJournalId(journalId);
    return journalId;
  }
  
  /**
   * 检查日志ID格式是否合法
   * @param jid 待检查的日志ID
   */
  public static void checkJournalId(String jid) {
    Preconditions.checkArgument(jid != null &&
        !jid.isEmpty() &&
        !jid.contains("/") &&
        !jid.startsWith("."),
        "bad journal id: " + jid);
  }

  
  /**
   * 隔离之前的写入者，并获取一个唯一的epoch编号用于写访问
   * @return 所有JournalNode的NewEpoch响应，key为对应AsyncLogger
   * @throws IOException 获取失败时抛出异常
   */
  Map<AsyncLogger, NewEpochResponseProto> createNewUniqueEpoch()
      throws IOException {
    Preconditions.checkState(!loggers.isEpochEstablished(),
        "epoch already created");
    
    Map<AsyncLogger, GetJournalStateResponseProto> lastPromises =
      loggers.waitForWriteQuorum(loggers.getJournalState(),
          getJournalStateTimeoutMs, "getJournalState()");
    
    // 找出所有节点中最大的已承诺epoch编号
    long maxPromised = Long.MIN_VALUE;
    for (GetJournalStateResponseProto resp : lastPromises.values()) {
      maxPromised = Math.max(maxPromised, resp.getLastPromisedEpoch());
    }
    assert maxPromised >= 0;
    
    // 新epoch编号比现有最大编号大1，保证唯一性
    long myEpoch = maxPromised + 1;
    Map<AsyncLogger, NewEpochResponseProto> resps =
        loggers.waitForWriteQuorum(loggers.newEpoch(nsInfo, myEpoch),
            newEpochTimeoutMs, "newEpoch(" + myEpoch + ")");
        
    loggers.setEpoch(myEpoch);
    return resps;
  }
  
  @Override
  /**
   * 格式化所有JournalNode上的日志存储
   */
  public void format(NamespaceInfo nsInfo, boolean force) throws IOException {
    QuorumCall<AsyncLogger, Void> call = loggers.format(nsInfo, force);
    try {
      call.waitFor(loggers.size(), loggers.size(), 0, timeoutMs,
          "format");
    } catch (InterruptedException e) {
      throw new IOException("Interrupted waiting for format() response");
    } catch (TimeoutException e) {
      throw new IOException("Timed out waiting for format() response");
    }
    
    if (call.countExceptions() > 0) {
      call.rethrowException("Could not format one or more JournalNodes");
    }
  }

  @Override
  /**
   * 检查是否已有至少一个JournalNode存储了数据
   * @return true如果已有数据，否则返回false
   */
  public boolean hasSomeData() throws IOException {
    QuorumCall<AsyncLogger, Boolean> call =
        loggers.isFormatted();

    try {
      call.waitFor(loggers.size(), 0, 0, timeoutMs, "hasSomeData");
    } catch (InterruptedException e) {
      throw new IOException("Interrupted while determining if JNs have data");
    } catch (TimeoutException e) {
      throw new IOException("Timed out waiting for response from loggers");
    }
    
    if (call.countExceptions() > 0) {
      call.rethrowException(
          "Unable to check if JNs are ready for formatting");
    }
    
    // If any of the loggers returned with a non-empty manifest, then
    // we should prompt for format.
    // 只要有一个节点有数据，就认为整个日志集群已有数据
    for (Boolean hasData : call.getResults().values()) {
      if (hasData) {
        return true;
      }
    }

    // 所有节点都未格式化，可以进行格式化
    return false;
  }

  /**
   * 恢复未关闭的日志段，将其最终化并同步所有节点。
   * 恢复完成后，大多数节点上该段将被最终化，且所有节点对段长度达成一致
   * @param segmentTxId 待恢复段的起始事务ID
   * @throws IOException 恢复失败时抛出异常
   */
  private void recoverUnclosedSegment(long segmentTxId) throws IOException {
    Preconditions.checkArgument(segmentTxId > 0);
    LOG.info("Beginning recovery of unclosed segment starting at txid " +
        segmentTxId);
    
    // Step 1. 准备恢复阶段，向所有节点发起准备请求
    QuorumCall<AsyncLogger,PrepareRecoveryResponseProto> prepare =
        loggers.prepareRecovery(segmentTxId);
    Map<AsyncLogger, PrepareRecoveryResponseProto> prepareResponses=
        loggers.waitForWriteQuorum(prepare, prepareRecoveryTimeoutMs,
            "prepareRecovery(" + segmentTxId + ")");
    LOG.info("Recovery prepare phase complete. Responses:\n" +
        QuorumCall.mapToString(prepareResponses));

    // 选择最优日志源：优先选择已经接受过恢复提案的，否则选择最长的日志段
    Entry<AsyncLogger, PrepareRecoveryResponseProto> bestEntry = Collections.max(
        prepareResponses.entrySet(), SegmentRecoveryComparator.INSTANCE); 
    AsyncLogger bestLogger = bestEntry.getKey();
    PrepareRecoveryResponseProto bestResponse = bestEntry.getValue();
    
    // 记录选择结果，检查不变量
    if (bestResponse.hasAcceptedInEpoch()) {
      LOG.info("Using already-accepted recovery for segment " +
          "starting at txid " + segmentTxId + ": " +
          bestEntry);
    } else if (bestResponse.hasSegmentState()) {
      LOG.info("Using longest log: " + bestEntry);
    } else {
      // 所有响应节点都没有该段日志，无需恢复直接返回
      for (PrepareRecoveryResponseProto resp : prepareResponses.values()) {
        assert !resp.hasSegmentState() :
          "One of the loggers had a response, but no best logger " +
          "was found.";
      }

      LOG.info("None of the responders had a log to recover: " +
          QuorumCall.mapToString(prepareResponses));
      return;
    }
    
    SegmentStateProto logToSync = bestResponse.getSegmentState();
    assert segmentTxId == logToSync.getStartTxId();
    
    // 一致性检查：所有节点已知的最大提交事务ID不超过要截断到的结束事务ID
    for (Map.Entry<AsyncLogger, PrepareRecoveryResponseProto> e :
         prepareResponses.entrySet()) {
      AsyncLogger logger = e.getKey();
      PrepareRecoveryResponseProto resp = e.getValue();

      if (resp.hasLastCommittedTxId() &&
          resp.getLastCommittedTxId() > logToSync.getEndTxId()) {
        throw new AssertionError("Decided to synchronize log to " + logToSync +
            " but logger " + logger + " had seen txid " +
            resp.getLastCommittedTxId() + " committed");
      }
    }
    
    // 从最优节点获取日志段同步地址
    URL syncFromUrl = bestLogger.buildURLToFetchLogs(segmentTxId);
    
    // 让所有节点从最优节点同步日志段
    QuorumCall<AsyncLogger,Void> accept = loggers.acceptRecovery(logToSync, syncFromUrl);
    loggers