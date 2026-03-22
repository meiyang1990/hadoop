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
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.hdfs.qjournal.protocol.JournalOutOfSyncException;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocol;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetEditLogManifestResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetJournaledEditsResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.GetJournalStateResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.NewEpochResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.PrepareRecoveryResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.SegmentStateProto;
import org.apache.hadoop.hdfs.qjournal.protocol.RequestInfo;
import org.apache.hadoop.hdfs.qjournal.protocolPB.QJournalProtocolPB;
import org.apache.hadoop.hdfs.qjournal.protocolPB.QJournalProtocolTranslatorPB;
import org.apache.hadoop.hdfs.qjournal.server.GetJournalEditServlet;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;
import org.apache.hadoop.ipc.ProtobufRpcEngine2;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.util.StopWatch;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.net.InetAddresses;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.FutureCallback;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.Futures;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ListenableFuture;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.MoreExecutors;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.UncaughtExceptionHandlers;
import org.apache.hadoop.util.Time;

/**
 * 文件说明：基于Hadoop IPC的远程JournalNode通信通道实现，属于HDFS QJM（Quorum Journal Manager）模块
 * 核心职责：为NameNode提供异步方式与单个JournalNode通信，所有RPC调用异步执行并返回ListenableFuture，
 * 支持quorum调用聚合，实现写操作串行、读操作并行的执行模型
 */
/**
 * Channel to a remote JournalNode using Hadoop IPC.
 * All of the calls are run on a separate thread, and return
 * {@link ListenableFuture} instances to wait for their result.
 * This allows calls to be bound together using the {@link QuorumCall}
 * class.
 */
@InterfaceAudience.Private
public class IPCLoggerChannel implements AsyncLogger {

  private final Configuration conf;
  protected final InetSocketAddress addr;
  private QJournalProtocol proxy;

  /**
   * 串行执行器：单线程FIFO顺序执行提交的任务，主要用于写操作，保证编辑日志顺序不被打乱
   */
  private final ListeningExecutorService singleThreadExecutor;
  /**
   * 并行执行器：可并行执行任务，与串行执行器任务互不干扰，主要用于读操作，可安全重排
   */
  private final ListeningExecutorService parallelExecutor;
  private long ipcSerial = 0;
  private long epoch = -1;
  private long committedTxId = HdfsServerConstants.INVALID_TXID;
  
  private final String journalId;
  private final String nameServiceId;

  private final NamespaceInfo nsInfo;

  private URL httpServerURL;

  private final IPCLoggerChannelMetrics metrics;
  
  /**
   * 队列中待处理的编辑日志字节数，用于限流防止OOM
   */
  private int queuedEditsSizeBytes = 0;
  
  /**
   * 当前远程JournalNode上已成功确认写入的最大事务ID
   */
  private long highestAckedTxId = 0;

  /**
   * 上次成功向远程节点写入编辑日志的纳秒时间戳，用于计算延迟
   */
  private long lastAckNanos = 0;

  /**
   * 上次更新committedTxId的纳秒时间戳，用于计算时间维度的延迟，不只是事务数延迟
   */
  private long lastCommitNanos = 0;
  
  /**
   * 等待队列最大字节数限制，防止单个JournalNode响应过慢导致OOM
   * 当队列溢出时，将该日志节点标记为错误，不再接受新写入
   */
  private final int queueSizeLimitBytes;

  /**
   * 日志节点不同步标记：当日志节点丢失部分编辑数据或分段重启，
   * 在下一个分段开始前无法继续写入，设置此标记避免发送无用RPC
   */
  private boolean outOfSync = false;
  
  /**
   * 心跳计时器：每次发送心跳后重新计时
   */
  private final StopWatch lastHeartbeatStopwatch = new StopWatch();
  
  private static final long HEARTBEAT_INTERVAL_MILLIS = 1000;

  private static final long WARN_JOURNAL_MILLIS_THRESHOLD = 1000;
  
  static final Factory FACTORY = IPCLoggerChannel::new;

  /**
   * 构造函数：创建IPC日志通道，不指定名称服务ID
   * @param conf Hadoop配置
   * @param nsInfo 命名空间信息
   * @param journalId 日志ID
   * @param addr 远程JournalNode地址
   */
  public IPCLoggerChannel(Configuration conf, NamespaceInfo nsInfo,
      String journalId, InetSocketAddress addr) {
    this(conf, nsInfo, journalId, null, addr);
  }

  /**
   * 构造函数：创建IPC日志通道，指定名称服务ID
   * @param conf Hadoop配置
   * @param nsInfo 命名空间信息
   * @param journalId 日志ID
   * @param nameServiceId 名称服务ID
   * @param addr 远程JournalNode地址
   */
  public IPCLoggerChannel(Configuration conf, NamespaceInfo nsInfo,
      String journalId, String nameServiceId, InetSocketAddress addr) {
    this.conf = conf;
    this.nsInfo = nsInfo;
    this.journalId = journalId;
    this.nameServiceId = nameServiceId;
    this.addr = addr;
    // 计算队列大小限制，配置值单位MB，转换为字节
    this.queueSizeLimitBytes = 1024 * 1024 * conf.getInt(
        DFSConfigKeys.DFS_QJOURNAL_QUEUE_SIZE_LIMIT_KEY,
        DFSConfigKeys.DFS_QJOURNAL_QUEUE_SIZE_LIMIT_DEFAULT);
    
    singleThreadExecutor = MoreExecutors.listeningDecorator(
        createSingleThreadExecutor());
    parallelExecutor = MoreExecutors.listeningDecorator(
        createParallelExecutor());
    
    metrics = IPCLoggerChannelMetrics.create(this);
  }
  
  @Override
  public synchronized void setEpoch(long epoch) {
    this.epoch = epoch;
  }
  
  @Override
  public synchronized void setCommittedTxId(long txid) {
    Preconditions.checkArgument(txid >= committedTxId,
        "Trying to move committed txid backwards in client " +
         "old: %s new: %s", committedTxId, txid);
    this.committedTxId = txid;
    this.lastCommitNanos = Time.monotonicNowNanos();
  }
  
  @Override
  public void close() {
    // 关闭后不再接受新任务
    singleThreadExecutor.shutdown();
    parallelExecutor.shutdown();
    if (proxy != null) {
      // TODO: this can hang for quite some time if the client
      // is currently in the middle of a call to a downed JN.
      // We should instead do this asynchronously, and just stop
      // making any more calls after this point (eg clear the queue)
      RPC.stopProxy(proxy);
    }
    metrics.unregister();
  }
  
  /**
   * 获取RPC代理，延迟创建代理实例
   * @return QJournalProtocol代理对象
   * @throws IOException 创建代理失败时抛出IO异常
   */
  protected QJournalProtocol getProxy() throws IOException {
    if (proxy != null) return proxy;
    proxy = createProxy();
    return proxy;
  }
  
  /**
   * 创建QJournalProtocol RPC代理
   * @return 创建好的协议代理
   * @throws IOException 创建失败时抛出IO异常
   */
  protected QJournalProtocol createProxy() throws IOException {
    final Configuration confCopy = new Configuration(conf);
    
    // 启用TCP_NODELAY，避免大于MTU的批量数据引发40ms延迟
    confCopy.setBoolean(CommonConfigurationKeysPublic.IPC_CLIENT_TCPNODELAY_KEY, true);
    RPC.setProtocolEngine(confCopy,
        QJournalProtocolPB.class, ProtobufRpcEngine2.class);
    return SecurityUtil.doAsLoginUser(
        (PrivilegedExceptionAction<QJournalProtocol>) () -> {
          RPC.setProtocolEngine(confCopy,
              QJournalProtocolPB.class, ProtobufRpcEngine2.class);
          QJournalProtocolPB pbproxy = RPC.getProxy(
              QJournalProtocolPB.class,
              RPC.getProtocolVersion(QJournalProtocolPB.class),
              addr, confCopy);
          return new QJournalProtocolTranslatorPB(pbproxy);
        });
  }
  
  
  /**
   * 创建单线程执行器，分离出来方便测试覆盖
   * @return 单线程线程池
   */
  @VisibleForTesting
  protected ExecutorService createSingleThreadExecutor() {
    return Executors.newSingleThreadExecutor(
        new ThreadFactoryBuilder()
          .setDaemon(true)
          .setNameFormat("Logger channel (from single-thread executor) to " + addr)
          .setUncaughtExceptionHandler(UncaughtExceptionHandlers.systemExit())
          .build());
  }

  /**
   * 创建并行执行器，分离出来方便测试覆盖
   * @return 并行线程池
   */
  @VisibleForTesting
  protected ExecutorService createParallelExecutor() {
    int numThreads =
        conf.getInt(DFSConfigKeys.DFS_QJOURNAL_PARALLEL_READ_NUM_THREADS_KEY,
            DFSConfigKeys.DFS_QJOURNAL_PARALLEL_READ_NUM_THREADS_DEFAULT);
    ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(numThreads,
        numThreads, 60L, TimeUnit.SECONDS,
        new LinkedBlockingQueue<>(),
        new ThreadFactoryBuilder().setDaemon(true)
            .setNameFormat("Logger channel (from parallel executor) to " + addr)
            .setUncaughtExceptionHandler(UncaughtExceptionHandlers.systemExit())
            .build());
    // 允许核心线程超时回收，节省资源
    threadPoolExecutor.allowCoreThreadTimeOut(true);
    return threadPoolExecutor;
  }
  
  @Override
  public URL buildURLToFetchLogs(long segmentTxId) {
    Preconditions.checkArgument(segmentTxId > 0,
        "Invalid segment: %s", segmentTxId);
    Preconditions.checkState(hasHttpServerEndPoint(), "No HTTP/HTTPS endpoint");
        
    try {
      String path = GetJournalEditServlet.buildPath(
          journalId, segmentTxId, nsInfo, true);
      return new URL(httpServerURL, path);
    } catch (MalformedURLException e) {
      // should never get here.
      throw new RuntimeException(e);
    }
  }

  private synchronized RequestInfo createReqInfo() {
    Preconditions.checkState(epoch > 0, "bad epoch: " + epoch);
    return new RequestInfo(journalId, nameServiceId,
        epoch, ipcSerial++, committedTxId);
  }

  public synchronized int getQueuedEditsSize() {
    return queuedEditsSizeBytes;
  }
  
  public InetSocketAddress getRemoteAddress() {
    return addr;
  }

  /**
   * 检查当前日志节点是否与客户端不同步，需要滚动日志后才能恢复写入
   * @return true表示已不同步，需要滚动日志
   */
  public synchronized boolean isOutOfSync() {
    return outOfSync;
  }
  
  @VisibleForTesting
  void waitForAllPendingCalls() throws InterruptedException {
    try {
      singleThreadExecutor.submit(() -> {}).get();
    } catch (ExecutionException e) {
      // This can't happen!
      throw new AssertionError(e);
    }
  }

  @Override
  public ListenableFuture<Boolean> isFormatted() {
    return singleThreadExecutor.submit(() -> getProxy().isFormatted(journalId, nameServiceId));
  }

  @Override
  public ListenableFuture<GetJournalStateResponseProto> getJournalState() {
    return singleThreadExecutor.submit(() -> {
      GetJournalStateResponseProto ret = getProxy().getJournalState(journalId, nameServiceId);
      constructHttpServerURI(ret);
      return ret;
    });
  }

  @Override
  public ListenableFuture<NewEpochResponseProto> newEpoch(
      final long epoch) {
    return singleThreadExecutor.submit(
        () -> getProxy().newEpoch(journalId, nameServiceId, nsInfo, epoch));
  }
  
  @Override
  public ListenableFuture<Void> sendEdits(
      final long segmentTxId, final long firstTxnId,
      final int numTxns, final byte[] data) {
    try {
      reserveQueueSpace(data.length);
    } catch (LoggerTooFarBehindException e) {
      return Futures.immediateFailedFuture(e);
    }
    
    // 记录批次提交时间，用于计算端到端延迟
    final long submitNanos = Time.monotonicNowNanos();
    
    ListenableFuture<Void> ret = null;
    try {
      ret = singleThreadExecutor.submit(() -> {
        throwIfOutOfSync();

        final long rpcSendTimeNanos = Time.monotonicNowNanos();
        try {
          getProxy().journal(createReqInfo(), segmentTxId, firstTxnId, numTxns, data);
        } catch (IOException e) {
          QuorumJournalManager.LOG.warn("Remote journal {} failed to write txns {}-{}."
                  + " Will try to write to this JN again after the next log roll.",
              IPCLoggerChannel.this, firstTxnId, (firstTxnId + numTxns - 1), e);
          synchronized (IPCLoggerChannel.this) {
            outOfSync = true;
          }
          throw e;
        } finally {
          final long nowNanos = Time.monotonicNowNanos();
          final long rpcTimeMicros = TimeUnit.MICROSECONDS.convert(
              (nowNanos - rpcSendTimeNanos), TimeUnit.NANOSECONDS);
          final long endToEndTimeMicros = TimeUnit.MICROSECONDS.convert(
              (nowNanos - submitNanos), TimeUnit.NANOSECONDS);
          metrics.addWriteEndToEndLatency(endToEndTimeMicros);
          metrics.addWriteRpcLatency(rpcTimeMicros);
          if (rpcTimeMicros / 1000 > WARN_JOURNAL_MILLIS_THRESHOLD) {
            QuorumJournalManager.LOG.warn(
                "Took {}ms to send a batch of {} edits ({} bytes) to remote journal {}.",
                rpcTimeMicros / 1000, numTxns, data.length, IPCLoggerChannel.this);
          }
        }
        synchronized (IPCLoggerChannel.this) {
          highestAckedTxId = firstTxnId + numTxns - 1;
          lastAckNanos = submitNanos;
        }
        return null;
      });
    } finally {
      if (ret == null) {
        // 提交失败，回滚队列大小
        unreserveQueueSpace(data.length);
      } else {
        // 提交成功，任务完成后调整队列大小，无论成功失败
        Futures.addCallback(ret, new FutureCallback<Void>() {
          @Override
          public void onFailure(Throwable t) {