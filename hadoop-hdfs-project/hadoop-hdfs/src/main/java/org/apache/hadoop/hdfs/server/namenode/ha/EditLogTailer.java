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

package org.apache.hadoop.hdfs.server.namenode.ha;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.thirdparty.com.google.common.collect.Iterators;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.util.Timer;
import org.apache.hadoop.util.concurrent.SubjectInheritingThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocolPB.NamenodeProtocolPB;
import org.apache.hadoop.hdfs.protocolPB.NamenodeProtocolTranslatorPB;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.EditLogInputException;
import org.apache.hadoop.hdfs.server.namenode.EditLogInputStream;
import org.apache.hadoop.hdfs.server.namenode.FSEditLog;
import org.apache.hadoop.hdfs.server.namenode.FSImage;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.util.RwLockMode;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.security.SecurityUtil;

import static org.apache.hadoop.util.ExitUtil.terminate;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Preconditions;


/**
 * 文件: hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/ha/EditLogTailer.java
 * 
 * HDFS高可用场景下，负责在备NameNode(Standby NameNode)后台周期性拉取主NameNode日志的服务类。
 * 通过持续从共享日志存储（如QJM）拉取最新编辑日志并应用到本地元数据，保持备NameNode元数据与主NameNode同步，
 * 为故障转移时快速切换提供数据基础。
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class EditLogTailer {
  public static final Logger LOG = LoggerFactory.getLogger(EditLogTailer.class);

  /**
   * 备NameNode每次获取namesystem写锁后，最多应用这么多事务。
   * 应用完成后会释放锁，让读请求可以得到处理，然后再重新获取锁加载更多事务。
   * 默认情况下会持有写锁直到整个日志段处理完成。
   */
  public static final String  DFS_HA_TAILEDITS_MAX_TXNS_PER_LOCK_KEY =
      "dfs.ha.tail-edits.max-txns-per-lock";
  public static final long DFS_HA_TAILEDITS_MAX_TXNS_PER_LOCK_DEFAULT =
      Long.MAX_VALUE;

  private final EditLogTailerThread tailerThread;
  
  private final Configuration conf;
  private final FSNamesystem namesystem;
  private final Iterator<RemoteNameNodeInfo> nnLookup;
  private FSEditLog editLog;

  private RemoteNameNodeInfo currentNN;

  /**
   * 上次触发日志滚动时对应的事务ID
   */
  private long lastRollTriggerTxId = HdfsServerConstants.INVALID_TXID;
  
  /**
   * 备NameNode已经加载的最大事务ID
   */
  private long lastLoadedTxnId = HdfsServerConstants.INVALID_TXID;

  /**
   * 上次成功从共享目录加载非零数量编辑日志的时间
   */
  private long lastLoadTimeMs;

  /**
   * 上次在主NameNode触发编辑日志滚动的时间
   */
  private long lastRollTimeMs;

  /**
   * 备NameNode触发主NameNode日志滚动的周期。由于备NameNode只能读取已完成的日志段，
   * 滚动频率决定了备NameNode可以落后主NameNode的最大延迟。
   */
  private final long logRollPeriodMs;

  /**
   * 向主NameNode发送rollEdits RPC调用的超时时间，详见HDFS-4176
   */
  private final long rollEditsTimeoutMs;

  /**
   * 用于异步执行日志滚动RPC调用的线程池
   */
  private final ExecutorService rollEditsRpcExecutor;

  /**
   * 编辑日志尾部检查周期，是指数退避前的初始等待时间
   */
  private final long sleepTimeMs;
  /**
   * 两次检查编辑日志之间的最大等待时间。当拉取日志但没有新内容时，会触发指数退避，
   * 等待时间会逐次翻倍直到该最大值。如果该值小于等于0则禁用退避。
   */
  private final long maxSleepTimeMs;

  private final int nnCount;
  private NamenodeProtocol cachedActiveProxy = null;
  // 当前循环中已经尝试过的NameNode计数
  private int nnLoopCount = 0;

  /**
   * 尝试连接远程NameNode失败后，最多重试每个节点多少次
   */
  private int maxRetries;

  /**
   * 是否允许拉取进行中的编辑日志段。如果开启，会优化尾拖延迟，使用RPC机制拉取（针对QJournalManager）
   */
  private final boolean inProgressOk;

  /**
   * 每次获取锁后最多加载多少事务，完成后释放锁再重新获取
   */
  private final long maxTxnsPerLock;

  /**
   * 计时器实例，仅通过构造函数设置，仅测试可修改，生产代码应视为final
   */
  private Timer timer;

  /**
   * 构造编辑日志尾拖服务，基于给定的Namesystem和配置初始化参数
   * @param namesystem 备NameNode的命名空间对象
   * @param conf Hadoop配置
   */
  public EditLogTailer(FSNamesystem namesystem, Configuration conf) {
    this.tailerThread = new EditLogTailerThread();
    this.conf = conf;
    this.namesystem = namesystem;
    this.timer = new Timer();
    this.editLog = namesystem.getEditLog();
    this.lastLoadTimeMs = timer.monotonicNow();
    this.lastRollTimeMs = timer.monotonicNow();

    // 从配置读取日志滚动周期，转换为毫秒
    logRollPeriodMs = conf.getTimeDuration(
        DFSConfigKeys.DFS_HA_LOGROLL_PERIOD_KEY,
        DFSConfigKeys.DFS_HA_LOGROLL_PERIOD_DEFAULT,
        TimeUnit.SECONDS, TimeUnit.MILLISECONDS);
    List<RemoteNameNodeInfo> nns = Collections.emptyList();
    if (logRollPeriodMs >= 0) {
      try {
        // 获取配置中所有远程NameNode信息
        nns = RemoteNameNodeInfo.getRemoteNameNodes(conf);
      } catch (IOException e) {
        throw new IllegalArgumentException("Remote NameNodes not correctly configured!", e);
      }

      // 遍历所有远程NameNode，设置IPC地址并做合法性检查
      for (RemoteNameNodeInfo info : nns) {
        InetSocketAddress ipc = NameNode.getServiceAddress(info.getConfiguration(), true);
        Preconditions.checkArgument(ipc.getPort() > 0,
            "Active NameNode must have an IPC port configured. " + "Got address '%s'", ipc);
        info.setIpcAddress(ipc);
      }

      LOG.info("Will roll logs on active node every " +
          (logRollPeriodMs / 1000) + " seconds.");
    } else {
      LOG.info("Not going to trigger log rolls on active node because " +
          DFSConfigKeys.DFS_HA_LOGROLL_PERIOD_KEY + " is negative.");
    }
    
    // 从配置读取初始检查周期
    sleepTimeMs = conf.getTimeDuration(
        DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_KEY,
        DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_DEFAULT,
        TimeUnit.SECONDS, TimeUnit.MILLISECONDS);
    // 从配置读取最大退避等待时间
    long maxSleepTimeMsTemp = conf.getTimeDuration(
        DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_BACKOFF_MAX_KEY,
        DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_BACKOFF_MAX_DEFAULT,
        TimeUnit.SECONDS, TimeUnit.MILLISECONDS);
    // 如果最大退避时间小于初始时间，禁用退避
    if (maxSleepTimeMsTemp > 0 && maxSleepTimeMsTemp < sleepTimeMs) {
      LOG.warn("{} was configured to be {} ms, but this is less than {}."
              + "Disabling backoff when tailing edit logs.",
          DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_BACKOFF_MAX_KEY,
          maxSleepTimeMsTemp, DFSConfigKeys.DFS_HA_TAILEDITS_PERIOD_KEY);
      maxSleepTimeMs = 0;
    } else {
      maxSleepTimeMs = maxSleepTimeMsTemp;
    }

    // 读取日志滚动RPC超时配置
    rollEditsTimeoutMs = conf.getTimeDuration(
        DFSConfigKeys.DFS_HA_TAILEDITS_ROLLEDITS_TIMEOUT_KEY,
        DFSConfigKeys.DFS_HA_TAILEDITS_ROLLEDITS_TIMEOUT_DEFAULT,
        TimeUnit.SECONDS, TimeUnit.MILLISECONDS);

    // 创建单线程守护线程池执行日志滚动RPC
    rollEditsRpcExecutor = Executors.newSingleThreadExecutor(
        new ThreadFactoryBuilder().setDaemon(true).build());

    // 读取最大重试次数配置，非法值则重置为默认
    maxRetries = conf.getInt(DFSConfigKeys.DFS_HA_TAILEDITS_ALL_NAMESNODES_RETRY_KEY,
      DFSConfigKeys.DFS_HA_TAILEDITS_ALL_NAMESNODES_RETRY_DEFAULT);
    if (maxRetries <= 0) {
      LOG.error("Specified a non-positive number of retries for the number of retries for the " +
          "namenode connection when manipulating the edit log (" +
          DFSConfigKeys.DFS_HA_TAILEDITS_ALL_NAMESNODES_RETRY_KEY + "), setting to default: " +
          DFSConfigKeys.DFS_HA_TAILEDITS_ALL_NAMESNODES_RETRY_DEFAULT);
      maxRetries = DFSConfigKeys.DFS_HA_TAILEDITS_ALL_NAMESNODES_RETRY_DEFAULT;
    }

    // 读取是否允许拉取进行中日志的配置
    inProgressOk = conf.getBoolean(
        DFSConfigKeys.DFS_HA_TAILEDITS_INPROGRESS_KEY,
        DFSConfigKeys.DFS_HA_TAILEDITS_INPROGRESS_DEFAULT);

    // 读取每次锁最多加载事务数配置
    this.maxTxnsPerLock = conf.getLong(
        DFS_HA_TAILEDITS_MAX_TXNS_PER_LOCK_KEY,
        DFS_HA_TAILEDITS_MAX_TXNS_PER_LOCK_DEFAULT);

    nnCount = nns.size();
    // 创建循环迭代器， endless遍历所有远程NameNode
    this.nnLookup = Iterators.cycle(nns);
    LOG.debug("logRollPeriodMs={} sleepTime={}.", logRollPeriodMs, sleepTimeMs);
  }

  /**
   * 启动日志尾拖后台线程
   */
  public void start() {
    tailerThread.start();
  }
  
  /**
   * 停止日志尾拖后台线程，关闭线程池
   * @throws IOException 如果线程被中断则抛出IO异常
   */
  public void stop() throws IOException {
    tailerThread.setShouldRun(false);
    tailerThread.interrupt();
    try {
      tailerThread.join();
    } catch (InterruptedException e) {
      LOG.warn("Edit log tailer thread exited with an exception");
      throw new IOException(e);
    } finally {
      rollEditsRpcExecutor.shutdown();
    }
  }
  
  @VisibleForTesting
  FSEditLog getEditLog() {
    return editLog;
  }
  
  @VisibleForTesting
  public void setEditLog(FSEditLog editLog) {
    this.editLog = editLog;
  }

  /**
   * 故障转移切换过程中，追齐所有编辑日志到最新，保证切换后主NameNode元数据完整
   * @throws IOException 追齐过程中发生IO异常则抛出
   */
  public void catchupDuringFailover() throws IOException {
    Preconditions.checkState(tailerThread == null ||
        !tailerThread.isAlive(),
        "Tailer thread should not be running once failover starts");
    // 使用登录用户身份执行，兼容需要安全凭证访问共享存储的场景
    SecurityUtil.doAsLoginUser(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        long editsTailed = 0;
        // 持续拉取直到没有新日志，追齐到最新
        do {
          long startTime = timer.monotonicNow();
          try {
            NameNode.getNameNodeMetrics().addEditLogTailInterval(
                startTime - lastLoadTimeMs);
            // 已经持有命名空间锁，检查点线程已停止，无需额外加锁
            // HDFS-16689: 禁用进行中日志，使用流式机制加载
            editsTailed = doTailEdits(false);
          } catch (InterruptedException e) {
            throw new IOException(e);
          } finally {
            NameNode.getNameNodeMetrics().addEditLogTailTime(
                timer.monotonicNow() - startTime);
          }
        } while(editsTailed > 0);
        return null;
      }
    });
  }

  @VisibleForTesting
  public long doTailEdits() throws IOException, InterruptedException {
    return doTailEdits(inProgressOk);
  }

  /**
   * 实际执行拉取加载编辑日志的核心方法，从共享日志存储读取从上次加载位置之后的所有新日志，并应用到本地命名空间
   * @param enableInProgress 是否允许加载进行中的未完成日志段
   * @return 本次加载的事务数量
   * @throws IOException 读取或加载日志过程中发生异常则抛出
   * @throws InterruptedException 如果线程被中断则抛出
   */
  private long doTailEdits(boolean enableInProgress) throws IOException, InterruptedException {
    Collection<EditLogInputStream> streams;
    FSImage image = namesystem.getFSImage();

    // 获取加载前的最新事务ID
    long lastTxnId = image.getLastAppliedTxId();
    LOG.debug("lastTxnId: {}", lastTxnId);
    long startTime = timer.monotonicNow();
    try {
      // 从编辑日志选择从lastTxnId+1开始的所有输入流
      streams = editLog.selectInputStreams(lastTxnId + 1, 0,
          null, enableInProgress, true);
    } catch (IOException ioe) {
      // 日志滚动过程中可能出现找不到流的情况，属于正常，稍后重试即可
      LOG.warn("Edits tailer failed to find any streams. Will try again " +
          "later.", ioe);
      return 0;
    } finally {
      // 记录获取编辑日志流耗时到指标
      NameNode.getNameNodeMetrics().addEditLogFetchTime(
          timer.monotonicNow() - startTime);
    }
    // 获取可中断的全局写锁，避免故障转移时死锁
    namesystem.writeLockInterruptibly(RwLockMode.GLOBAL);
    try {
      long currentLastTxnId = image.getLastAppliedTxId();
      // 如果事务ID已经变化，说明其他线程修改了元数据，直接返回
      if (lastTxnId != currentLastTxnId) {
        LOG.warn("The currentLastTxnId({}) is different from preLastTxtId({})",
            currentLastTxnId, lastTxnId);
        return 0;
      }
      LOG.debug("edit streams to load from: {}.", streams.size());
      
      long editsLoaded = 0;
      try {
        // 加载所有选中的编辑日志到元数据，最多加载maxTxnsPerLock个事务
        editsLoaded = image.loadEdits(
            streams, namesystem, maxTxnsPerLock, null, null);
      } catch (EditLogInputException elie) {
        // 加载出错，保留已经加载的事务数后重新抛出异常
        editsLoaded = elie.getNumEditsLoaded();