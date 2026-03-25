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

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_IP_PROXY_USERS;
import static org.apache.hadoop.util.ExitUtil.terminate;
import static org.apache.hadoop.util.Time.monotonicNow;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.LongAdder;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.XAttr;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NamenodeRole;
import org.apache.hadoop.hdfs.server.common.Storage.FormatConfirmable;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.AddBlockOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.AddCacheDirectiveInfoOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.AddCachePoolOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.AddOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.AllocateBlockIdOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.AllowSnapshotOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.AppendOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.CancelDelegationTokenOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.CloseOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.ConcatDeleteOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.CreateSnapshotOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.DeleteOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.DeleteSnapshotOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.DisallowSnapshotOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.GetDelegationTokenOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.LogSegmentOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.MkdirOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.ModifyCacheDirectiveInfoOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.ModifyCachePoolOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.OpInstanceCache;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.ReassignLeaseOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.RemoveCacheDirectiveInfoOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.RemoveCachePoolOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.RemoveXAttrOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.RenameOldOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.RenameOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.RenameSnapshotOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.RenewDelegationTokenOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.RollingUpgradeFinalizeOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.RollingUpgradeOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.RollingUpgradeStartOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.SetAclOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.SetGenstampV1Op;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.SetGenstampV2Op;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.SetOwnerOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.SetPermissionsOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.SetQuotaOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.SetQuotaByStorageTypeOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.SetReplicationOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.SetStoragePolicyOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.SetXAttrOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.SymlinkOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.TimesOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.TruncateOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.UpdateBlocksOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.UpdateMasterKeyOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.AddErasureCodingPolicyOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.EnableErasureCodingPolicyOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.DisableErasureCodingPolicyOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.RemoveErasureCodingPolicyOp;
import org.apache.hadoop.hdfs.server.namenode.JournalSet.JournalAndStream;
import org.apache.hadoop.hdfs.server.namenode.metrics.NameNodeMetrics;
import org.apache.hadoop.hdfs.server.protocol.NamenodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.util.Lists;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 文件级注释：FSEditLog是HDFS NameNode的编辑日志管理器，负责维护文件系统命名空间所有修改操作的日志记录
 * 用于NameNode重启时重新应用日志恢复命名空间修改，保证数据持久性和一致性
 * FSEditLog maintains a log of the namespace modifications.
 * 
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class FSEditLog implements LogsPurgeable {
  public static final Logger LOG = LoggerFactory.getLogger(FSEditLog.class);
  /**
   * 编辑日志状态机，描述编辑日志在不同场景下的运行状态
   * 
   * 非HA场景:
   * 日志创建后初始为UNINITIALIZED状态，初始化完成后通常处于IN_SEGMENT状态，表示可以写入新操作
   * 在日志滚动或保存命名空间的过程中，会短暂进入BETWEEN_LOG_SEGMENTS状态，表示前一个段已关闭，新段尚未打开
   * 
   * HA场景:
   * 日志创建后初始为UNINITIALIZED状态，初始化完成后，当NameNode处于Standby状态时，日志一直处于OPEN_FOR_READING状态
   * 当NameNode切换为Active时，日志会先变为CLOSED，然后切换到BETWEEN_LOG_SEGMENTS，之后进入IN_SEGMENT状态开始写入
   * 后续状态变化和非HA场景一致
   */
  private enum State {
    UNINITIALIZED,
    BETWEEN_LOG_SEGMENTS,
    IN_SEGMENT,
    OPEN_FOR_READING,
    CLOSED;
  }  
  private State state = State.UNINITIALIZED;
  
  // 日志集管理对象，管理多个日志存储节点的journal
  private JournalSet journalSet = null;

  @VisibleForTesting
  EditLogOutputStream editLogStream = null;

  // 单调递增的事务ID计数器，所有更新操作都在同步块中修改，因此使用volatile保证可见性即可
  private volatile long txid = 0;

  // 记录已经同步到磁盘的最后一个事务ID
  private long synctxid = 0;

  // 当前打开写入的日志段起始事务ID，如果值为N，表示当前正在写入edits_inprogress_N
  private volatile long curSegmentTxId = HdfsServerConstants.INVALID_TXID;

  // 上一次打印统计信息到日志的时间
  private long lastPrintTime;

  // 当前是否有同步操作正在执行
  private volatile boolean isSyncRunning;

  // 是否已经调度了自动同步
  private volatile boolean isAutoSyncScheduled = false;
  
  // 统计计数器：总事务数
  private long numTransactions;
  // 同步中批量处理的事务数
  private final LongAdder numTransactionsBatchedInSync = new LongAdder();
  // 所有事务处理总耗时
  private long totalTimeTransactions;
  // 指标统计对象
  private NameNodeMetrics metrics;

  private final NNStorage storage;
  private final Configuration conf;

  // 编辑日志存储目录URI列表
  private final List<URI> editsDirs;

  protected final OpInstanceCache cache = new OpInstanceCache();

  // 允许代理客户端IP的用户列表
  private final String[] ipProxyUsers;

  /**
   * 主备NameNode共享的编辑日志目录列表
   */
  private final List<URI> sharedEditsDirs;

  /**
   * 添加journal或关闭JournalSet时需要获取的锁，保证selectInputStreams调用时不会并发修改JournalSet
   */
  private final Object journalSetLock = new Object();

  /**
   * 线程本地事务ID封装类，用于存储每个线程当前的事务ID
   */
  private static class TransactionId {
    public long txid;

    TransactionId(long value) {
      this.txid = value;
    }
  }

  // 存储当前线程最新事务ID的ThreadLocal对象
  private static final ThreadLocal<TransactionId> myTransactionId = new ThreadLocal<TransactionId>() {
    @Override
    protected synchronized TransactionId initialValue() {
      // 如果RPC调用没有产生任何事务，logSync应该直接退出不执行同步，因此初始值为0
      return new TransactionId(0L);
    }
  };

  /**
   * 根据配置创建同步或异步编辑日志实例
   * @param conf 配置对象
   * @param storage NameNode存储对象
   * @param editsDirs 编辑日志目录列表
   * @return FSEditLog实例，异步场景返回FSEditLogAsync
   */
  static FSEditLog newInstance(Configuration conf, NNStorage storage,
      List<URI> editsDirs) {
    boolean asyncEditLogging = conf.getBoolean(
        DFSConfigKeys.DFS_NAMENODE_EDITS_ASYNC_LOGGING,
        DFSConfigKeys.DFS_NAMENODE_EDITS_ASYNC_LOGGING_DEFAULT);
    LOG.info("Edit logging is async:" + asyncEditLogging);
    return asyncEditLogging
        ? new FSEditLogAsync(conf, storage, editsDirs)
        : new FSEditLog(conf, storage, editsDirs);
  }

  /**
   * 构造FSEditLog对象，仅初始化基础属性，不会打开流，需要调用open()后才能使用
   * 
   * @param conf NameNode配置对象
   * @param storage NameNode存储对象
   * @param editsDirs 要使用的journal目录列表
   */
  FSEditLog(Configuration conf, NNStorage storage, List<URI> editsDirs) {
    ipProxyUsers = conf.getStrings(DFS_NAMENODE_IP_PROXY_USERS);
    isSyncRunning = false;
    this.conf = conf;
    this.storage = storage;
    metrics = NameNode.getNameNodeMetrics();
    lastPrintTime = monotonicNow();
     
    // 如果列表为空，第一次使用编辑日志时会报错，因为没有可用的journal
    this.editsDirs = Lists.newArrayList(editsDirs);

    this.sharedEditsDirs = FSNamesystem.getSharedEditsDirs(conf);
  }
  
  /**
   * 初始化journal用于写入操作
   */
  public synchronized void initJournalsForWrite() {
    Preconditions.checkState(state == State.UNINITIALIZED ||
        state == State.CLOSED, "Unexpected state: %s", state);
    
    initJournals(this.editsDirs);
    state = State.BETWEEN_LOG_SEGMENTS;
  }
  
  /**
   * 初始化共享journal用于读取操作（Standby NameNode场景）
   */
  public synchronized void initSharedJournalsForRead() {
    if (state == State.OPEN_FOR_READING) {
      LOG.warn("Initializing shared journals for READ, already open for READ",
          new Exception());
      return;
    }
    Preconditions.checkState(state == State.UNINITIALIZED ||
        state == State.CLOSED);
    
    initJournals(this.sharedEditsDirs);
    state = State.OPEN_FOR_READING;
  }
  
  /**
   * 初始化指定目录列表的journal
   */
  private synchronized void initJournals(List<URI> dirs) {
    int minimumRedundantJournals = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_MINIMUM_KEY,
        DFSConfigKeys.DFS_NAMENODE_EDITS_DIR_MINIMUM_DEFAULT);

    synchronized(journalSetLock) {
      journalSet = new JournalSet(minimumRedundantJournals);

      for (URI u : dirs) {
        boolean required = FSNamesystem.getRequiredNamespaceEditsDirs(conf)
            .contains(u);
        if (u.getScheme().equals(NNStorage.LOCAL_URI_SCHEME)) {
          // 本地文件存储类型的journal
          StorageDirectory sd = storage.getStorageDirectory(u);
          if (sd != null) {
            journalSet.add(new FileJournalManager(conf, sd, storage),
                required, sharedEditsDirs.contains(u));
          }
        } else {
          // 自定义扩展journal，从配置中获取实现类
          journalSet.add(createJournal(u), required,
              sharedEditsDirs.contains(u));
        }
      }
    }
 
    if (journalSet.isEmpty()) {
      LOG.error("No edits directories configured!");
    } 
  }

  /**
   * 获取编辑日志当前使用的所有存储URI列表
   * @return 当前编辑日志使用的URI集合
   */
  Collection<URI> getEditURIs() {
    return editsDirs;
  }

  /**
   * 初始化输出流，打开第一个日志段用于写入
   * @throws IOException IO异常
   */
  synchronized void openForWrite(int layoutVersion) throws IOException {
    Preconditions.checkState(state == State.BETWEEN_LOG_SEGMENTS,
        "Bad state: %s", state);

    long segmentTxId = getLastWrittenTxId() + 1;
    // 安全检查：如果已经存在比起始txid更新的可读日志，不能开始写入
    List<EditLogInputStream> streams = new ArrayList<EditLogInputStream>();
    journalSet.selectInputStreams(streams, segmentTxId, true, false);
    if (!streams.isEmpty()) {
      String error = String.format("Cannot start writing at txid %s " +
        "when there is a stream available for read: %s",
        segmentTxId, streams.get(0));
      IOUtils.cleanupWithLogger(LOG,
          streams.toArray(new EditLogInputStream[0]));
      throw new IllegalStateException(error);
    }
    
    startLogSegmentAndWriteHeaderTxn(segmentTxId, layoutVersion);
    assert state == State.IN_SEGMENT : "Bad state: " + state;
  }
  
  /**
   * 检查日志当前是否处于可写入模式，无论是否有打开的段都返回true
   * @return true表示日志已打开可写入