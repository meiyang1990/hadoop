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

import static org.apache.hadoop.hdfs.server.namenode.FSImageFormat.renameReservedPathsOnUpgrade;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockIdManager;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoStriped;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LastBlockWithStatus;
import org.apache.hadoop.hdfs.protocol.LayoutVersion;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.BlockUCState;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.RollingUpgradeStartupOption;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory.DirOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.AddBlockOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.AddCacheDirectiveInfoOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.AddCachePoolOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.AddCloseOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.AllocateBlockIdOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.AllowSnapshotOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.AppendOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.BlockListUpdatingOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.CancelDelegationTokenOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.ClearNSQuotaOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.ConcatDeleteOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.CreateSnapshotOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.DeleteOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.DeleteSnapshotOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.DisallowSnapshotOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.GetDelegationTokenOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.MkdirOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.ModifyCacheDirectiveInfoOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.ModifyCachePoolOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.ReassignLeaseOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.RemoveCacheDirectiveInfoOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.RemoveCachePoolOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.RemoveXAttrOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.RenameOldOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.RenameOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.RenameSnapshotOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.RenewDelegationTokenOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.RollingUpgradeOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.SetAclOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.SetGenstampV1Op;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.SetGenstampV2Op;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.SetNSQuotaOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.SetOwnerOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.SetPermissionsOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.SetQuotaOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.SetReplicationOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.SetStoragePolicyOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.SetXAttrOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.SymlinkOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.TimesOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.TruncateOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.UpdateBlocksOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp.UpdateMasterKeyOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp
    .AddErasureCodingPolicyOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp
    .RemoveErasureCodingPolicyOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp
    .EnableErasureCodingPolicyOp;
import org.apache.hadoop.hdfs.server.namenode.FSEditLogOp
    .DisableErasureCodingPolicyOp;
import org.apache.hadoop.hdfs.server.namenode.INode.BlocksMapUpdateInfo;
import org.apache.hadoop.hdfs.server.namenode.LeaseManager.Lease;
import org.apache.hadoop.hdfs.server.namenode.NNStorage.NameNodeFile;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.Phase;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgress;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgress.Counter;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.Step;
import org.apache.hadoop.hdfs.util.Holder;
import org.apache.hadoop.hdfs.util.RwLockMode;
import org.apache.hadoop.log.LogThrottlingHelper;
import org.apache.hadoop.util.ChunkedArrayList;
import org.apache.hadoop.util.Timer;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;
import org.apache.hadoop.util.Preconditions;

import static org.apache.hadoop.log.LogThrottlingHelper.LogAction;

/**
 * FSEditLog加载器，负责NameNode启动时回放编辑日志（edit log），将所有事务操作应用到内存元数据结构
 * 核心职责：在NameNode启动时，加载FsImage后回放所有未合并到FsImage的编辑日志，重建内存元数据
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class FSEditLogLoader {
  static final Logger LOG =
      LoggerFactory.getLogger(FSEditLogLoader.class.getName());
  static final long REPLAY_TRANSACTION_LOG_INTERVAL = 1000; // 1sec

  /** Limit logging about edit loading to every 5 seconds max. */
  @VisibleForTesting
  static final long LOAD_EDIT_LOG_INTERVAL_MS = 5000;
  @VisibleForTesting
  static final LogThrottlingHelper LOAD_EDITS_LOG_HELPER =
      new LogThrottlingHelper(LOAD_EDIT_LOG_INTERVAL_MS);

  private final FSNamesystem fsNamesys;
  private final BlockManager blockManager;
  private final Timer timer;
  private long lastAppliedTxId;
  /** Total number of end transactions loaded. */
  private int totalEdits = 0;
  
  /**
   * 构造FSEditLogLoader，使用默认定时器
   * @param fsNamesys FSNamesystem对象，目标内存元数据系统
   * @param lastAppliedTxId 已经应用的最后一个事务ID
   */
  public FSEditLogLoader(FSNamesystem fsNamesys, long lastAppliedTxId) {
    this(fsNamesys, lastAppliedTxId, new Timer());
  }

  /**
   * 构造FSEditLogLoader，可指定定时器（用于测试）
   * @param fsNamesys FSNamesystem对象，目标内存元数据系统
   * @param lastAppliedTxId 已经应用的最后一个事务ID
   * @param timer 定时器对象
   */
  @VisibleForTesting
  FSEditLogLoader(FSNamesystem fsNamesys, long lastAppliedTxId, Timer timer) {
    this.fsNamesys = fsNamesys;
    this.blockManager = fsNamesys.getBlockManager();
    this.lastAppliedTxId = lastAppliedTxId;
    this.timer = timer;
  }
  
  long loadFSEdits(EditLogInputStream edits, long expectedStartingTxId)
      throws IOException {
    return loadFSEdits(edits, expectedStartingTxId, Long.MAX_VALUE, null, null);
  }

  /**
   * 加载编辑日志文件，并将所有操作应用到内存元数据结构
   * 负责启动加载流程、管理锁和日志输出，最终返回加载的事务总数
   * @param edits 编辑日志输入流
   * @param expectedStartingTxId 期望起始事务ID
   * @param maxTxnsToRead 最大读取事务数
   * @param startOpt NameNode启动选项
   * @param recovery 元数据恢复上下文，处理损坏日志时的恢复策略
   * @return 加载的事务总数
   * @throws IOException 加载过程中发生IO错误或不可恢复的损坏
   */
  long loadFSEdits(EditLogInputStream edits, long expectedStartingTxId,
      long maxTxnsToRead,
      StartupOption startOpt, MetaRecoveryContext recovery) throws IOException {
    StartupProgress prog = NameNode.getStartupProgress();
    Step step = createStartupProgressStep(edits);
    prog.beginStep(Phase.LOADING_EDITS, step);
    fsNamesys.writeLock(RwLockMode.GLOBAL);
    try {
      long startTime = timer.monotonicNow();
      LogAction preLogAction = LOAD_EDITS_LOG_HELPER.record("pre", startTime);
      if (preLogAction.shouldLog()) {
        FSImage.LOG.info("Start loading edits file " + edits.getName()
            + " maxTxnsToRead = " + maxTxnsToRead +
            LogThrottlingHelper.getLogSupressionMessage(preLogAction));
      }
      long numEdits = loadEditRecords(edits, false, expectedStartingTxId,
          maxTxnsToRead, startOpt, recovery);
      long endTime = timer.monotonicNow();
      LogAction postLogAction = LOAD_EDITS_LOG_HELPER.record("post", endTime,
          numEdits, edits.length(), endTime - startTime);
      if (postLogAction.shouldLog()) {
        FSImage.LOG.info("Loaded {} edits file(s) (the last named {}) of " +
                "total size {}, total edits {}, total load time {} ms",
            postLogAction.getCount(), edits.getName(),
            postLogAction.getStats(1).getSum(),
            postLogAction.getStats(0).getSum(),
            postLogAction.getStats(2).getSum());
      }
      return numEdits;
    } finally {
      edits.close();
      fsNamesys.writeUnlock(RwLockMode.GLOBAL, "loadFSEdits");
      prog.endStep(Phase.LOADING_EDITS, step);
    }
  }

  long loadEditRecords(EditLogInputStream in, boolean closeOnExit,
      long expectedStartingTxId, StartupOption startOpt,
      MetaRecoveryContext recovery) throws IOException {
    return loadEditRecords(in, closeOnExit, expectedStartingTxId,
        Long.MAX_VALUE, startOpt, recovery);
  }

  /**
   * 逐条加载编辑日志中的事务记录，并逐个应用到内存元数据
   * 处理日志损坏、事务序号不连续等异常情况，支持元数据恢复
   * @param in 编辑日志输入流
   * @param closeOnExit 加载完成后是否关闭输入流
   * @param expectedStartingTxId 期望起始事务ID
   * @param maxTxnsToRead 最大读取事务数
   * @param startOpt NameNode启动选项
   * @param recovery 元数据恢复上下文
   * @return 成功加载的事务总数
   * @throws IOException 读取或应用操作发生错误且无法恢复
   */
  long loadEditRecords(EditLogInputStream in, boolean closeOnExit,
      long expectedStartingTxId, long maxTxnsToRead, StartupOption startOpt,
      MetaRecoveryContext recovery) throws IOException {
    EnumMap<FSEditLogOpCodes, Holder<Integer>> opCounts =
      new EnumMap<FSEditLogOpCodes, Holder<Integer>>(FSEditLogOpCodes.class);

    if (LOG.isTraceEnabled()) {
      LOG.trace("Acquiring write lock to replay edit log");
    }

    fsNamesys.writeLock(RwLockMode.GLOBAL);
    FSDirectory fsDir = fsNamesys.dir;
    fsDir.writeLock();

    // 保存最近几个操作的偏移量，出错时用于辅助定位问题
    long recentOpcodeOffsets[] = new long[4];
    Arrays.fill(recentOpcodeOffsets, -1);
    
    long expectedTxId = expectedStartingTxId;
    long numEdits = 0;
    long lastTxId = in.getLastTxId();
    long numTxns = (lastTxId - expectedStartingTxId) + 1;
    StartupProgress prog = NameNode.getStartupProgress();
    Step step = createStartupProgressStep(in);
    prog.setTotal(Phase.LOADING_EDITS, step, numTxns);
    Counter counter = prog.getCounter(Phase.LOADING_EDITS, step);
    long lastLogTime = timer.monotonicNow();
    long lastInodeId = fsNamesys.dir.getLastInodeId();
    
    try {
      while (true) {
        try {
          FSEditLogOp op;
          try {
            op = in.readOp();
            if (op == null) {
              break;
            }
          } catch (Throwable e) {
            // 检查是否是203版本升级导致的已知冲突
            check203UpgradeFailure(in.getVersion(true), e);
            String errorMessage =
              formatEditLogReplayError(in, recentOpcodeOffsets, expectedTxId);
            FSImage.LOG.error(errorMessage, e);
            if (recovery == null) {
               // 仅在恢复模式下跳过损坏操作，非恢复模式直接抛出异常
              throw new EditLogInputException(errorMessage, e, numEdits);
            }
            MetaRecoveryContext.editLogLoaderPrompt(
                "We failed to read txId " + expectedTxId,
                recovery, "skipping the bad section in the log");
            in.resync();
            continue;
          }
          // 记录最近操作的偏移量，方便错误定位
          recentOpcodeOffsets[(int)(numEdits % recentOpcodeOffsets.length)] =
            in.getPosition();
          if (op.hasTransactionId()) {
            if (op.getTransactionId() > expectedTxId) { 
              // 事务ID不连续，存在空洞，提示用户并继续
              MetaRecoveryContext.editLogLoaderPrompt("There appears " +
                  "to be a gap in the edit log.  We expected txid " +
                  expectedTxId + ", but got txid " +
                  op.getTransactionId() + ".", recovery, "ignoring missing " +
                  " transaction IDs");
            } else if (op.getTransactionId() < expectedTxId) { 
              // 事务ID乱序，跳过已经处理过的事务
              MetaRecoveryContext.editLogLoaderPrompt("There appears " +
                  "to be an out-of-order edit in the edit log.  We " +
                  "expected txid " + expectedTxId + ", but got txid " +
                  op.getTransactionId() + ".", recovery,
                  "skipping the out-of-order edit");
              continue;
            }
          }