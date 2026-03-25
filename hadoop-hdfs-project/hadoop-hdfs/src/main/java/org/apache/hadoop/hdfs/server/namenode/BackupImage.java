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

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.InconsistentFSStateException;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.common.Storage.StorageState;
import org.apache.hadoop.hdfs.util.RwLockMode;
import org.apache.hadoop.util.Lists;
import org.apache.hadoop.util.StringUtils;

import org.apache.hadoop.util.Preconditions;

/**
 * 文件系统镜像在BackupNode节点的扩展实现
 * 该类负责处理备份NameNode上日志缓存的初始化和管理，支持从主NameNode同步元数据
 */
@InterfaceAudience.Private
public class BackupImage extends FSImage {
  /** 用于将远程接收的 edits 加载到内存的备份输入流 */
  private final EditLogBackupInputStream backupInputStream =
    new EditLogBackupInputStream("Data from remote NameNode");
  
  /**
   * BackupNode的当前状态，状态转换逻辑如下：
   * 初始状态: DROP_UNTIL_NEXT_ROLL
   * - 在下一次日志滚动时转换为JOURNAL_ONLY
   * - 在convergeJournalSpool阶段转换为IN_SYNC
   * - 如果stopApplyingOnNextRoll为true，当日志滚动时从IN_SYNC转换回JOURNAL_ONLY
   */
  volatile BNState bnState;
  /** BackupNode状态枚举 */
  enum BNState {
    /**
     * 丢弃来自主NameNode的所有 edits，等待下一次日志滚动后转换到JOURNAL_ONLY
     */
    DROP_UNTIL_NEXT_ROLL,
    /**
     * 将来自主NameNode的edits写入本地日志，但不应用到本地命名空间
     */
    JOURNAL_ONLY,
    /**
     * 将来自主NameNode的edits写入本地日志，并同步应用到本地命名空间，保持和主NameNode一致
     */
    IN_SYNC;
  }

  /**
   * 标记下一次主NameNode日志滚动时，BackupNode应该转换到JOURNAL_ONLY状态
   * {@see #freezeNamespaceAtNextRoll()}
   */
  private boolean stopApplyingEditsOnNextRoll = false;
  
  private FSNamesystem namesystem;

  private int quotaInitThreads;

  /**
   * 构造BackupImage对象
   * @param conf Hadoop配置对象
   * @throws IOException 存储初始化失败时抛出异常
   */
  BackupImage(Configuration conf) throws IOException {
    super(conf);
    storage.setDisablePreUpgradableLayoutCheck(true);
    bnState = BNState.DROP_UNTIL_NEXT_ROLL;
  }

  synchronized FSNamesystem getNamesystem() {
    return namesystem;
  }

  synchronized void setNamesystem(FSNamesystem fsn) {
    // 避免覆盖已经设置的namesystem对象
    if (namesystem == null) {
      this.namesystem = fsn;
    }
  }

  /**
   * 分析备份存储目录一致性，必要时从不完整检查点恢复<br>
   * 读取VERSION和fstime文件（如果存在）<br>
   * 不加载镜像和edits日志
   *
   * @throws IOException 如果节点需要关闭则抛出异常
   */
  void recoverCreateRead() throws IOException {
    for (Iterator<StorageDirectory> it = storage.dirIterator(); it.hasNext();) {
      StorageDirectory sd = it.next();
      StorageState curState;
      try {
        curState = sd.analyzeStorage(HdfsServerConstants.StartupOption.REGULAR, storage);
        // sd已加锁但未打开
        switch(curState) {
        case NON_EXISTENT:
          // 任一配置的存储目录不可访问则直接失败
          throw new InconsistentFSStateException(sd.getRoot(),
                "checkpoint directory does not exist or is not accessible.");
        case NOT_FORMATTED:
          // 备份节点初始状态下所有目录都可能未格式化，直接格式化
          LOG.info("Storage directory " + sd.getRoot() + " is not formatted.");
          LOG.info("Formatting ...");
          sd.clearDirectory(); // 创建空的current目录
          break;
        case NORMAL:
          break;
        default:  // 其他状态需要恢复
          sd.doRecover(curState);
        }
        if(curState != StorageState.NOT_FORMATTED) {
          // 读取属性并和其他目录做一致性校验
          storage.readProperties(sd);
        }
      } catch(IOException ioe) {
        sd.unlock();
        throw ioe;
      }
    }
  }

  /**
   * 接收来自主NameNode的一批edits日志
   * 
   * 根据当前bnState执行不同处理，详见 {@link BackupImage.BNState}
   * 
   * @param firstTxId 批次中第一个事务ID
   * @param numTxns 批次包含的事务数量
   * @param data 序列化后的日志记录数据
   * @throws IOException IO异常
   * @see #convergeJournalSpool()
   */
  synchronized void journal(long firstTxId, int numTxns, byte[] data) throws IOException {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Got journal, " +
          "state = " + bnState +
          "; firstTxId = " + firstTxId +
          "; numTxns = " + numTxns);
    }
    
    switch(bnState) {
      case DROP_UNTIL_NEXT_ROLL:
        // 直接丢弃所有收到的edits
        return;

      case IN_SYNC:
        // 需要将edits应用到内存命名空间
        applyEdits(firstTxId, numTxns, data);
        break;
      
      case JOURNAL_ONLY:
        // 只写入本地日志，不应用到命名空间，直接跳过处理
        break;
      
      default:
        throw new AssertionError("Unhandled state: " + bnState);
    }
    
    // 无论哪种状态（除DROP）都需要写入备份节点本地edits日志
    editLog.journal(firstTxId, numTxns, data);
  }


  /**
   * 将接收的edits批次应用到本地命名空间
   */
  private synchronized void applyEdits(long firstTxId, int numTxns, byte[] data)
      throws IOException {
    // 校验事务连续性：收到的批次必须紧接着上一个应用完成的事务
    Preconditions.checkArgument(firstTxId == lastAppliedTxId + 1,
        "Received txn batch starting at %s but expected %s",
        firstTxId, lastAppliedTxId + 1);
    assert backupInputStream.length() == 0 : "backup input stream is not empty";
    try {
      if (LOG.isTraceEnabled()) {
        LOG.trace("data:" + StringUtils.byteToHexString(data));
      }

      // 创建edits加载器
      FSEditLogLoader logLoader =
          new FSEditLogLoader(getNamesystem(), lastAppliedTxId);
      int logVersion = storage.getLayoutVersion();
      // 将收到的字节数据设置到备份输入流
      backupInputStream.setBytes(data, logVersion);

      // 加载并应用edits记录到命名空间
      long numTxnsAdvanced = logLoader.loadEditRecords(
          backupInputStream, true, lastAppliedTxId + 1, null, null);
      if (numTxnsAdvanced != numTxns) {
        throw new IOException("Batch of txns starting at txnid " +
            firstTxId + " was supposed to contain " + numTxns +
            " transactions, but we were only able to advance by " +
            numTxnsAdvanced);
      }
      // 更新已应用的最后事务ID
      lastAppliedTxId = logLoader.getLastAppliedTxId();

      // 获取文件系统写锁，更新配额统计
      getNamesystem().writeLock(RwLockMode.FS);
      try {
        getNamesystem().dir.updateCountForQuota();
      } finally {
        getNamesystem().writeUnlock(RwLockMode.FS, "applyEdits");
      }
    } finally {
      // 清空输入流，准备下一次使用
      backupInputStream.clear();
    }
  }

  /**
   * 将BackupNode从JOURNAL_ONLY状态转换到IN_SYNC状态
   * 通过重复调用tryConvergeJournalSpool直到追上主NameNode最新的edits
   */
  void convergeJournalSpool() throws IOException {
    Preconditions.checkState(bnState == BNState.JOURNAL_ONLY,
        "bad state: %s", bnState);

    while (!tryConvergeJournalSpool()) {
      ;
    }
    assert bnState == BNState.IN_SYNC;
  }
  
  /**
   * 尝试一次追赶，将已缓存的edits应用到本地命名空间，直到追上最新进度
   * @return true表示已完成追赶进入IN_SYNC，false表示需要重试
   * @throws IOException IO异常
   */
  private boolean tryConvergeJournalSpool() throws IOException {
    Preconditions.checkState(bnState == BNState.JOURNAL_ONLY,
        "bad state: %s", bnState);
    
    // 此处不需要同步，因为当前状态是JOURNAL_ONLY，lastAppliedTxId不会变化
    // curSegmentTxId只会递增，所以并发写入不影响读取

    // 循环应用已归档的edits文件
    while (lastAppliedTxId < editLog.getCurSegmentTxId() - 1) {
      long target = editLog.getCurSegmentTxId();
      LOG.info("Loading edits into backupnode to try to catch up from txid "
          + lastAppliedTxId + " to " + target);
      FSImageTransactionalStorageInspector inspector =
        new FSImageTransactionalStorageInspector();
      
      // 检查存储目录获取镜像信息
      storage.inspectStorageDirs(inspector);

      // 恢复未关闭的流
      editLog.recoverUnclosedStreams();
      // 获取从lastAppliedTxId开始到目标txid的所有输入流
      Iterable<EditLogInputStream> editStreamsAll 
        = editLog.selectInputStreams(lastAppliedTxId, target - 1);
      // 过滤掉当前正在写入的分段文件，只处理已归档的
      List<EditLogInputStream> editStreams = Lists.newArrayList();
      for (EditLogInputStream s : editStreamsAll) {
        if (s.getFirstTxId() != editLog.getCurSegmentTxId()) {
          editStreams.add(s);
        }
      }
      // 加载应用所有归档edits
      loadEdits(editStreams, getNamesystem());
    }
    
    // 处理当前正在写入的in-progress分段文件
    synchronized (this) {
      if (lastAppliedTxId != editLog.getCurSegmentTxId() - 1) {
        // 追赶过程中日志发生滚动，需要重试
        LOG.debug("Logs rolled while catching up to current segment");
        return false;
      }
      
      EditLogInputStream stream = null;
      // 获取当前分段的输入流
      Collection<EditLogInputStream> editStreams
        = getEditLog().selectInputStreams(
            getEditLog().getCurSegmentTxId(),
            getEditLog().getCurSegmentTxId());
      
      for (EditLogInputStream s : editStreams) {
        if (s.getFirstTxId() == getEditLog().getCurSegmentTxId()) {
          stream = s;
        }
        break;
      }
      if (stream == null) {
        LOG.warn("Unable to find stream starting with " + editLog.getCurSegmentTxId()
                 + ". This indicates that there is an error in synchronization in BackupImage");
        return false;
      }

      try {
        // 计算剩余需要应用的事务数量
        long remainingTxns = getEditLog().getLastWrittenTxId() - lastAppliedTxId;
        
        LOG.info("Going to finish converging with remaining " + remainingTxns
            + " txns from in-progress stream " + stream);
        
        // 加载应用剩余的in-progress edits
        FSEditLogLoader loader =
            new FSEditLogLoader(getNamesystem(), lastAppliedTxId);
        loader.loadFSEdits(stream, lastAppliedTxId + 1);
        lastAppliedTxId = loader.getLastAppliedTxId();
        assert lastAppliedTxId == getEditLog().getLastWrittenTxId();
      } finally {
        // 关闭所有打开的输入流
        FSEditLog.closeAllStreams(editStreams);
      }

      LOG.info("Successfully synced BackupNode with NameNode at txnid " +
          lastAppliedTxId);
      // 转换状态为IN_SYNC，完成同步
      setState(BNState.IN_SYNC);
    }
    return true;
  }

  /**
   * 转换BackupNode状态，记录状态转换日志
   */
  private synchronized void setState(BNState newState) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("State transition " + bnState + " -> " + newState);
    }
    bnState = newState;
  }

  /**
   * 接收主NameNode已经开始新日志分段的通知，BackupNode也在本地目录启动新的日志分段
   * @param txid 新日志分段起始事务ID
   * @throws IOException IO异常
   */
  synchronized void namenodeStartedLogSegment(long txid) throws IOException {
    // 在本地启动新的日志分段
    editLog.startLogSegment(txid, true, namesystem.getEffectiveLayoutVersion());

    if (bnState == BNState.DROP_UNTIL_NEXT_ROLL) {
      // 首次滚动日志，从DROP状态转换到JOURNAL_ONLY
      setState(BNState.JOURNAL_ONLY);
    }
    
    if (stopApplyingEditsOnNextRoll) {
      if (bnState == BNState.IN_SYNC) {
        // 需要冻结命名空间，从IN_SYNC转换到JOURNAL_ONLY
        LOG.info("Stopped applying edits to prepare for checkpoint.");
        setState(BNState.JOURNAL_ONLY);
      }
      stopApplyingEditsOnNextRoll = false;
      notifyAll();
    }
  }

  /**
   * 设置标记：下一次BackupNode收到日志滚动通知时，停止将edits应用到本地命名空间
   * 该方法通常之后会调用 {@link #waitUntilNamespaceFrozen()}
   */
  synchronized void freezeNamespaceAtNextRoll() {
    stopApplyingEditsOnNextRoll = true;
  }

  /**
   * 在调用{@link #freezeNamespaceAtNextRoll()}后，等待直到BackupNode收到下一次日志滚动
   * 完成命名空间冻结
   * @throws IOException 等待被中断时抛出异常
   */
  synchronized void waitUntilNamespaceFrozen() throws IOException {
    if (bnState != BNState.IN_SYNC) return;

    LOG.info("Waiting until the NameNode rolls its edit logs in order " +
        "to freeze the BackupNode namespace.");
    while (bnState == BNState.IN_SYNC) {
      Preconditions.checkState(stopApplyingEditsOnNextRoll,
        "If still in sync, we should still have the flag set to " +
        "freeze at next roll");
      try {
        // 等待状态转换通知
        wait();
      } catch (InterruptedException ie) {
        LOG.warn("Interrupted waiting for namespace to freeze", ie);
        throw new IOException(ie);
      }
    }
    LOG.info("BackupNode namespace frozen.");
  }

  /**
   * 重写close方法，不需要完成当前edits日志分段，直接终止
   */
  @Override
  public synchronized void close() throws IOException {
    editLog.abortCurrentLogSegment();
    storage.close();
  }
}