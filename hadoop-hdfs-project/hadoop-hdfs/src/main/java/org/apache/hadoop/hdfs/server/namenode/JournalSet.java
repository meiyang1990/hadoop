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

import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants.INVALID_TXID;
import static org.apache.hadoop.util.ExitUtil.terminate;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CopyOnWriteArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLog;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;
import org.apache.hadoop.util.Lists;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.util.Preconditions;

/**
 * 文件级注释：JournalSet是HDFS NameNode中管理多个JournalManager的集合类，
 * 负责协调多个日志存储实例（本地磁盘、QJM共享存储等）的读写操作，
 * 提供冗余日志管理、错误处理和一致性保证，支持多副本日志写入和读取。
 * 所有方法不做同步，由调用方FSEditLog保证线程安全。
 * Manages a collection of Journals. None of the methods are synchronized, it is
 * assumed that FSEditLog methods, that use this class, use proper
 * synchronization.
 */
@InterfaceAudience.Private
public class JournalSet implements JournalManager {

  static final Logger LOG = LoggerFactory.getLogger(FSEditLog.class);

  // 优先返回本地日志，比较器反转排序，让本地日志排在前面
  private static final Comparator<EditLogInputStream>
      LOCAL_LOG_PREFERENCE_COMPARATOR = Comparator
      .comparing(EditLogInputStream::isLocalLog)
      .reversed();

  public static final Comparator<EditLogInputStream>
      EDIT_LOG_INPUT_STREAM_COMPARATOR = Comparator
      .comparing(EditLogInputStream::getFirstTxId)
      .thenComparing(EditLogInputStream::getLastTxId);

  /**
   * 容器类，绑定JournalManager及其当前活跃输出流，
   * 当写入出错时会禁用该Journal，并将流置为null。
   * Container for a JournalManager paired with its currently
   * active stream.
   * 
   * If a Journal gets disabled due to an error writing to its
   * stream, then the stream will be aborted and set to null.
   */
  static class JournalAndStream implements CheckableNameNodeResource {
    private JournalManager journal;
    private boolean disabled = false;
    private EditLogOutputStream stream;
    private final boolean required;
    private final boolean shared;
    
    public JournalAndStream(JournalManager manager, boolean required,
        boolean shared) {
      this.journal = manager;
      this.required = required;
      this.shared = shared;
    }

    public void startLogSegment(long txId, int layoutVersion) throws IOException {
      Preconditions.checkState(stream == null);
      disabled = false;
      stream = journal.startLogSegment(txId, layoutVersion);
    }

    /**
     * Closes the stream, also sets it to null.
     */
    public void closeStream() throws IOException {
      if (stream == null) return;
      stream.close();
      stream = null;
    }

    /**
     * Close the Journal and Stream
     */
    public void close() throws IOException {
      closeStream();

      journal.close();
    }
    
    /**
     * Aborts the stream, also sets it to null.
     */
    public void abort() {
      if (stream == null) return;
      try {
        stream.abort();
      } catch (IOException ioe) {
        LOG.error("Unable to abort stream " + stream, ioe);
      }
      stream = null;
    }

    boolean isActive() {
      return stream != null;
    }
    
    /**
     * Should be used outside JournalSet only for testing.
     */
    EditLogOutputStream getCurrentStream() {
      return stream;
    }
    
    @Override
    public String toString() {
      return "JournalAndStream(mgr=" + journal +
        ", " + "stream=" + stream + ")";
    }

    void setCurrentStreamForTests(EditLogOutputStream stream) {
      this.stream = stream;
    }

    @VisibleForTesting
    void setJournalForTests(JournalManager jm) {
      this.journal = jm;
    }

    JournalManager getManager() {
      return journal;
    }

    boolean isDisabled() {
      return disabled;
    }

    private void setDisabled(boolean disabled) {
      this.disabled = disabled;
    }
    
    @Override
    public boolean isResourceAvailable() {
      return !isDisabled();
    }
    
    @Override
    public boolean isRequired() {
      return required;
    }
    
    public boolean isShared() {
      return shared;
    }
  }
 
  // 使用CopyOnWriteArrayList保证遍历安全，适合读多写少场景，Web UI等遍历操作不会抛出并发修改异常
  private final List<JournalAndStream> journals =
      new CopyOnWriteArrayList<JournalSet.JournalAndStream>();
  // 最小可用冗余日志数，满足该数量才能继续提供服务
  final int minimumRedundantJournals;

  private boolean closed;
  // 最后写入日志的事务ID
  private long lastJournalledTxId;

  /**
   * 构造JournalSet，指定最小需要的冗余日志数量
   * @param minimumRedundantResources 最小可用冗余日志数
   */
  JournalSet(int minimumRedundantResources) {
    this.minimumRedundantJournals = minimumRedundantResources;
    lastJournalledTxId = INVALID_TXID;
  }
  
  @Override
  public void format(NamespaceInfo nsInfo, boolean force) throws IOException {
    // 格式化操作由上层FSEditLog统一处理，本方法不支持
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean hasSomeData() throws IOException {
    // 判断是否有数据操作会逐个下发到底层Journal，本方法不支持
    throw new UnsupportedOperationException();
  }

  
  @Override
  public EditLogOutputStream startLogSegment(final long txId,
      final int layoutVersion) throws IOException {
    // 遍历所有Journal启动新日志段，处理错误
    mapJournalsAndReportErrors(new JournalClosure() {
      @Override
      public void apply(JournalAndStream jas) throws IOException {
        jas.startLogSegment(txId, layoutVersion);
      }
    }, "starting log segment " + txId);
    return new JournalSetOutputStream();
  }
  
  @Override
  public void finalizeLogSegment(final long firstTxId, final long lastTxId)
      throws IOException {
    // 遍历所有Journal结束当前日志段
    mapJournalsAndReportErrors(new JournalClosure() {
      @Override
      public void apply(JournalAndStream jas) throws IOException {
        if (jas.isActive()) {
          jas.closeStream();
          jas.getManager().finalizeLogSegment(firstTxId, lastTxId);
        }
      }
    }, "finalize log segment " + firstTxId + ", " + lastTxId);
  }
   
  @Override
  public void close() throws IOException {
    // 关闭所有Journal
    mapJournalsAndReportErrors(new JournalClosure() {
      @Override
      public void apply(JournalAndStream jas) throws IOException {
        jas.close();
      }
    }, "close journal");
    closed = true;
  }

  public boolean isOpen() {
    return !closed;
  }

  /**
   * 从所有JournalManager收集符合条件的输入流，整理后输出给调用者
   * 
   * @param streams          用于存放结果流的集合
   * @param fromTxId         起始事务ID，只返回包含该ID之后的流
   * @param inProgressOk     是否允许返回未结束的正在写入的流
   * @param onlyDurableTxns  是否只返回已经持久化完成的事务对应的流
   */
  @Override
  public void selectInputStreams(Collection<EditLogInputStream> streams,
      long fromTxId, boolean inProgressOk, boolean onlyDurableTxns) {
    // 使用优先队列按事务ID排序所有收集到的流
    final PriorityQueue<EditLogInputStream> allStreams = 
        new PriorityQueue<EditLogInputStream>(64,
            EDIT_LOG_INPUT_STREAM_COMPARATOR);
    // 遍历所有Journal收集输入流
    for (JournalAndStream jas : journals) {
      if (jas.isDisabled()) {
        LOG.info("Skipping jas " + jas + " since it's disabled");
        continue;
      }
      try {
        jas.getManager().selectInputStreams(allStreams, fromTxId,
            inProgressOk, onlyDurableTxns);
      } catch (IOException ioe) {
        LOG.warn("Unable to determine input streams from " + jas.getManager() +
            ". Skipping.", ioe);
      }
    }
    // 整理流，处理冗余，生成最终结果链
    chainAndMakeRedundantStreams(streams, allStreams, fromTxId);
  }
  
  /**
   * 将来自多个Journal的输入流整理成连续的冗余流链，优先选择已完成的本地流
   * @param outStreams 输出结果集合
   * @param allStreams 所有收集到的输入流，已按起始事务ID排序
   * @param fromTxId 起始事务ID
   */
  public static void chainAndMakeRedundantStreams(
      Collection<EditLogInputStream> outStreams,
      PriorityQueue<EditLogInputStream> allStreams, long fromTxId) {
    // 按起始事务ID分组，累加同一事务ID开始的所有流
    LinkedList<EditLogInputStream> acc =
        new LinkedList<EditLogInputStream>();
    EditLogInputStream elis;
    while ((elis = allStreams.poll()) != null) {
      if (acc.isEmpty()) {
        acc.add(elis);
      } else {
        EditLogInputStream accFirst = acc.get(0);
        long accFirstTxId = accFirst.getFirstTxId();
        if (accFirstTxId == elis.getFirstTxId()) {
          // 相同起始事务ID，优先保留已完成的流，丢弃进行中的流
          if (elis.isInProgress()) {
            if (accFirst.isInProgress()) {
              acc.add(elis);
            }
          } else {
            if (accFirst.isInProgress()) {
              acc.clear();
            }
            acc.add(elis);
          }
        } else if (accFirstTxId < elis.getFirstTxId()) {
          // 遇到更大起始事务ID，处理当前累加组，优先选择本地流
          Collections.sort(acc, LOCAL_LOG_PREFERENCE_COMPARATOR);
          // 包装为冗余输入流，加入结果集合
          outStreams.add(new RedundantEditLogInputStream(acc, fromTxId));
          acc.clear();
          acc.add(elis);
        } else if (accFirstTxId > elis.getFirstTxId()) {
          // 排序错误，抛出异常
          throw new RuntimeException("sorted set invariants violated!  " +
              "Got stream with first txid " + elis.getFirstTxId() +
              ", but the last firstTxId was " + accFirstTxId);
        }
      }
    }
    // 处理最后一组累加的流
    if (!acc.isEmpty()) {
      Collections.sort(acc, LOCAL_LOG_PREFERENCE_COMPARATOR);
      outStreams.add(new RedundantEditLogInputStream(acc, fromTxId));
      acc.clear();
    }
  }

  /**
   * 判断当前JournalSet资源是否可用，返回true表示没有可用Journal或资源不足
   * Returns true if there are no journals, all redundant journals are disabled,
   * or any required journals are disabled.
   * 
   * @return True if there no journals, all redundant journals are disabled,
   * or any required journals are disabled.
   */
  public boolean isEmpty() {
    return !NameNodeResourcePolicy.areResourcesAvailable(journals,
        minimumRedundantJournals);
  }
  
  /**
   * 禁用出错的Journal并记录错误日志
   * Called when some journals experience an error in some operation.
   */
  private void disableAndReportErrorOnJournals(List<JournalAndStream> badJournals) {
    if (badJournals == null || badJournals.isEmpty()) {
      return; // nothing to do
    }
 
    for (JournalAndStream j : badJournals) {
      LOG.error("Disabling journal " + j);
      j.abort();
      j.setDisabled(true);
    }
  }

  /**
   * 函数式接口，封装对JournalAndStream的操作
   * Implementations of this interface encapsulate operations that can be
   * iteratively applied on all the journals. For example see
   * {@link JournalSet#mapJournalsAndReportErrors}.
   */
  private interface JournalClosure {
    /**
     * The operation on JournalAndStream.
     * @param jas Object on which operations are performed.
     * @throws IOException
     */
    public void apply(JournalAndStream jas) throws IOException;
  }
  
  /**
   * 对所有Journal执行指定操作，处理异常，禁用出错的非必需Journal，
   * 必需Journal出错直接终止NameNode
   * @param closure {@link JournalClosure} object encapsulating the operation.
   * @param status message used for logging errors (e.g. "opening journal")
   * @throws IOException If the operation fails on too many journals.
   */
  private void mapJournalsAndReportErrors(
      JournalClosure closure, String status) throws IOException{

    List<JournalAndStream> badJAS = Lists.newLinkedList();
    // 遍历所有Journal
    for (JournalAndStream jas : journals) {
      try {
        closure.apply(jas);
      } catch (Throwable t) {
        if (jas.isRequired()) {
          // 必需Journal出错，终止所有Journal并退出NameNode
          final String msg = "Error: " + status + " failed for required journal ("
            + jas + ")";
          LOG.error(msg, t);
          abortAllJournals();
          terminate(1, msg);
        } else {
          // 非必需Journal出错，加入坏列表后续禁用
          LOG.error("Error: " + status + " failed for (journal " + jas + ")", t);
          badJAS.add(jas);          
        }
      }
    }
    // 禁用所有出错的非必需Journal
    disableAndReportErrorOnJournals(badJAS);
    // 检查资源是否满足最小要求，不满足抛出异常
    if (!NameNodeResourcePolicy.areResourcesAvailable(journals,
        minimumRedundantJournals)) {
      String message = status + " failed for too many journals";
      LOG.error("Error: " + message);
      throw new IOException(message);
    }
  }
  
  /**
   * Abort all of the underlying streams.
   */
  private void abortAllJournals() {
    for (JournalAndStream jas : journals) {
      if (jas.isActive()) {
        jas.abort();
      }
    }
  }

  /**
   * 实现EditLogOutputStream，将写操作转发到所有活跃的底层Journal输出流，
   * 保证多副本日志同时写入。
   * An implementation of EditLogOutputStream that applies a requested method on
   * all the journals that are currently active.
   */
  private class JournalSetOutputStream extends EditLogOutputStream {

    JournalSetOutputStream() throws IOException {
      super();
    }

    /**
     * Get the last txId journalled in the stream.
     * The txId is recorded when FSEditLogOp is written to the journal.
     * JournalSet tracks the txId uniformly for all underlying streams.
     */
    @Override
    public long getLastJournalledTxId() {
      return lastJournalledTxId;
    }

    @Override
    public void write(final FSEditLogOp op)
        throws IOException {
      // 遍历所有活跃Journal写入操作
      mapJournalsAndReportErrors(new JournalClosure() {
        @Override
        public void apply(JournalAndStream jas) throws IOException {
          if (jas.isActive()) {
            jas.getCurrentStream().write(op);
          }
        }
      }, "write op");